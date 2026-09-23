"""CDK construct for the rolled-up Iceberg records table.

Creates the Iceberg table itself through an Athena DDL custom resource, the
NDJSON staging table the rollup merges from, the S3-inventory table reconcile
anti-joins against, and the Glue table optimizer that compacts the Iceberg
table.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from aws_cdk import (
    Aws,
    CustomResource,
    Duration,
    RemovalPolicy,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    custom_resources as cr,
)
from constructs import Construct

from batch_event_job_monitor.rollup_schema import staging_columns

from .athena_common import (
    HIVE_TEXT_OUTPUT_FORMAT,
    JSON_INPUT_FORMAT,
    JSON_SERDE,
    create_inventory_table,
)
from .partition_key_spec import PartitionKeySpec

ICEBERG_PREFIX = "iceberg/records/"
STAGING_PREFIX = "staging/"


class IcebergRecordsTable(Construct):
    """Iceberg records table, its staging table, and its inventory table.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    database : glue.CfnDatabase
        Glue database every table is created in.
    database_name : str
        Literal name of ``database`` (not a CDK token).
    processing_bucket_name : str
        Name of the bucket holding records/, staging/, and the Iceberg data.
    records_inventory_location_s3path : str
        s3:// URI of the records prefix's S3 Inventory Hive symlink manifests.
    inventory_datetime_start : dt.datetime
        Anchor for the inventory table's dt partition projection. Its
        time-of-day must match the S3 delivery hour.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading job_type entry.
    workgroup_name : str, optional
        Athena workgroup the DDL runs in. Defaults to "primary".
    iceberg_table_name : str, optional
        Name of the Iceberg table. Defaults to "records_iceberg".
    staging_table_name : str, optional
        Name of the staging table. Defaults to "records_staging".
    inventory_table_name : str, optional
        Name of the inventory table. Defaults to "records_inventory".
    removal_policy : RemovalPolicy, optional
        Removal policy for the catalog entries. Defaults to
        RemovalPolicy.RETAIN.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    iceberg_table_name : str
        Name of the Iceberg table.
    staging_table_name : str
        Name of the staging table.
    inventory_table_name : str
        Name of the inventory table.
    table_location : str
        s3:// URI of the Iceberg table's data location.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        processing_bucket_name: str,
        records_inventory_location_s3path: str,
        inventory_datetime_start: dt.datetime,
        partition_keys: list[PartitionKeySpec],
        workgroup_name: str = "primary",
        iceberg_table_name: str = "records_iceberg",
        staging_table_name: str = "records_staging",
        inventory_table_name: str = "records_inventory",
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.iceberg_table_name = iceberg_table_name
        self.staging_table_name = staging_table_name
        self.inventory_table_name = inventory_table_name
        self.table_location = f"s3://{processing_bucket_name}/{ICEBERG_PREFIX}"

        partition_key_names = [key.name for key in partition_keys]

        self.inventory_table = create_inventory_table(
            self,
            "RecordsInventoryTable",
            database=database,
            table_name=inventory_table_name,
            location=records_inventory_location_s3path,
            datetime_start=inventory_datetime_start,
        )

        self.staging_table = self._create_staging_table(
            database=database,
            table_name=staging_table_name,
            location=f"s3://{processing_bucket_name}/{STAGING_PREFIX}",
            partition_key_names=partition_key_names,
        )

        self.ddl_resource = self._create_ddl_resource(
            database=database,
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
            partition_key_names=partition_key_names,
            workgroup_name=workgroup_name,
            removal_policy=removal_policy,
        )

        self._create_table_optimizer(
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
        )

    def _create_staging_table(
        self,
        *,
        database: glue.CfnDatabase,
        table_name: str,
        location: str,
        partition_key_names: list[str],
    ) -> glue.CfnTable:
        columns = [
            glue.CfnTable.ColumnProperty(name=name, type=col_type)
            for name, col_type in staging_columns(partition_key_names)
        ]
        table = glue.CfnTable(
            self,
            "StagingTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters={
                    "EXTERNAL": "TRUE",
                    "projection.enabled": "true",
                    "projection.run_id.type": "injected",
                    "storage.location.template": f"{location}run_id=${{run_id}}/",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="run_id", type="string")
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=location,
                    input_format=JSON_INPUT_FORMAT,
                    output_format=HIVE_TEXT_OUTPUT_FORMAT,
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library=JSON_SERDE,
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )
        table.apply_removal_policy(RemovalPolicy.DESTROY)
        table.add_resource_dependency(database)
        return table

    def _create_ddl_resource(
        self,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        processing_bucket_name: str,
        partition_key_names: list[str],
        workgroup_name: str,
        removal_policy: RemovalPolicy,
    ) -> CustomResource:
        ddl_function = lambda_.Function(
            self,
            "DdlFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="batch_event_job_monitor.handlers.iceberg_ddl_handler.handler",
            code=lambda_.Code.from_asset("src"),
            timeout=Duration.minutes(10),
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                ],
                resources=["*"],
            )
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{processing_bucket_name}",
                    f"arn:aws:s3:::{processing_bucket_name}/*",
                ],
            )
        )

        provider = cr.Provider(self, "DdlProvider", on_event_handler=ddl_function)
        resource = CustomResource(
            self,
            "IcebergTable",
            service_token=provider.service_token,
            resource_type="Custom::IcebergRecordsTable",
            properties={
                "Database": database_name,
                "Table": self.iceberg_table_name,
                "Location": self.table_location,
                "PartitionKeyNames": ",".join(partition_key_names),
                "Workgroup": workgroup_name,
                "RemovalPolicy": (
                    "destroy" if removal_policy is RemovalPolicy.DESTROY else "retain"
                ),
            },
        )
        resource.node.add_dependency(database)
        return resource

    def _create_table_optimizer(
        self, *, database_name: str, processing_bucket_name: str
    ) -> None:
        optimizer_role = iam.Role(
            self,
            "TableOptimizerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        optimizer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::{processing_bucket_name}",
                    f"arn:aws:s3:::{processing_bucket_name}/*",
                ],
            )
        )
        optimizer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetTable", "glue:UpdateTable"],
                resources=["*"],
            )
        )

        optimizer = glue.CfnTableOptimizer(
            self,
            "CompactionOptimizer",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_name=self.iceberg_table_name,
            type="compaction",
            table_optimizer_configuration=(
                glue.CfnTableOptimizer.TableOptimizerConfigurationProperty(
                    enabled=True,
                    role_arn=optimizer_role.role_arn,
                )
            ),
        )
        optimizer.node.add_dependency(self.ddl_resource)
