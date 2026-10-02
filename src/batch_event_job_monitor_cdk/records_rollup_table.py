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
    Stack,
    aws_athena as athena,
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
from .lambda_asset import HANDLER_ENTRY, HANDLER_EXCLUDE
from .partition_key_spec import PartitionKeySpec

ROLLUP_PREFIX = "records-rollup/"
TABLE_PREFIX = f"{ROLLUP_PREFIX}table/"
STAGING_PREFIX = f"{ROLLUP_PREFIX}staging/"
ATHENA_RESULTS_PREFIX = f"{ROLLUP_PREFIX}athena-results/"

# Athena's documented minimum grant for a query-results location. Shared by
# every function that queries through this table's workgroup -- the DDL
# Lambda, and the rollup/reconcile Lambdas in RecordsRollupFunction --
# because they all read and write the same results prefix. The multipart
# actions matter only once a result file crosses the multipart threshold, so
# omitting them looks harmless until a large backfill run hits it.
ATHENA_RESULTS_ACTIONS = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
    "s3:GetBucketLocation",
    "s3:AbortMultipartUpload",
    "s3:ListBucketMultipartUploads",
    "s3:ListMultipartUploadParts",
]


class RecordsRollupTable(Construct):
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
    key_prefix : str, optional
        Parent prefix the rollup's own S3 prefixes sit under. Must match
        the ProcessingBucket's own key_prefix. Defaults to "".
    processing_bucket_name : str
        Name of the bucket holding records/, staging/, and the Iceberg data.
    records_inventory_location_s3path : str
        s3:// URI of the records prefix's S3 Inventory Hive symlink manifests.
    inventory_datetime_start : dt.datetime
        Anchor for the inventory table's dt partition projection. Its
        time-of-day must match the S3 delivery hour.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading job_type entry.
    workgroup_name : str or None, optional
        Athena workgroup the DDL runs in. When None (the default), this
        construct creates its own workgroup (exposed as ``workgroup``),
        named ``{stack_name}-{construct_id}-workgroup``,
        with query results under ``s3://{processing_bucket_name}/
        athena-results/`` and orders the DDL custom resource after it.
        Pass an existing workgroup's name to skip creating one; in that
        case its query results location is not this construct's
        responsibility.
    table_name : str, optional
        Name of the Iceberg table. Defaults to "records".
    staging_table_name : str, optional
        Name of the staging table. Defaults to "records-staging".
    inventory_table_name : str, optional
        Name of the inventory table. Defaults to "records-inventory".
    removal_policy : RemovalPolicy, optional
        Removal policy for the Iceberg table (applied via the DDL custom
        resource). The staging table and the inventory table are always
        DESTROY, matching the other inventory-backed tables in this package.
        Defaults to RemovalPolicy.RETAIN.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    table_name : str
        Name of the Iceberg table.
    staging_table_name : str
        Name of the staging table.
    inventory_table_name : str
        Name of the inventory table.
    partition_key_names : list[str]
        Ordered partition key names, job_type first -- the names extracted
        from the ``partition_keys`` given at construction. Read by
        RecordsRollupFunction to default its own partition key list, so
        both constructs' generated SQL agrees on the natural key by
        construction rather than by two callers coincidentally passing
        matching lists.
    table_location : str
        s3:// URI of the Iceberg table's data location.
    inventory_location_s3path : str
        s3:// URI of the records prefix's S3 Inventory Hive symlink
        manifests, as given.
    workgroup_name : str
        Name of the Athena workgroup the DDL runs in -- either the name
        handed in, or the name of the workgroup created here.
    workgroup : athena.CfnWorkGroup or None
        The Athena workgroup created here, or None when an existing
        workgroup name was handed in instead.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        processing_bucket_name: str,
        key_prefix: str = "",
        records_inventory_location_s3path: str,
        inventory_datetime_start: dt.datetime,
        partition_keys: list[PartitionKeySpec],
        workgroup_name: str | None = None,
        table_name: str = "records",
        staging_table_name: str = "records-staging",
        inventory_table_name: str = "records-inventory",
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table_name = table_name
        self.staging_table_name = staging_table_name
        self.inventory_table_name = inventory_table_name
        self.key_prefix = (
            f"{key_prefix}/"
            if key_prefix and not key_prefix.endswith("/")
            else key_prefix
        )
        self.table_location = (
            f"s3://{processing_bucket_name}/{self.key_prefix}{TABLE_PREFIX}"
        )
        self.inventory_location_s3path = records_inventory_location_s3path

        self.workgroup_name, self.workgroup = self._resolve_workgroup(
            workgroup_name, processing_bucket_name=processing_bucket_name
        )

        partition_key_names = [key.name for key in partition_keys]
        self.partition_key_names = partition_key_names

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
            location=f"s3://{processing_bucket_name}/{self.key_prefix}{STAGING_PREFIX}",
            partition_key_names=partition_key_names,
        )

        self.ddl_resource = self._create_ddl_resource(
            database=database,
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
            partition_key_names=partition_key_names,
            workgroup_name=self.workgroup_name,
            removal_policy=removal_policy,
        )

        self._create_table_optimizer(
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
        )

    def _resolve_workgroup(
        self, workgroup_name: str | None, *, processing_bucket_name: str
    ) -> tuple[str, athena.CfnWorkGroup | None]:
        """Resolve the Athena workgroup name, creating one if not handed in.

        Parameters
        ----------
        workgroup_name : str or None
            Caller-supplied workgroup name, or None to create one here.
        processing_bucket_name : str
            Bucket the created workgroup's query results are written
            under.

        Returns
        -------
        tuple[str, athena.CfnWorkGroup or None]
            The resolved workgroup name, and the CfnWorkGroup created here
            (None when an existing name was handed in).
        """
        if workgroup_name is not None:
            return workgroup_name, None

        # Workgroup names are unique per account and region, so a name
        # without the stack would collide across stages sharing an account.
        name = f"{Stack.of(self).stack_name}-{self.node.id}-workgroup"
        workgroup = athena.CfnWorkGroup(
            self,
            "Workgroup",
            name=name,
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=(
                        f"s3://{processing_bucket_name}/{self.key_prefix}{ATHENA_RESULTS_PREFIX}"
                    ),
                ),
            ),
        )
        return name, workgroup

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
            handler="batch_event_job_monitor.handlers.records_table_ddl_handler.handler",
            code=lambda_.Code.from_asset(HANDLER_ENTRY, exclude=HANDLER_EXCLUDE),
            timeout=Duration.minutes(10),
        )
        workgroup_arn = (
            f"arn:aws:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/{workgroup_name}"
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["athena:StartQueryExecution", "athena:GetQueryExecution"],
                resources=[workgroup_arn],
            )
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                ],
                resources=self._glue_table_arns(
                    database_name=database_name, table_name=self.table_name
                ),
            )
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                # DeleteObject is additionally required here because
                # Athena's Iceberg DROP TABLE deletes the table's data and
                # metadata files in S3.
                actions=ATHENA_RESULTS_ACTIONS,
                resources=[
                    f"arn:aws:s3:::{processing_bucket_name}",
                    f"arn:aws:s3:::{processing_bucket_name}/*",
                ],
            )
        )

        provider = cr.Provider(self, "DdlProvider", on_event_handler=ddl_function)
        resource = CustomResource(
            self,
            "Table",
            service_token=provider.service_token,
            resource_type="Custom::RecordsRollupTable",
            properties={
                "Database": database_name,
                "Table": self.table_name,
                "Location": self.table_location,
                "PartitionKeyNames": ",".join(partition_key_names),
                "Workgroup": workgroup_name,
                "RemovalPolicy": (
                    "destroy" if removal_policy is RemovalPolicy.DESTROY else "retain"
                ),
                # CloudFormation re-invokes a custom resource only when its
                # properties change, so a handler whose code changed but
                # whose inputs did not is shipped and never called. The
                # asset carries both the handler and the SQL generation, so
                # its version changes whenever either does.
                "HandlerVersion": ddl_function.current_version.version,
            },
        )
        resource.node.add_dependency(database)
        if self.workgroup is not None:
            resource.node.add_dependency(self.workgroup)
        return resource

    def _create_table_optimizer(
        self, *, database_name: str, processing_bucket_name: str
    ) -> None:
        optimizer_role = iam.Role(
            self,
            "TableOptimizerRole",
            assumed_by=iam.ServicePrincipal(
                "glue.amazonaws.com",
                conditions={"StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID}},
            ),
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
                resources=self._glue_table_arns(
                    database_name=database_name, table_name=self.table_name
                ),
            )
        )

        for construct_id, optimizer_type in (
            ("CompactionOptimizer", "compaction"),
            ("RetentionOptimizer", "retention"),
            ("OrphanFileDeletionOptimizer", "orphan_file_deletion"),
        ):
            optimizer = glue.CfnTableOptimizer(
                self,
                construct_id,
                catalog_id=Aws.ACCOUNT_ID,
                database_name=database_name,
                table_name=self.table_name,
                type=optimizer_type,
                table_optimizer_configuration=(
                    glue.CfnTableOptimizer.TableOptimizerConfigurationProperty(
                        enabled=True,
                        role_arn=optimizer_role.role_arn,
                    )
                ),
            )
            optimizer.node.add_dependency(self.ddl_resource)

    @staticmethod
    def _glue_table_arns(*, database_name: str, table_name: str) -> list[str]:
        """Build the catalog/database/table ARN triple for one Glue table.

        Parameters
        ----------
        database_name : str
            Glue database name.
        table_name : str
            Glue table name.

        Returns
        -------
        list[str]
            The catalog, database, and table ARNs, in that order. Glue
            CreateTable/UpdateTable/DeleteTable/GetTable each act on the
            table but also require catalog- and database-level permission.
        """
        return [
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{database_name}",
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/"
            f"{database_name}/{table_name}",
        ]
