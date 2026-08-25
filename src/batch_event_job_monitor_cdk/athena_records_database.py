"""CDK construct for the Athena records/ database.

Creates a Glue database + partition-projected table over the Hive-style
``records/`` prefix. Partition projection means new partitions are
queryable immediately as records land -- no ``MSCK REPAIR TABLE`` or Glue
crawler needed.

S3 key schema:
  ``records/job_type={job_type}/{partition_fields...}/input_entity_id={input_entity_id}/{attempt:03d}.json``

``partition_keys`` is the full ordered partition-key list, including the
leading ``job_type`` entry -- job_type is just another partition key at this
layer, not special-cased.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct

from .athena_common import HIVE_TEXT_OUTPUT_FORMAT, JSON_INPUT_FORMAT, JSON_SERDE
from .partition_key_spec import (
    PartitionKeySpec,
    glue_partition_keys,
    partition_projection_parameters,
    storage_location_template_segment,
)

# events[] struct -- matches ProcessingEventRecord fields.
_EVENTS_TYPE = (
    "array<struct<state:string,timestamp:string,batch_job_id:string,exit_code:int>>"
)

# Partition keys are excluded here -- their values are read from the object
# key path, not the JSON body.
_COLUMNS = [
    ("input_entity_id", "string", "Processed entity identifier"),
    ("output_entity_id", "string", "Output entity identifier"),
    ("attempt", "int", "Attempt number (1-indexed)"),
    ("batch_job_id", "string", "AWS Batch job ID"),
    ("events", _EVENTS_TYPE, "Append-only list of state-transition events"),
    ("current_state", "string", "Most recent state"),
]


class AthenaRecordsDatabase(Construct):
    """Athena database for querying canonical processing records.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    database : glue.CfnDatabase
        Glue database the records table is created in.
    database_name : str
        Literal name of ``database`` (not a CDK token).
    records_bucket_name : str
        Name of the bucket holding the ``records/`` prefix.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading ``job_type`` entry.
    table_name : str
        Name of the records table.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    database : glue.CfnDatabase
        The Glue database passed in.
    records_table : glue.CfnTable
        The partition-projected records table.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        records_bucket_name: str,
        partition_keys: list[PartitionKeySpec],
        table_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = database

        s3_location = f"s3://{records_bucket_name}/records/"
        self.records_table = self._create_records_table(
            table_name=table_name,
            s3_location=s3_location,
            partition_keys=partition_keys,
        )

    def _create_records_table(
        self,
        *,
        table_name: str,
        s3_location: str,
        partition_keys: list[PartitionKeySpec],
    ) -> glue.CfnTable:
        columns = [
            glue.CfnTable.ColumnProperty(name=name, type=col_type, comment=comment)
            for name, col_type, comment in _COLUMNS
        ]

        location_template = s3_location + storage_location_template_segment(
            partition_keys
        )
        projection_params = {
            "EXTERNAL": "TRUE",
            "projection.enabled": "true",
            "storage.location.template": location_template,
            **partition_projection_parameters(partition_keys),
        }

        table = glue.CfnTable(
            self,
            "RecordsTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=self.database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters=projection_params,
                partition_keys=glue_partition_keys(partition_keys),
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=s3_location,
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
        table.add_resource_dependency(self.database)
        return table
