"""Tests for the IcebergRecordsTable CDK construct."""

from __future__ import annotations

import datetime as dt

from aws_cdk import App, RemovalPolicy, Stack, aws_glue as glue
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.iceberg_records_table import IcebergRecordsTable
from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec

PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "injected"),
]


def _template() -> Template:
    app = App()
    stack = Stack(app, "TestStack")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    IcebergRecordsTable(
        stack,
        "IcebergRecords",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path="s3://test-bucket/inv/test-bucket/records/hive/",
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
        removal_policy=RemovalPolicy.DESTROY,
    )
    return Template.from_stack(stack)


def test_staging_table_is_partitioned_by_run_id_with_injected_projection() -> None:
    _template().has_resource_properties(
        "AWS::Glue::Table",
        {
            "TableInput": Match.object_like(
                {
                    "PartitionKeys": [{"Name": "run_id", "Type": "string"}],
                    "Parameters": Match.object_like(
                        {"projection.run_id.type": "injected"}
                    ),
                }
            )
        },
    )


def test_staging_table_keeps_last_event_timestamp_as_a_string() -> None:
    _template().has_resource_properties(
        "AWS::Glue::Table",
        {
            "TableInput": Match.object_like(
                {
                    "StorageDescriptor": Match.object_like(
                        {
                            "Columns": Match.array_with(
                                [{"Name": "last_event_timestamp", "Type": "string"}]
                            )
                        }
                    )
                }
            )
        },
    )


def test_table_optimizer_is_enabled_for_compaction() -> None:
    _template().has_resource_properties(
        "AWS::Glue::TableOptimizer",
        {
            "Type": "compaction",
            "TableOptimizerConfiguration": Match.object_like({"Enabled": True}),
        },
    )


def test_records_inventory_table_is_created() -> None:
    _template().resource_count_is("AWS::Glue::Table", 2)


def test_ddl_custom_resource_receives_the_partition_key_names() -> None:
    _template().has_resource_properties(
        "Custom::IcebergRecordsTable",
        Match.object_like(
            {
                "Database": "test_db",
                "PartitionKeyNames": "job_type,tile_id",
                "RemovalPolicy": "destroy",
            }
        ),
    )


def test_ddl_handler_role_can_read_the_live_table_schema() -> None:
    """glue:GetTable/GetDatabase are required for every Update.

    Task 10's handler reads the live table schema via Glue GetTable on
    every Update to detect a column retyped since the last deploy. Without
    this grant every stack Update of this table fails.
    """
    _template().has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": Match.array_with(
                                    ["glue:GetDatabase", "glue:GetTable"]
                                )
                            }
                        )
                    ]
                )
            }
        },
    )


def test_ddl_handler_role_can_run_the_ddl_through_athena() -> None:
    _template().has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": Match.array_with(
                                    [
                                        "athena:StartQueryExecution",
                                        "athena:GetQueryExecution",
                                    ]
                                )
                            }
                        )
                    ]
                )
            }
        },
    )
