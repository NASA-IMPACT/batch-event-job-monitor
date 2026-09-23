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

INVENTORY_LOCATION = "s3://test-bucket/inv/test-bucket/records/hive/"


def _construct() -> tuple[Stack, IcebergRecordsTable]:
    app = App()
    stack = Stack(app, "TestStack")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    construct = IcebergRecordsTable(
        stack,
        "IcebergRecords",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path=INVENTORY_LOCATION,
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
        removal_policy=RemovalPolicy.DESTROY,
    )
    return stack, construct


def _template() -> Template:
    stack, _ = _construct()
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


def test_staging_table_storage_location_template() -> None:
    _template().has_resource_properties(
        "AWS::Glue::Table",
        {
            "TableInput": Match.object_like(
                {
                    "Parameters": Match.object_like(
                        {
                            "storage.location.template": (
                                "s3://test-bucket/staging/run_id=${run_id}/"
                            )
                        }
                    )
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
    template = _template()
    template.resource_count_is("AWS::Glue::Table", 2)
    template.has_resource_properties(
        "AWS::Glue::Table",
        {
            "TableInput": Match.object_like(
                {
                    "Name": "records_inventory",
                    "StorageDescriptor": Match.object_like(
                        {"Location": INVENTORY_LOCATION}
                    ),
                }
            )
        },
    )


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


def test_ddl_custom_resource_receives_the_table_location_and_workgroup() -> None:
    _template().has_resource_properties(
        "Custom::IcebergRecordsTable",
        Match.object_like(
            {
                "Table": "records_iceberg",
                "Location": "s3://test-bucket/iceberg/records/",
                "Workgroup": "IcebergRecords-workgroup",
            }
        ),
    )


def test_exposed_attributes_are_the_table_names_and_location() -> None:
    _, construct = _construct()
    assert construct.iceberg_table_name == "records_iceberg"
    assert construct.staging_table_name == "records_staging"
    assert construct.inventory_table_name == "records_inventory"
    assert construct.table_location == "s3://test-bucket/iceberg/records/"
    assert construct.inventory_location_s3path == INVENTORY_LOCATION
    assert construct.workgroup_name == "IcebergRecords-workgroup"
    assert construct.workgroup is not None


def test_a_workgroup_is_created_with_results_under_the_processing_bucket() -> None:
    template = _template()
    workgroups = template.find_resources("AWS::Athena::WorkGroup")
    assert len(workgroups) == 1
    (workgroup,) = workgroups.values()
    assert workgroup["Properties"]["Name"] == "IcebergRecords-workgroup"
    output_location = workgroup["Properties"]["WorkGroupConfiguration"][
        "ResultConfiguration"
    ]["OutputLocation"]
    assert output_location == "s3://test-bucket/athena-results/"


def test_ddl_custom_resource_depends_on_the_created_workgroup() -> None:
    template = _template()
    ddl_resources = template.find_resources("Custom::IcebergRecordsTable")
    (resource,) = ddl_resources.values()
    depends_on = resource["DependsOn"]
    workgroups = template.find_resources("AWS::Athena::WorkGroup")
    (workgroup_logical_id,) = workgroups.keys()
    assert workgroup_logical_id in depends_on


def test_an_explicit_workgroup_name_skips_creating_one() -> None:
    app = App()
    stack = Stack(app, "TestStack")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    construct = IcebergRecordsTable(
        stack,
        "IcebergRecords",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path=INVENTORY_LOCATION,
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
        workgroup_name="primary",
    )
    assert construct.workgroup_name == "primary"
    assert construct.workgroup is None
    template = Template.from_stack(stack)
    template.resource_count_is("AWS::Athena::WorkGroup", 0)
    template.has_resource_properties(
        "Custom::IcebergRecordsTable",
        Match.object_like({"Workgroup": "primary"}),
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
