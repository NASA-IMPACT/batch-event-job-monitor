"""Tests for athena_common CDK construct helpers."""

from __future__ import annotations

import datetime as dt

import pytest
from aws_cdk import App, Stack, aws_glue as glue
from aws_cdk.assertions import Template

from batch_event_job_monitor_cdk.athena_common import (
    HIVE_TEXT_OUTPUT_FORMAT,
    PARQUET_SERDE,
    SYMLINK_INPUT_FORMAT,
    athena_to_presto,
    create_inventory_table,
    create_presto_view,
)


class TestAthenaToPresto:
    """Tests for athena_to_presto type mapping function."""

    def test_string_to_varchar(self) -> None:
        assert athena_to_presto("string") == "varchar"

    def test_struct_to_row(self) -> None:
        assert athena_to_presto("struct") == "row"

    def test_float_to_real(self) -> None:
        assert athena_to_presto("float") == "real"

    def test_binary_to_varbinary(self) -> None:
        assert athena_to_presto("binary") == "varbinary"

    def test_passthrough_unmapped_type(self) -> None:
        assert athena_to_presto("bigint") == "bigint"
        assert athena_to_presto("array") == "array"

    def test_case_insensitive(self) -> None:
        assert athena_to_presto("STRING") == "varchar"
        assert athena_to_presto("Float") == "real"

    def test_none_raises_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot convert null Athena type"):
            athena_to_presto(None)


class TestCreateInventoryTable:
    """Tests for create_inventory_table function."""

    def test_creates_external_table(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        dt_start = dt.datetime(2025, 1, 1, 12, 0)
        create_inventory_table(
            stack,
            "TestTable",
            database=database,
            table_name="inventory",
            location="s3://bucket/prefix/",
            datetime_start=dt_start,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "inventory",
                    "TableType": "EXTERNAL_TABLE",
                }
            },
        )

    def test_partition_projection_parameters(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        dt_start = dt.datetime(2025, 1, 1, 12, 30)
        create_inventory_table(
            stack,
            "TestTable",
            database=database,
            table_name="inventory",
            location="s3://bucket/prefix/",
            datetime_start=dt_start,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Parameters": {
                        "EXTERNAL": "TRUE",
                        "projection.enabled": "true",
                        "projection.dt.type": "date",
                        "projection.dt.format": "yyyy-MM-dd-HH-mm",
                        "projection.dt.range": "2025-01-01-12-30,NOW",
                        "projection.dt.interval": "1",
                        "projection.dt.interval.unit": "DAYS",
                    },
                }
            },
        )

    def test_storage_descriptor_configuration(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        dt_start = dt.datetime(2025, 1, 1, 0, 0)
        create_inventory_table(
            stack,
            "TestTable",
            database=database,
            table_name="inventory",
            location="s3://bucket/prefix/",
            datetime_start=dt_start,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "StorageDescriptor": {
                        "InputFormat": SYMLINK_INPUT_FORMAT,
                        "OutputFormat": HIVE_TEXT_OUTPUT_FORMAT,
                        "SerdeInfo": {
                            "SerializationLibrary": PARQUET_SERDE,
                            "Parameters": {"serialization.format": "1"},
                        },
                        "Location": "s3://bucket/prefix/",
                    }
                }
            },
        )

    def test_inventory_columns_in_table(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        dt_start = dt.datetime(2025, 1, 1, 0, 0)
        create_inventory_table(
            stack,
            "TestTable",
            database=database,
            table_name="inventory",
            location="s3://bucket/prefix/",
            datetime_start=dt_start,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "bucket", "Type": "string"},
                            {"Name": "key", "Type": "string"},
                            {"Name": "version_id", "Type": "string"},
                            {"Name": "is_latest", "Type": "boolean"},
                            {"Name": "is_delete_marker", "Type": "boolean"},
                            {"Name": "last_modified_date", "Type": "timestamp"},
                        ]
                    }
                }
            },
        )

    def test_partition_key_configuration(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        dt_start = dt.datetime(2025, 1, 1, 0, 0)
        create_inventory_table(
            stack,
            "TestTable",
            database=database,
            table_name="inventory",
            location="s3://bucket/prefix/",
            datetime_start=dt_start,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "PartitionKeys": [
                        {"Name": "dt", "Type": "string"},
                    ]
                }
            },
        )


class TestCreatePrestoView:
    """Tests for create_presto_view function."""

    def test_creates_virtual_view(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [
            glue.CfnTable.ColumnProperty(name="col1", type="string"),
            glue.CfnTable.ColumnProperty(name="col2", type="bigint"),
        ]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "test_view",
                    "TableType": "VIRTUAL_VIEW",
                }
            },
        )

    def test_view_parameters(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [glue.CfnTable.ColumnProperty(name="col1", type="string")]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Parameters": {
                        "presto_view": "true",
                        "comment": "Presto View",
                    }
                }
            },
        )

    def test_view_contains_base64_encoded_spec(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [glue.CfnTable.ColumnProperty(name="col1", type="string")]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
        )

        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        presto_view_resource = next(
            r
            for r in resources.values()
            if r["Type"] == "AWS::Glue::Table"
            and r["Properties"]["TableInput"]["Name"] == "test_view"
        )
        view_text = presto_view_resource["Properties"]["TableInput"]["ViewOriginalText"]
        assert view_text.startswith("/* Presto View: ")
        assert view_text.endswith(" */")

    def test_view_with_partition_keys(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [glue.CfnTable.ColumnProperty(name="col1", type="string")]
        partition_keys = [glue.CfnTable.ColumnProperty(name="dt", type="string")]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
            partition_keys=partition_keys,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "PartitionKeys": [
                        {"Name": "dt", "Type": "string"},
                    ]
                }
            },
        )

    def test_view_without_partition_keys(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [glue.CfnTable.ColumnProperty(name="col1", type="string")]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
            partition_keys=None,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "PartitionKeys": [],
                }
            },
        )

    def test_view_type_mapping_in_columns(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [
            glue.CfnTable.ColumnProperty(name="str_col", type="string"),
            glue.CfnTable.ColumnProperty(name="float_col", type="float"),
            glue.CfnTable.ColumnProperty(name="binary_col", type="binary"),
        ]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "str_col", "Type": "string"},
                            {"Name": "float_col", "Type": "float"},
                            {"Name": "binary_col", "Type": "binary"},
                        ]
                    }
                }
            },
        )

    def test_view_storage_descriptor(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")

        database = glue.CfnDatabase(
            stack,
            "TestDatabase",
            catalog_id="123456789012",
            database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
        )

        columns = [glue.CfnTable.ColumnProperty(name="col1", type="string")]

        create_presto_view(
            stack,
            "TestView",
            database=database,
            database_name="test_db",
            view_name="test_view",
            sql="SELECT * FROM table1",
            columns=columns,
        )

        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "StorageDescriptor": {
                        "InputFormat": SYMLINK_INPUT_FORMAT,
                        "OutputFormat": HIVE_TEXT_OUTPUT_FORMAT,
                    }
                }
            },
        )
