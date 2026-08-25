"""Tests for the records/state/outputs Athena CDK database constructs."""

from __future__ import annotations

import base64
import datetime as dt
import json

import pytest
from aws_cdk import App, Stack, aws_glue as glue
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.athena_outputs_table import (
    AthenaOutputsTable,
)
from batch_event_job_monitor_cdk.athena_records_table import (
    AthenaRecordsTable,
)
from batch_event_job_monitor_cdk.athena_state_table import AthenaStateTable
from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec

_PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "enum", enum_values=("12TVK", "13TVK")),
    PartitionKeySpec(
        "year_month",
        "string",
        "date",
        date_range=("2020-01", "NOW"),
        date_format="yyyy-MM",
        date_interval_unit="MONTHS",
    ),
]


def _make_stack() -> Stack:
    app = App()
    stack = Stack(app, "TestStack")

    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )

    AthenaRecordsTable(
        stack,
        "Records",
        database=database,
        database_name="test_db",
        records_bucket_name="test-bucket",
        partition_keys=_PARTITION_KEYS,
        table_name="records",
    )

    dt_start = dt.datetime(2025, 1, 1, 12, 0)

    AthenaStateTable(
        stack,
        "State",
        database=database,
        database_name="test_db",
        inventory_location_s3path=(
            "s3://test-bucket/inventory/test-bucket/state/hive/"
        ),
        table_datetime_start=dt_start,
        table_name="state_inventory",
        view_name="state",
        partition_keys=_PARTITION_KEYS,
    )

    AthenaOutputsTable(
        stack,
        "Outputs",
        database=database,
        database_name="test_db",
        inventory_location_s3path=(
            "s3://test-bucket/inventory/test-bucket/outputs/hive/"
        ),
        table_datetime_start=dt_start,
        table_name="outputs_inventory",
        view_name="outputs",
        partition_keys=_PARTITION_KEYS,
    )

    return stack


def _decoded_view_sql(template: Template, view_table_name: str) -> str:
    resources = template.to_json()["Resources"]
    [view] = [
        r
        for r in resources.values()
        if r["Type"] == "AWS::Glue::Table"
        and r["Properties"]["TableInput"]["Name"] == view_table_name
    ]
    view_text = view["Properties"]["TableInput"]["ViewOriginalText"]
    b64 = view_text.removeprefix("/* Presto View: ").removesuffix(" */")
    spec = json.loads(base64.b64decode(b64))
    return str(spec["originalSql"])


class TestSynthesis:
    """Synthesizing a stack with all three constructs must not raise."""

    def test_synthesizes_without_error(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack).to_json()
        assert "Resources" in template


class TestAthenaRecordsTable:
    """Tests for the records table."""

    def test_records_table_is_external_table(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {"TableInput": {"Name": "records", "TableType": "EXTERNAL_TABLE"}},
        )

    def test_records_table_partition_keys(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "records",
                    "PartitionKeys": [
                        {"Name": "job_type", "Type": "string"},
                        {"Name": "tile_id", "Type": "string"},
                        {"Name": "year_month", "Type": "string"},
                    ],
                }
            },
        )

    def test_records_table_storage_location_template_and_projection(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "records",
                    "Parameters": Match.object_like(
                        {
                            "storage.location.template": (
                                "s3://test-bucket/records/"
                                "job_type=${job_type}/tile_id=${tile_id}/"
                                "year_month=${year_month}/"
                            ),
                            "projection.job_type.type": "enum",
                            "projection.job_type.values": "monthly-composite",
                            "projection.tile_id.type": "enum",
                            "projection.tile_id.values": "12TVK,13TVK",
                            "projection.year_month.type": "date",
                            "projection.year_month.format": "yyyy-MM",
                            "projection.year_month.range": "2020-01,NOW",
                            "projection.year_month.interval": "1",
                            "projection.year_month.interval.unit": "MONTHS",
                        }
                    ),
                }
            },
        )

    def test_records_table_body_columns(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "records",
                    "StorageDescriptor": Match.object_like(
                        {
                            "Columns": Match.array_with(
                                [
                                    Match.object_like(
                                        {"Name": "input_entity_id", "Type": "string"}
                                    ),
                                    Match.object_like(
                                        {
                                            "Name": "output_entity_id",
                                            "Type": "string",
                                        }
                                    ),
                                    Match.object_like(
                                        {"Name": "attempt", "Type": "int"}
                                    ),
                                    Match.object_like(
                                        {"Name": "current_state", "Type": "string"}
                                    ),
                                ]
                            )
                        }
                    ),
                }
            },
        )


class TestAthenaStateTable:
    """Tests for the state inventory table and current-state view."""

    def test_inventory_table_is_external_table(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "state_inventory",
                    "TableType": "EXTERNAL_TABLE",
                }
            },
        )

    def test_state_view_is_virtual_view(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {"TableInput": {"Name": "state", "TableType": "VIRTUAL_VIEW"}},
        )

    def test_state_view_columns_include_partition_keys(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "state",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "state", "Type": "string"},
                            {"Name": "job_type", "Type": "string"},
                            {"Name": "tile_id", "Type": "string"},
                            {"Name": "year_month", "Type": "string"},
                            {"Name": "input_entity_id", "Type": "string"},
                            {"Name": "attempt", "Type": "int"},
                            {"Name": "last_modified_date", "Type": "timestamp"},
                            {"Name": "key", "Type": "string"},
                        ]
                    },
                }
            },
        )

    def test_state_view_sql_extracts_all_partition_keys(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        sql = _decoded_view_sql(template, "state")
        for key in ("job_type", "tile_id", "year_month"):
            assert f"/{key}=([^/]+)/" in sql
        assert "/input_entity_id=([^/]+)/" in sql
        assert "/([0-9]{3})$" in sql


class TestAthenaOutputsTable:
    """Tests for the outputs inventory table and current-outputs view."""

    def test_inventory_table_is_external_table(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "outputs_inventory",
                    "TableType": "EXTERNAL_TABLE",
                }
            },
        )

    def test_outputs_view_is_virtual_view(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {"TableInput": {"Name": "outputs", "TableType": "VIRTUAL_VIEW"}},
        )

    def test_outputs_view_columns_include_partition_keys(self) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": {
                    "Name": "outputs",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "state", "Type": "string"},
                            {"Name": "job_type", "Type": "string"},
                            {"Name": "tile_id", "Type": "string"},
                            {"Name": "year_month", "Type": "string"},
                            {"Name": "output_entity_id", "Type": "string"},
                            {"Name": "last_modified_date", "Type": "timestamp"},
                            {"Name": "key", "Type": "string"},
                        ]
                    },
                }
            },
        )

    def test_outputs_view_sql_extracts_all_partition_keys_and_trailing_segment(
        self,
    ) -> None:
        stack = _make_stack()
        template = Template.from_stack(stack)
        sql = _decoded_view_sql(template, "outputs")
        for key in ("job_type", "tile_id", "year_month"):
            assert f"/{key}=([^/]+)/" in sql
        assert "/([^/]+)$" in sql


class TestPartitionKeySpecValidation:
    """Tests for PartitionKeySpec's __post_init__ validation."""

    def test_enum_projection_requires_enum_values(self) -> None:
        with pytest.raises(ValueError, match="enum_values"):
            PartitionKeySpec("job_type", "string", "enum")

    def test_date_projection_requires_date_range(self) -> None:
        with pytest.raises(ValueError, match="date_range"):
            PartitionKeySpec("year_month", "string", "date")

    def test_date_projection_requires_date_format(self) -> None:
        with pytest.raises(ValueError, match="date_format"):
            PartitionKeySpec(
                "year",
                "string",
                "date",
                date_range=("2020", "NOW"),
                date_interval_unit="YEARS",
            )

    def test_date_projection_requires_date_interval_unit(self) -> None:
        with pytest.raises(ValueError, match="date_interval_unit"):
            PartitionKeySpec(
                "year",
                "string",
                "date",
                date_range=("2020", "NOW"),
                date_format="yyyy",
            )


def _logical_id(template: Template, table_name: str) -> str:
    resources = template.to_json()["Resources"]
    [logical_id] = [
        key
        for key, r in resources.items()
        if r["Type"] == "AWS::Glue::Table"
        and r["Properties"]["TableInput"]["Name"] == table_name
    ]
    return str(logical_id)


def _depends_on(template: Template, table_name: str) -> list[str]:
    resources = template.to_json()["Resources"]
    return list(resources[_logical_id(template, table_name)].get("DependsOn", []))


class TestResourceDependencies:
    """Tables must be created after the database, views after their table."""

    @pytest.mark.parametrize(
        "table_name", ["records", "state_inventory", "outputs_inventory"]
    )
    def test_table_depends_on_database(self, table_name: str) -> None:
        template = Template.from_stack(_make_stack())
        assert _depends_on(template, table_name) == ["TestDatabase"]

    @pytest.mark.parametrize(
        ("view_name", "table_name"),
        [("state", "state_inventory"), ("outputs", "outputs_inventory")],
    )
    def test_view_depends_on_inventory_table(
        self, view_name: str, table_name: str
    ) -> None:
        template = Template.from_stack(_make_stack())
        assert _depends_on(template, view_name) == [_logical_id(template, table_name)]
