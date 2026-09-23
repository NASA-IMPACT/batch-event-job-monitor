"""Tests for the Iceberg table DDL custom resource handler."""

from __future__ import annotations

from typing import Any

import pytest

from batch_event_job_monitor.handlers import iceberg_ddl_handler
from batch_event_job_monitor.handlers.iceberg_ddl_handler import (
    IncompatibleSchemaChange,
    added_column_sql,
)

PROPERTIES = {
    "Database": "test_db",
    "Table": "records_iceberg",
    "Location": "s3://test-bucket/iceberg/records/",
    "PartitionKeyNames": "job_type,tile_id",
    "Workgroup": "wg",
    "RemovalPolicy": "destroy",
}


class FakeAthena:
    def __init__(self) -> None:
        self.started: list[str] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, str]:
        self.started.append(kwargs["QueryString"])
        return {"QueryExecutionId": "qid-1"}

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}


@pytest.fixture
def athena(monkeypatch: pytest.MonkeyPatch) -> FakeAthena:
    client = FakeAthena()
    monkeypatch.setattr(iceberg_ddl_handler, "_athena_client", client)
    return client


def test_create_runs_the_create_table_ddl(athena: FakeAthena) -> None:
    result = iceberg_ddl_handler.handler(
        {"RequestType": "Create", "ResourceProperties": PROPERTIES}, None
    )
    assert "CREATE TABLE IF NOT EXISTS" in athena.started[0]
    assert result["PhysicalResourceId"] == "test_db.records_iceberg"


def test_delete_drops_the_table_when_the_policy_is_destroy(
    athena: FakeAthena,
) -> None:
    iceberg_ddl_handler.handler(
        {"RequestType": "Delete", "ResourceProperties": PROPERTIES}, None
    )
    assert athena.started == ['DROP TABLE IF EXISTS "test_db"."records_iceberg"']


def test_delete_leaves_the_table_when_the_policy_is_retain(
    athena: FakeAthena,
) -> None:
    properties = {**PROPERTIES, "RemovalPolicy": "retain"}
    iceberg_ddl_handler.handler(
        {"RequestType": "Delete", "ResourceProperties": properties}, None
    )
    assert athena.started == []


def test_update_adds_a_column_for_a_new_partition_key(athena: FakeAthena) -> None:
    old_properties = {**PROPERTIES, "PartitionKeyNames": "job_type"}
    iceberg_ddl_handler.handler(
        {
            "RequestType": "Update",
            "ResourceProperties": PROPERTIES,
            "OldResourceProperties": old_properties,
        },
        None,
    )
    assert athena.started == [
        'ALTER TABLE "test_db"."records_iceberg" ADD COLUMNS ("tile_id" string)'
    ]


def test_update_with_no_schema_change_issues_no_alter(athena: FakeAthena) -> None:
    iceberg_ddl_handler.handler(
        {
            "RequestType": "Update",
            "ResourceProperties": PROPERTIES,
            "OldResourceProperties": PROPERTIES,
        },
        None,
    )
    assert athena.started == []


def test_update_raises_on_a_dropped_partition_key_without_altering(
    athena: FakeAthena,
) -> None:
    new_properties = {**PROPERTIES, "PartitionKeyNames": "job_type"}
    with pytest.raises(IncompatibleSchemaChange, match="tile_id"):
        iceberg_ddl_handler.handler(
            {
                "RequestType": "Update",
                "ResourceProperties": new_properties,
                "OldResourceProperties": PROPERTIES,
            },
            None,
        )
    assert athena.started == []


def test_update_raises_on_a_swapped_partition_key_without_partial_application(
    athena: FakeAthena,
) -> None:
    new_properties = {**PROPERTIES, "PartitionKeyNames": "job_type,region"}
    with pytest.raises(IncompatibleSchemaChange, match="tile_id"):
        iceberg_ddl_handler.handler(
            {
                "RequestType": "Update",
                "ResourceProperties": new_properties,
                "OldResourceProperties": PROPERTIES,
            },
            None,
        )
    assert athena.started == []


def test_added_column_sql_emits_one_alter_per_new_column() -> None:
    statements = added_column_sql(
        database="test_db",
        table="records_iceberg",
        old_columns=[("job_type", "string"), ("attempt", "int")],
        new_columns=[("job_type", "string"), ("attempt", "int"), ("note", "string")],
    )
    assert statements == [
        'ALTER TABLE "test_db"."records_iceberg" ADD COLUMNS ("note" string)'
    ]


def test_added_column_sql_refuses_a_removed_column() -> None:
    with pytest.raises(IncompatibleSchemaChange, match="attempt"):
        added_column_sql(
            database="test_db",
            table="records_iceberg",
            old_columns=[("job_type", "string"), ("attempt", "int")],
            new_columns=[("job_type", "string")],
        )


def test_added_column_sql_refuses_a_retyped_column() -> None:
    with pytest.raises(IncompatibleSchemaChange, match="attempt"):
        added_column_sql(
            database="test_db",
            table="records_iceberg",
            old_columns=[("job_type", "string"), ("attempt", "int")],
            new_columns=[("job_type", "string"), ("attempt", "string")],
        )
