"""Tests for the Iceberg table DDL custom resource handler."""

from __future__ import annotations

from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from batch_event_job_monitor.handlers import records_table_ddl_handler
from batch_event_job_monitor.handlers.records_table_ddl_handler import (
    IncompatibleSchemaChange,
    added_column_sql,
)
from batch_event_job_monitor.rollup_schema import records_columns

PROPERTIES = {
    "Database": "test_db",
    "Table": "records",
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
    monkeypatch.setattr(records_table_ddl_handler, "_athena_client", client)
    return client


class FakeGlue:
    """Stands in for the Glue client backing the live table's schema.

    columns=None simulates a table absent from the catalog: get_table
    raises EntityNotFoundException, matching real Glue behavior.
    """

    def __init__(self, columns: list[tuple[str, str]] | None) -> None:
        self._columns = columns

    def get_table(self, **kwargs: Any) -> dict[str, Any]:
        if self._columns is None:
            raise ClientError(
                {"Error": {"Code": "EntityNotFoundException", "Message": "no table"}},
                "GetTable",
            )
        return {
            "Table": {
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": name, "Type": col_type}
                        for name, col_type in self._columns
                    ]
                }
            }
        }


def _set_glue(
    monkeypatch: pytest.MonkeyPatch, columns: list[tuple[str, str]] | None
) -> None:
    monkeypatch.setattr(
        records_table_ddl_handler, "_glue_client", FakeGlue(columns), raising=False
    )


def test_create_runs_the_create_table_ddl(athena: FakeAthena) -> None:
    result = records_table_ddl_handler.handler(
        {"RequestType": "Create", "ResourceProperties": PROPERTIES}, None
    )
    assert "CREATE TABLE IF NOT EXISTS" in athena.started[0]
    assert result["PhysicalResourceId"] == "test_db.records"


def test_delete_drops_the_table_when_the_policy_is_destroy(
    athena: FakeAthena,
) -> None:
    records_table_ddl_handler.handler(
        {"RequestType": "Delete", "ResourceProperties": PROPERTIES}, None
    )
    assert len(athena.started) == 1
    assert athena.started[0].startswith("DROP TABLE IF EXISTS")


def test_delete_leaves_the_table_when_the_policy_is_retain(
    athena: FakeAthena,
) -> None:
    properties = {**PROPERTIES, "RemovalPolicy": "retain"}
    records_table_ddl_handler.handler(
        {"RequestType": "Delete", "ResourceProperties": properties}, None
    )
    assert athena.started == []


def test_update_adds_a_column_for_a_new_partition_key(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_glue(monkeypatch, records_columns(["job_type"]))
    records_table_ddl_handler.handler(
        {
            "RequestType": "Update",
            "ResourceProperties": PROPERTIES,
            "OldResourceProperties": {**PROPERTIES, "PartitionKeyNames": "job_type"},
        },
        None,
    )
    assert len(athena.started) == 1
    assert athena.started[0].startswith("ALTER TABLE")
    assert "tile_id" in athena.started[0]


def test_update_with_no_schema_change_issues_no_alter(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_glue(monkeypatch, records_columns(["job_type", "tile_id"]))
    records_table_ddl_handler.handler(
        {
            "RequestType": "Update",
            "ResourceProperties": PROPERTIES,
            "OldResourceProperties": PROPERTIES,
        },
        None,
    )
    assert athena.started == []


def test_update_raises_on_a_dropped_partition_key_without_altering(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_glue(monkeypatch, records_columns(["job_type", "tile_id"]))
    new_properties = {**PROPERTIES, "PartitionKeyNames": "job_type"}
    with pytest.raises(IncompatibleSchemaChange, match="tile_id"):
        records_table_ddl_handler.handler(
            {
                "RequestType": "Update",
                "ResourceProperties": new_properties,
                "OldResourceProperties": PROPERTIES,
            },
            None,
        )
    assert athena.started == []


def test_update_raises_on_a_swapped_partition_key_without_partial_application(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_glue(monkeypatch, records_columns(["job_type", "tile_id"]))
    new_properties = {**PROPERTIES, "PartitionKeyNames": "job_type,region"}
    with pytest.raises(IncompatibleSchemaChange, match="tile_id"):
        records_table_ddl_handler.handler(
            {
                "RequestType": "Update",
                "ResourceProperties": new_properties,
                "OldResourceProperties": PROPERTIES,
            },
            None,
        )
    assert athena.started == []


def test_update_raises_on_a_live_column_retyped_since_the_last_deploy(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The live table's "attempt" column is bigint. The code running now
    # would declare "attempt" as int (see rollup_schema._BODY_COLUMNS).
    # Nothing in ResourceProperties changed -- this drift can only be seen
    # by reading the deployed table's actual catalog schema.
    deployed = [
        (name, "bigint" if name == "attempt" else col_type)
        for name, col_type in records_columns(
            PROPERTIES["PartitionKeyNames"].split(",")
        )
    ]
    _set_glue(monkeypatch, deployed)

    with pytest.raises(IncompatibleSchemaChange, match="attempt"):
        records_table_ddl_handler.handler(
            {
                "RequestType": "Update",
                "ResourceProperties": PROPERTIES,
                "OldResourceProperties": PROPERTIES,
            },
            None,
        )
    assert athena.started == []


def test_update_falls_back_to_create_when_the_table_is_not_yet_in_the_catalog(
    athena: FakeAthena, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_glue(monkeypatch, None)

    records_table_ddl_handler.handler(
        {
            "RequestType": "Update",
            "ResourceProperties": PROPERTIES,
            "OldResourceProperties": PROPERTIES,
        },
        None,
    )
    assert "CREATE TABLE IF NOT EXISTS" in athena.started[0]


def test_deployed_columns_raises_diagnosably_on_a_malformed_response() -> None:
    class BrokenGlue:
        def get_table(self, **kwargs: Any) -> dict[str, Any]:
            return {"Table": {}}

    with pytest.raises(RuntimeError, match="StorageDescriptor"):
        records_table_ddl_handler._deployed_columns(
            glue_client=cast(Any, BrokenGlue()),
            database="test_db",
            table="records",
        )


def test_added_column_sql_emits_one_alter_per_new_column() -> None:
    statements = added_column_sql(
        database="test_db",
        table="records",
        old_columns=[("job_type", "string"), ("attempt", "int")],
        new_columns=[("job_type", "string"), ("attempt", "int"), ("note", "string")],
    )
    assert statements == ['ALTER TABLE "test_db"."records" ADD COLUMNS (note string)']


def test_added_column_sql_refuses_a_removed_column() -> None:
    with pytest.raises(IncompatibleSchemaChange, match="attempt"):
        added_column_sql(
            database="test_db",
            table="records",
            old_columns=[("job_type", "string"), ("attempt", "int")],
            new_columns=[("job_type", "string")],
        )


def test_added_column_sql_refuses_a_retyped_column() -> None:
    with pytest.raises(IncompatibleSchemaChange, match="attempt"):
        added_column_sql(
            database="test_db",
            table="records",
            old_columns=[("job_type", "string"), ("attempt", "int")],
            new_columns=[("job_type", "string"), ("attempt", "string")],
        )


def test_added_column_sql_tolerates_glues_type_string_normalization() -> None:
    # Glue's catalog-normalized type string need not match the hand-written
    # one in rollup_schema.py byte for byte -- differing case and internal
    # whitespace within a nested type is not a real retype.
    statements = added_column_sql(
        database="test_db",
        table="records",
        old_columns=[
            ("job_type", "string"),
            (
                "events",
                "ARRAY<STRUCT<state: string, timestamp: string>>",
            ),
        ],
        new_columns=[
            ("job_type", "string"),
            ("events", "array<struct<state:string,timestamp:string>>"),
        ],
    )
    assert statements == []
