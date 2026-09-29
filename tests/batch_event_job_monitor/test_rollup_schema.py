"""Tests for rollup schema and SQL generation."""

from __future__ import annotations

import pytest

from batch_event_job_monitor.rollup_schema import (
    InvalidRunId,
    create_table_sql,
    key_columns,
    merge_sql,
    records_columns,
    staging_columns,
)

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]


def test_records_columns_lead_with_partition_keys_in_order() -> None:
    columns = records_columns(PARTITION_KEY_NAMES)
    assert [name for name, _ in columns[:3]] == PARTITION_KEY_NAMES
    assert all(col_type == "string" for _, col_type in columns[:3])


def test_records_columns_carry_timestamp_types() -> None:
    columns = dict(records_columns(PARTITION_KEY_NAMES))
    assert columns["last_event_timestamp"] == "timestamp"
    assert columns["rolled_up_at"] == "timestamp"


def test_staging_columns_use_string_timestamp_and_omit_rolled_up_at() -> None:
    columns = dict(staging_columns(PARTITION_KEY_NAMES))
    assert columns["last_event_timestamp"] == "string"
    assert "rolled_up_at" not in columns


def test_key_columns_are_partition_keys_plus_entity_and_attempt() -> None:
    assert key_columns(PARTITION_KEY_NAMES) == [
        "job_type",
        "tile_id",
        "year_month",
        "input_entity_id",
        "attempt",
    ]


def test_merge_rejects_a_run_id_that_is_not_a_uuid() -> None:
    with pytest.raises(InvalidRunId):
        merge_sql(
            database="test_db",
            records_table="records",
            staging_table="records-staging",
            run_id="'; DROP TABLE records; --",
            partition_key_names=PARTITION_KEY_NAMES,
        )


def test_create_table_sql_quotes_the_table_but_not_the_columns() -> None:
    """A quoted column name makes Athena reject the events column's struct.

    The table name is quoted so it may contain hyphens, which callers can
    supply through the table_name parameters.
    """
    sql = create_table_sql(
        database="test_db",
        table="records-rollup",
        location="s3://test-bucket/records-rollup/",
        partition_key_names=PARTITION_KEY_NAMES,
    )
    assert '"test_db"."records-rollup"' in sql
    assert '"events"' not in sql
    assert "events array<struct<" in sql


def test_log_stream_name_is_declared_on_both_tables() -> None:
    records = dict(records_columns(PARTITION_KEY_NAMES))
    staging = dict(staging_columns(PARTITION_KEY_NAMES))
    assert records["log_stream_name"] == "string"
    assert staging["log_stream_name"] == "string"
