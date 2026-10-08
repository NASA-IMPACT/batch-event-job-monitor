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


def test_create_table_sql_backticks_the_table_and_leaves_columns_bare() -> None:
    """Backticks keep Athena on the parser that understands struct<...>.

    Double quotes switch it to one that does not, which is why a hyphenated
    table name is expressible and a hyphenated column name is not.
    """
    sql = create_table_sql(
        database="test_db",
        table="records-rollup",
        location="s3://test-bucket/records-rollup/table/",
        partition_key_names=PARTITION_KEY_NAMES,
    )
    assert "CREATE TABLE IF NOT EXISTS `test_db`.`records-rollup`" in sql
    assert "`job_type` string" in sql
    assert '"' not in sql
    assert "`events` array<struct<" in sql


@pytest.mark.parametrize("name", ["created_at", "started_at", "stopped_at"])
def test_batch_job_timestamps_are_timestamps_in_iceberg_and_strings_in_staging(
    name: str,
) -> None:
    assert dict(records_columns(PARTITION_KEY_NAMES))[name] == "timestamp"
    assert dict(staging_columns(PARTITION_KEY_NAMES))[name] == "string"


@pytest.mark.parametrize("name", ["created_at", "started_at", "stopped_at"])
def test_merge_parses_batch_job_timestamps(name: str) -> None:
    sql = merge_sql(
        database="test_db",
        records_table="records",
        staging_table="records-staging",
        run_id="12345678-1234-1234-1234-123456789abc",
        partition_key_names=PARTITION_KEY_NAMES,
    )
    parsed = f'CAST(from_iso8601_timestamp(s."{name}") AS timestamp(6))'
    assert f'"{name}" = {parsed}' in sql
    assert sql.count(parsed) == 2  # the UPDATE and the INSERT


def test_log_stream_name_is_declared_on_both_tables() -> None:
    records = dict(records_columns(PARTITION_KEY_NAMES))
    staging = dict(staging_columns(PARTITION_KEY_NAMES))
    assert records["log_stream_name"] == "string"
    assert staging["log_stream_name"] == "string"
