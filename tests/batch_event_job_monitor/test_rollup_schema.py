"""Tests for rollup schema and SQL generation."""

from __future__ import annotations

import pytest

from batch_event_job_monitor.rollup_schema import (
    InvalidRunId,
    create_table_sql,
    iceberg_columns,
    key_columns,
    merge_sql,
    reconcile_sql,
    staging_columns,
)

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]


def test_iceberg_columns_lead_with_partition_keys_in_order() -> None:
    columns = iceberg_columns(PARTITION_KEY_NAMES)
    assert [name for name, _ in columns[:3]] == PARTITION_KEY_NAMES
    assert all(col_type == "string" for _, col_type in columns[:3])


def test_iceberg_columns_carry_timestamp_types() -> None:
    columns = dict(iceberg_columns(PARTITION_KEY_NAMES))
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


def test_create_table_sql_declares_iceberg_partitioned_by_job_type() -> None:
    sql = create_table_sql(
        database="test_db",
        table="records_iceberg",
        location="s3://test-bucket/iceberg/records/",
        partition_key_names=PARTITION_KEY_NAMES,
    )
    assert 'CREATE TABLE IF NOT EXISTS "test_db"."records_iceberg"' in sql
    assert 'PARTITIONED BY ("job_type")' in sql
    assert "'table_type'='ICEBERG'" in sql
    assert "'format'='parquet'" in sql
    assert "LOCATION 's3://test-bucket/iceberg/records/'" in sql
    assert '"year_month" string' in sql
    assert '"rolled_up_at" timestamp' in sql


def _merge() -> str:
    return merge_sql(
        database="test_db",
        iceberg_table="records_iceberg",
        staging_table="records_staging",
        run_id="0f9d7a1c-2b3e-4c5d-8e9f-0a1b2c3d4e5f",
        partition_key_names=PARTITION_KEY_NAMES,
    )


def test_merge_on_clause_covers_every_key_column() -> None:
    sql = _merge()
    for column in ["job_type", "tile_id", "year_month", "input_entity_id", "attempt"]:
        assert f't."{column}" = s."{column}"' in sql


def test_merge_guards_against_stale_replay() -> None:
    sql = _merge()
    assert (
        'WHEN MATCHED AND from_iso8601_timestamp(s."last_event_timestamp") '
        '>= t."last_event_timestamp"'
    ) in sql


def test_merge_scopes_staging_to_the_run() -> None:
    sql = _merge()
    assert "WHERE run_id = '0f9d7a1c-2b3e-4c5d-8e9f-0a1b2c3d4e5f'" in sql


def test_merge_sets_rolled_up_at_on_both_branches() -> None:
    sql = _merge()
    assert '"rolled_up_at" = CAST(current_timestamp AS timestamp(6))' in sql
    assert sql.count("CAST(current_timestamp AS timestamp(6))") == 2
    assert "VALUES (" in sql
    assert sql.rstrip().endswith("CAST(current_timestamp AS timestamp(6)))")


def test_merge_casts_the_staged_timestamp_to_a_naive_timestamp() -> None:
    # last_event_timestamp is a zone-naive Iceberg column, but
    # from_iso8601_timestamp() returns a zoned timestamp; Trino only
    # coerces naive -> zoned implicitly, not the reverse, so the
    # assignment needs an explicit CAST to type-check.
    sql = _merge()
    assert (
        '"last_event_timestamp" = CAST(from_iso8601_timestamp('
        's."last_event_timestamp") AS timestamp(6))'
    ) in sql


def test_merge_rejects_a_run_id_that_is_not_a_uuid() -> None:
    with pytest.raises(InvalidRunId):
        merge_sql(
            database="test_db",
            iceberg_table="records_iceberg",
            staging_table="records_staging",
            run_id="'; DROP TABLE records_iceberg; --",
            partition_key_names=PARTITION_KEY_NAMES,
        )


def test_reconcile_selects_missing_and_stale_keys_from_latest_report() -> None:
    sql = reconcile_sql(
        database="test_db",
        iceberg_table="records_iceberg",
        inventory_table="records_inventory",
    )
    assert "LEFT JOIN" in sql
    assert 'ON t."source_key" = inv."key"' in sql
    assert 'inv."dt" = (SELECT max("dt") FROM "test_db"."records_inventory")' in sql
    assert 'inv."is_latest"' in sql
    assert 'NOT inv."is_delete_marker"' in sql
    assert (
        't."source_key" IS NULL OR inv."last_modified_date" > t."rolled_up_at"'
    ) in sql
