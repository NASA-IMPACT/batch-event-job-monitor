# Iceberg Records Rollup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Roll the small per-attempt JSON objects under `records/` into an Apache Iceberg table that supports wide,
cross-partition Athena scans, and retire the JSON-backed Glue table.

**Architecture:** S3 EventBridge notifications on `records/` fan into an SQS queue. An hourly Lambda drains the queue to
a distinct key set, GETs those objects, concatenates them into one gzipped NDJSON staging object, and runs an Athena
`MERGE INTO` against the Iceberg table. A weekly reconcile mode anti-joins the `records/` S3 Inventory against the
Iceberg table and re-enqueues anything missing or stale; run against a cold table it is also the backfill. Both modes
self-invoke to continue past a single invocation's budget.

**Tech Stack:** Python 3.12, boto3, pytest, moto, aws-cdk-lib (Glue, Lambda, SQS, Events, Athena), Apache Iceberg via
the Glue Data Catalog, Athena SQL.

**Spec:** `docs/superpowers/specs/2026-09-22-iceberg-records-rollup-design.md`

## Global Constraints

- **ASCII only** in all code, comments, and docstrings. No Unicode punctuation - use `-` not en/em dashes, `x` not the
  multiplication sign.
- **No narration comments.** Do not write comments explaining why a change was made, what it replaced, or what was
  considered. Only document non-obvious constraints that remain true regardless of history.
- **Numpydoc docstrings** on every public function and class, matching `log_store.py` and `untracked.py`.
- `from __future__ import annotations` at the top of every module.
- **Line length 88** (ruff). Lint is `./scripts/lint`, types `./scripts/typecheck`, tests `./scripts/test`.
- **Markdown files under `docs/` are prettier-checked by `./scripts/lint`.** Run
  `npx --yes prettier@3 --write "docs/**/*.md"` after editing any doc.
- `aws-cdk-lib>=2.262.0` is already pinned and supports `lambda.RecursiveLoop`.
- Metrics are emitted as **Embedded Metric Format records on stdout**, namespace `BatchEventJobMonitor`, following
  `untracked.py`. Never call `PutMetricData`.
- The **natural key** is the ordered partition-key list (whose first entry is `job_type`) plus `input_entity_id` plus
  `attempt`.
- Every canonical record body already contains every field needed. `job_type` is a top-level key; the remaining
  partition values live in the `partition_fields` dict. No S3 key parsing is required for data - only `source_key` comes
  from the key itself.

---

## File Structure

| file                                                          | responsibility                                                                                                                                                                  |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/batch_event_job_monitor/rollup_schema.py`                | Column definitions and all SQL generation (CREATE, MERGE, reconcile). Pure; no boto3. Imported by both the Lambda and the CDK constructs so the schema has one source of truth. |
| `src/batch_event_job_monitor/rollup.py`                       | Runtime pipeline: SQS drain, object fetch, NDJSON staging, Athena execution, chaining decisions, metrics.                                                                       |
| `src/batch_event_job_monitor/handlers/rollup_handler.py`      | Thin dispatch on `mode`.                                                                                                                                                        |
| `src/batch_event_job_monitor/handlers/iceberg_ddl_handler.py` | CloudFormation custom resource handler running table DDL.                                                                                                                       |
| `src/batch_event_job_monitor_cdk/iceberg_records_table.py`    | Iceberg table custom resource, staging Glue table, `records/` inventory table, table optimizer.                                                                                 |
| `src/batch_event_job_monitor_cdk/records_rollup_function.py`  | Queue, DLQ, EventBridge rule, both Lambda functions, schedules, grants.                                                                                                         |
| `src/batch_event_job_monitor_cdk/athena_records_table.py`     | **Deleted** in the final task.                                                                                                                                                  |

---

## Task 1: Rollup schema and CREATE TABLE DDL

**Files:**

- Create: `src/batch_event_job_monitor/rollup_schema.py`
- Test: `tests/batch_event_job_monitor/test_rollup_schema.py`

**Interfaces:**

- Consumes: nothing.
- Produces: `EVENTS_TYPE: str`; `iceberg_columns(partition_key_names: list[str]) -> list[tuple[str, str]]`;
  `staging_columns(partition_key_names: list[str]) -> list[tuple[str, str]]`;
  `key_columns(partition_key_names: list[str]) -> list[str]`;
  `create_table_sql(*, database: str, table: str, location: str, partition_key_names: list[str]) -> str`.

- [ ] **Step 1: Write the failing test**

```python
"""Tests for rollup schema and SQL generation."""

from __future__ import annotations

from batch_event_job_monitor.rollup_schema import (
    create_table_sql,
    iceberg_columns,
    key_columns,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_schema.py -v` Expected: FAIL with
`ModuleNotFoundError: No module named 'batch_event_job_monitor.rollup_schema'`

- [ ] **Step 3: Write minimal implementation**

```python
"""Column definitions and SQL generation for the Iceberg records rollup.

Shared by the rollup Lambda and the CDK constructs so the rolled-up schema
is declared exactly once. Partition keys arrive as plain names rather than
PartitionKeySpec objects, keeping this module free of any CDK import.
"""

from __future__ import annotations

EVENTS_TYPE = (
    "array<struct<state:string,timestamp:string,"
    "batch_job_id:string,exit_code:int>>"
)

# Columns common to the Iceberg table and its NDJSON staging table, in the
# order both tables declare them.
_BODY_COLUMNS: list[tuple[str, str]] = [
    ("input_entity_id", "string"),
    ("attempt", "int"),
    ("output_entity_id", "string"),
    ("batch_job_id", "string"),
    ("current_state", "string"),
    ("events", EVENTS_TYPE),
]


def key_columns(partition_key_names: list[str]) -> list[str]:
    """Natural-key column names, in order.

    Parameters
    ----------
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    list[str]
        Partition keys followed by input_entity_id and attempt.
    """
    return [*partition_key_names, "input_entity_id", "attempt"]


def iceberg_columns(partition_key_names: list[str]) -> list[tuple[str, str]]:
    """(name, Athena type) pairs for the Iceberg table, in declaration order.

    Parameters
    ----------
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    list[tuple[str, str]]
        Partition key columns, body columns, then provenance columns.
    """
    return [
        *((name, "string") for name in partition_key_names),
        *_BODY_COLUMNS,
        ("last_event_timestamp", "timestamp"),
        ("source_key", "string"),
        ("rolled_up_at", "timestamp"),
    ]


def staging_columns(partition_key_names: list[str]) -> list[tuple[str, str]]:
    """(name, Athena type) pairs for the NDJSON staging table.

    Differs from the Iceberg table in two ways: last_event_timestamp stays a
    string because the JSON SerDe does not parse timestamps, and rolled_up_at
    is absent because the MERGE sets it.

    Parameters
    ----------
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    list[tuple[str, str]]
        Partition key columns, body columns, then staging provenance columns.
    """
    return [
        *((name, "string") for name in partition_key_names),
        *_BODY_COLUMNS,
        ("last_event_timestamp", "string"),
        ("source_key", "string"),
    ]


def create_table_sql(
    *,
    database: str,
    table: str,
    location: str,
    partition_key_names: list[str],
) -> str:
    """Build the Athena DDL creating the Iceberg records table.

    No sort order is declared: Athena supports neither WRITE ORDERED BY nor a
    sorted_by table property for Iceberg.

    Parameters
    ----------
    database : str
        Glue database name.
    table : str
        Iceberg table name.
    location : str
        s3:// URI of the table's data location.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    str
        A CREATE TABLE IF NOT EXISTS statement.
    """
    column_sql = ",\n    ".join(
        f'"{name}" {col_type}' for name, col_type in iceberg_columns(partition_key_names)
    )
    return (
        f'CREATE TABLE IF NOT EXISTS "{database}"."{table}" (\n'
        f"    {column_sql}\n"
        f")\n"
        f'PARTITIONED BY ("{partition_key_names[0]}")\n'
        f"LOCATION '{location}'\n"
        f"TBLPROPERTIES ("
        f"'table_type'='ICEBERG', "
        f"'format'='parquet', "
        f"'format-version'='2'"
        f")"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_schema.py -v` Expected: 5 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup_schema.py tests/batch_event_job_monitor/test_rollup_schema.py
git commit -m "feat: add rollup schema definitions and Iceberg CREATE TABLE DDL"
```

---

## Task 2: MERGE and reconcile SQL generation

**Files:**

- Modify: `src/batch_event_job_monitor/rollup_schema.py`
- Test: `tests/batch_event_job_monitor/test_rollup_schema.py`

**Interfaces:**

- Consumes: `key_columns`, `iceberg_columns`, `staging_columns` from Task 1.
- Produces:
  `merge_sql(*, database: str, iceberg_table: str, staging_table: str, run_id: str, partition_key_names: list[str]) -> str`;
  `reconcile_sql(*, database: str, iceberg_table: str, inventory_table: str) -> str`; `InvalidRunId(ValueError)`.

- [ ] **Step 1: Write the failing test**

```python
import pytest

from batch_event_job_monitor.rollup_schema import (
    InvalidRunId,
    merge_sql,
    reconcile_sql,
)


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
        "WHEN MATCHED AND from_iso8601_timestamp(s.\"last_event_timestamp\") "
        '>= t."last_event_timestamp"'
    ) in sql


def test_merge_scopes_staging_to_the_run() -> None:
    sql = _merge()
    assert "WHERE run_id = '0f9d7a1c-2b3e-4c5d-8e9f-0a1b2c3d4e5f'" in sql


def test_merge_sets_rolled_up_at_on_both_branches() -> None:
    sql = _merge()
    assert '"rolled_up_at" = current_timestamp' in sql
    assert "current_timestamp\n)" in sql or "current_timestamp)" in sql


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_schema.py -v` Expected: FAIL with
`ImportError: cannot import name 'merge_sql'`

- [ ] **Step 3: Write the implementation**

Append to `rollup_schema.py`:

```python
import re

# Run ids are interpolated into SQL, so they are constrained to the UUID
# shape the rollup generates rather than quoted and hoped for.
_RUN_ID_PATTERN = re.compile(r"\A[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


class InvalidRunId(ValueError):
    """Raised when a run id is not a lowercase hyphenated UUID."""


def _staging_expression(name: str) -> str:
    if name == "last_event_timestamp":
        return 'from_iso8601_timestamp(s."last_event_timestamp")'
    return f's."{name}"'


def merge_sql(
    *,
    database: str,
    iceberg_table: str,
    staging_table: str,
    run_id: str,
    partition_key_names: list[str],
) -> str:
    """Build the MERGE upserting one staging run into the Iceberg table.

    Parameters
    ----------
    database : str
        Glue database holding both tables.
    iceberg_table : str
        Iceberg table name (the MERGE target).
    staging_table : str
        NDJSON staging table name (the MERGE source).
    run_id : str
        Staging partition value for this run. Must be a lowercase hyphenated
        UUID.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    str
        A MERGE INTO statement.

    Raises
    ------
    InvalidRunId
        If run_id is not a lowercase hyphenated UUID.
    """
    if not _RUN_ID_PATTERN.match(run_id):
        raise InvalidRunId(f"run_id is not a lowercase hyphenated UUID: {run_id!r}")

    keys = key_columns(partition_key_names)
    on_clause = "\n   AND ".join(f't."{column}" = s."{column}"' for column in keys)

    updatable = [
        name
        for name, _ in iceberg_columns(partition_key_names)
        if name not in keys and name != "rolled_up_at"
    ]
    set_clause = ",\n               ".join(
        f'"{name}" = {_staging_expression(name)}' for name in updatable
    )

    insert_names = [name for name, _ in iceberg_columns(partition_key_names)]
    insert_columns = ", ".join(f'"{name}"' for name in insert_names)
    insert_values = ", ".join(
        "current_timestamp" if name == "rolled_up_at" else _staging_expression(name)
        for name in insert_names
    )

    return (
        f'MERGE INTO "{database}"."{iceberg_table}" t\n'
        f'USING (SELECT * FROM "{database}"."{staging_table}" '
        f"WHERE run_id = '{run_id}') s\n"
        f"    ON {on_clause}\n"
        f'WHEN MATCHED AND from_iso8601_timestamp(s."last_event_timestamp") '
        f'>= t."last_event_timestamp" THEN\n'
        f"    UPDATE SET {set_clause},\n"
        f'               "rolled_up_at" = current_timestamp\n'
        f"WHEN NOT MATCHED THEN\n"
        f"    INSERT ({insert_columns})\n"
        f"    VALUES ({insert_values})"
    )


def reconcile_sql(
    *,
    database: str,
    iceberg_table: str,
    inventory_table: str,
) -> str:
    """Build the query selecting keys missing from or stale in the table.

    Compares the newest inventory report against the rolled-up table. An
    object whose last_modified_date is later than the row's rolled_up_at was
    rewritten after it was merged. Clock skew can make that comparison fire
    spuriously; a spurious re-merge is idempotent.

    Parameters
    ----------
    database : str
        Glue database holding both tables.
    iceberg_table : str
        Iceberg table name.
    inventory_table : str
        S3-inventory table over the records/ prefix.

    Returns
    -------
    str
        A SELECT returning one source_key column.
    """
    return (
        f'SELECT inv."key" AS source_key\n'
        f'FROM "{database}"."{inventory_table}" inv\n'
        f'LEFT JOIN "{database}"."{iceberg_table}" t\n'
        f'    ON t."source_key" = inv."key"\n'
        f'WHERE inv."dt" = (SELECT max("dt") FROM "{database}"."{inventory_table}")\n'
        f'  AND inv."is_latest"\n'
        f'  AND NOT inv."is_delete_marker"\n'
        f'  AND (t."source_key" IS NULL '
        f'OR inv."last_modified_date" > t."rolled_up_at")'
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_schema.py -v` Expected: 11 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup_schema.py tests/batch_event_job_monitor/test_rollup_schema.py
git commit -m "feat: add MERGE and reconcile SQL generation"
```

---

## Task 3: Record flattening

**Files:**

- Create: `src/batch_event_job_monitor/rollup.py`
- Test: `tests/batch_event_job_monitor/test_rollup.py`

**Interfaces:**

- Consumes: nothing from earlier tasks.
- Produces: `record_to_row(*, source_key: str, body: dict[str, Any], partition_key_names: list[str]) -> dict[str, Any]`;
  `MalformedRecord(ValueError)`.

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the records rollup runtime."""

from __future__ import annotations

from typing import Any

import pytest

from batch_event_job_monitor.rollup import MalformedRecord, record_to_row

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]

SOURCE_KEY = (
    "records/job_type=monthly-composite/tile_id=12TVK/year_month=2024-06/"
    "input_entity_id=12TVK_2024-06_source/001.json"
)


def _body() -> dict[str, Any]:
    return {
        "input_entity_id": "12TVK_2024-06_source",
        "output_entity_id": "HLS.COMPOSITE.T12TVK.202406.v2.0",
        "job_type": "monthly-composite",
        "partition_fields": {"tile_id": "12TVK", "year_month": "2024-06"},
        "attempt": 1,
        "batch_job_id": "abc-123",
        "current_state": "SUCCESS",
        "events": [
            {"state": "SUBMITTED", "timestamp": "2026-09-22T10:00:00+00:00"},
            {
                "state": "SUCCESS",
                "timestamp": "2026-09-22T10:05:00+00:00",
                "batch_job_id": "abc-123",
                "exit_code": 0,
            },
        ],
    }


def test_row_flattens_partition_fields_into_columns() -> None:
    row = record_to_row(
        source_key=SOURCE_KEY, body=_body(), partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["job_type"] == "monthly-composite"
    assert row["tile_id"] == "12TVK"
    assert row["year_month"] == "2024-06"


def test_row_takes_last_event_timestamp_from_the_final_event() -> None:
    row = record_to_row(
        source_key=SOURCE_KEY, body=_body(), partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["last_event_timestamp"] == "2026-09-22T10:05:00+00:00"


def test_row_carries_source_key_and_preserves_events_verbatim() -> None:
    body = _body()
    row = record_to_row(
        source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["source_key"] == SOURCE_KEY
    assert row["events"] == body["events"]


def test_row_tolerates_events_missing_optional_fields() -> None:
    body = _body()
    body["events"] = [{"state": "SUBMITTED", "timestamp": "2026-09-22T10:00:00+00:00"}]
    row = record_to_row(
        source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["events"][0] == {
        "state": "SUBMITTED",
        "timestamp": "2026-09-22T10:00:00+00:00",
    }


def test_row_rejects_a_record_with_no_events() -> None:
    body = _body()
    body["events"] = []
    with pytest.raises(MalformedRecord):
        record_to_row(
            source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
        )


def test_row_rejects_a_record_missing_a_declared_partition_field() -> None:
    body = _body()
    del body["partition_fields"]["year_month"]
    with pytest.raises(MalformedRecord):
        record_to_row(
            source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ModuleNotFoundError: No module named 'batch_event_job_monitor.rollup'`

- [ ] **Step 3: Write the implementation**

```python
"""Rolling canonical record objects up into the Iceberg records table."""

from __future__ import annotations

from typing import Any


class MalformedRecord(ValueError):
    """Raised when a canonical record cannot be flattened into a row."""


def record_to_row(
    *,
    source_key: str,
    body: dict[str, Any],
    partition_key_names: list[str],
) -> dict[str, Any]:
    """Flatten one canonical record body into a staging row.

    job_type is a top-level field of the record body; every other partition
    key is read from the body's partition_fields dict.

    Parameters
    ----------
    source_key : str
        S3 key the record was read from.
    body : dict[str, Any]
        Parsed canonical record JSON.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    dict[str, Any]
        One row, keyed by staging column name.

    Raises
    ------
    MalformedRecord
        If the record has no events, or is missing a declared partition key.
    """
    events = body.get("events") or []
    if not events:
        raise MalformedRecord(f"record has no events: {source_key}")

    partition_fields = body.get("partition_fields") or {}
    row: dict[str, Any] = {}
    for name in partition_key_names:
        value = body.get(name) if name == "job_type" else partition_fields.get(name)
        if value is None:
            raise MalformedRecord(
                f"record is missing partition key {name!r}: {source_key}"
            )
        row[name] = value

    row.update(
        {
            "input_entity_id": body.get("input_entity_id"),
            "attempt": body.get("attempt"),
            "output_entity_id": body.get("output_entity_id"),
            "batch_job_id": body.get("batch_job_id"),
            "current_state": body.get("current_state"),
            "events": events,
            "last_event_timestamp": events[-1]["timestamp"],
            "source_key": source_key,
        }
    )
    return row
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 6 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/test_rollup.py
git commit -m "feat: flatten canonical records into rollup staging rows"
```

---

## Task 4: SQS drain with dedup and cap

**Files:**

- Modify: `src/batch_event_job_monitor/rollup.py`
- Test: `tests/batch_event_job_monitor/test_rollup.py`
- Modify: `tests/batch_event_job_monitor/conftest.py`

**Interfaces:**

- Consumes: nothing.
- Produces: `DrainedKeys` dataclass with fields `keys: list[str]` and `receipt_handles: list[str]`;
  `drain_keys(*, sqs_client: Any, queue_url: str, max_keys: int) -> DrainedKeys`;
  `queue_has_messages(*, sqs_client: Any, queue_url: str) -> bool`.

Notification bodies are S3 EventBridge events; the key is at `detail.object.key`.

- [ ] **Step 1: Write the failing test**

Add to `conftest.py`:

```python
@pytest.fixture
def rollup_queue_url(sqs: SQSClient) -> str:
    return sqs.create_queue(QueueName="test-rollup-queue")["QueueUrl"]
```

Add to `test_rollup.py`:

```python
import json

from mypy_boto3_sqs import SQSClient

from batch_event_job_monitor.rollup import drain_keys, queue_has_messages


def _send(sqs: SQSClient, queue_url: str, key: str) -> None:
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"detail": {"object": {"key": key}}}),
    )


def test_drain_deduplicates_repeated_keys(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    for _ in range(3):
        _send(sqs, rollup_queue_url, "records/job_type=a/001.json")

    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=100)

    assert drained.keys == ["records/job_type=a/001.json"]
    assert len(drained.receipt_handles) == 3


def test_drain_stops_at_the_cap(sqs: SQSClient, rollup_queue_url: str) -> None:
    for index in range(12):
        _send(sqs, rollup_queue_url, f"records/job_type=a/{index:03d}.json")

    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=5)

    assert len(drained.keys) <= 5


def test_drain_of_an_empty_queue_returns_nothing(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=100)
    assert drained.keys == []
    assert drained.receipt_handles == []


def test_queue_has_messages_reflects_queue_state(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    _send(sqs, rollup_queue_url, "records/job_type=a/001.json")
    assert queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ImportError: cannot import name 'drain_keys'`

- [ ] **Step 3: Write the implementation**

Append to `rollup.py`:

```python
import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_RECEIVE_BATCH = 10


@dataclass
class DrainedKeys:
    """Distinct object keys drained from the rollup queue.

    Attributes
    ----------
    keys : list[str]
        Distinct S3 keys, in first-seen order.
    receipt_handles : list[str]
        Every receipt handle drained, including those for duplicate keys.
        All of them are deleted together once the merge succeeds.
    """

    keys: list[str] = field(default_factory=list)
    receipt_handles: list[str] = field(default_factory=list)


def _key_from_body(body: str) -> str | None:
    try:
        return str(json.loads(body)["detail"]["object"]["key"])
    except (json.JSONDecodeError, KeyError, TypeError):
        logger.warning("Skipping notification with no object key")
        return None


def drain_keys(*, sqs_client: Any, queue_url: str, max_keys: int) -> DrainedKeys:
    """Drain notifications into a distinct key set.

    Stops once max_keys distinct keys have been seen; anything left stays on
    the queue and surfaces as queue depth.

    Parameters
    ----------
    sqs_client : Any
        Boto3 SQS client.
    queue_url : str
        Rollup queue URL.
    max_keys : int
        Per-run cap on distinct keys.

    Returns
    -------
    DrainedKeys
        Distinct keys and every receipt handle that produced them.
    """
    drained = DrainedKeys()
    seen: set[str] = set()

    while len(seen) < max_keys:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=_RECEIVE_BATCH,
            WaitTimeSeconds=1,
        )
        messages = response.get("Messages", [])
        if not messages:
            break

        for message in messages:
            drained.receipt_handles.append(message["ReceiptHandle"])
            key = _key_from_body(message["Body"])
            if key is not None and key not in seen:
                seen.add(key)
                drained.keys.append(key)

    return drained


def queue_has_messages(*, sqs_client: Any, queue_url: str) -> bool:
    """Report whether the queue still holds visible messages.

    Parameters
    ----------
    sqs_client : Any
        Boto3 SQS client.
    queue_url : str
        Rollup queue URL.

    Returns
    -------
    bool
        True if ApproximateNumberOfMessages is greater than zero.
    """
    attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"]) > 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 10 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/
git commit -m "feat: drain rollup queue into a deduplicated key set"
```

---

## Task 5: Object fetch and NDJSON staging

**Files:**

- Modify: `src/batch_event_job_monitor/rollup.py`
- Test: `tests/batch_event_job_monitor/test_rollup.py`

**Interfaces:**

- Consumes: `record_to_row`, `MalformedRecord` (Task 3).
- Produces: `FetchResult` dataclass with `rows: list[dict[str, Any]]`, `missing: int`, `malformed: int`;
  `fetch_rows(*, s3_client: Any, bucket: str, keys: list[str], partition_key_names: list[str], max_workers: int = 32) -> FetchResult`;
  `write_staging_object(*, s3_client: Any, bucket: str, staging_prefix: str, run_id: str, rows: list[dict[str, Any]]) -> str`.

- [ ] **Step 1: Write the failing test**

```python
import gzip

from mypy_boto3_s3 import S3Client

from batch_event_job_monitor.rollup import fetch_rows, write_staging_object

RUN_ID = "0f9d7a1c-2b3e-4c5d-8e9f-0a1b2c3d4e5f"


def _put_record(s3: S3Client, bucket: str, key: str, body: dict[str, Any]) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body).encode())


def test_fetch_returns_one_row_per_object(s3: S3Client, bucket: str) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=[SOURCE_KEY],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert len(result.rows) == 1
    assert result.rows[0]["source_key"] == SOURCE_KEY
    assert result.missing == 0
    assert result.malformed == 0


def test_fetch_skips_and_counts_a_missing_object(s3: S3Client, bucket: str) -> None:
    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=["records/job_type=a/gone.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.missing == 1


def test_fetch_skips_and_counts_malformed_json(s3: S3Client, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key="records/bad.json", Body=b"{not json")

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=["records/bad.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.malformed == 1


def test_staging_object_is_gzipped_ndjson(s3: S3Client, bucket: str) -> None:
    rows = [
        record_to_row(
            source_key=SOURCE_KEY,
            body=_body(),
            partition_key_names=PARTITION_KEY_NAMES,
        )
    ]

    key = write_staging_object(
        s3_client=s3,
        bucket=bucket,
        staging_prefix="staging/",
        run_id=RUN_ID,
        rows=rows,
    )

    assert key == f"staging/run_id={RUN_ID}/part.ndjson.gz"
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    lines = gzip.decompress(raw).decode().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["tile_id"] == "12TVK"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ImportError: cannot import name 'fetch_rows'`

- [ ] **Step 3: Write the implementation**

Append to `rollup.py`:

```python
import gzip
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError

_MISSING_OBJECT_CODES = ("NoSuchKey", "404", "NotFound")


@dataclass
class FetchResult:
    """Outcome of fetching a batch of canonical records.

    Attributes
    ----------
    rows : list[dict[str, Any]]
        Successfully flattened rows.
    missing : int
        Objects that no longer exist. Not an error: the object was deleted
        between notification and read.
    malformed : int
        Objects that could not be parsed or flattened.
    """

    rows: list[dict[str, Any]] = field(default_factory=list)
    missing: int = 0
    malformed: int = 0


def fetch_rows(
    *,
    s3_client: Any,
    bucket: str,
    keys: list[str],
    partition_key_names: list[str],
    max_workers: int = 32,
) -> FetchResult:
    """Fetch and flatten canonical records concurrently.

    Parameters
    ----------
    s3_client : Any
        Boto3 S3 client.
    bucket : str
        Processing bucket name.
    keys : list[str]
        Distinct record object keys.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.
    max_workers : int, optional
        Thread pool size. Defaults to 32.

    Returns
    -------
    FetchResult
        Rows plus counts of missing and malformed objects.
    """

    def fetch_one(key: str) -> dict[str, Any] | str:
        try:
            raw = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if exc.response["Error"]["Code"] in _MISSING_OBJECT_CODES:
                return "missing"
            raise
        try:
            return record_to_row(
                source_key=key,
                body=json.loads(raw),
                partition_key_names=partition_key_names,
            )
        except (json.JSONDecodeError, MalformedRecord, KeyError):
            logger.warning("Skipping unreadable record %s", key)
            return "malformed"

    result = FetchResult()
    if not keys:
        return result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for outcome in pool.map(fetch_one, keys):
            if outcome == "missing":
                result.missing += 1
            elif outcome == "malformed":
                result.malformed += 1
            else:
                result.rows.append(outcome)
    return result


def write_staging_object(
    *,
    s3_client: Any,
    bucket: str,
    staging_prefix: str,
    run_id: str,
    rows: list[dict[str, Any]],
) -> str:
    """Write rows as one gzipped NDJSON object under the staging prefix.

    The gzip stream is written to a temporary file rather than buffered in
    memory, so a large backlog cannot exhaust the Lambda heap.

    Parameters
    ----------
    s3_client : Any
        Boto3 S3 client.
    bucket : str
        Processing bucket name.
    staging_prefix : str
        Key prefix for staging objects, with a trailing slash.
    run_id : str
        Staging partition value for this run.
    rows : list[dict[str, Any]]
        Rows to write.

    Returns
    -------
    str
        The S3 key written.
    """
    key = f"{staging_prefix}run_id={run_id}/part.ndjson.gz"
    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as handle:
        path = handle.name
    try:
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row))
                stream.write("\n")
        with open(path, "rb") as body:
            s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    finally:
        os.unlink(path)
    return key
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 14 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/test_rollup.py
git commit -m "feat: fetch records concurrently and stage them as gzipped NDJSON"
```

---

## Task 6: Athena query execution

**Files:**

- Modify: `src/batch_event_job_monitor/rollup.py`
- Modify: `pyproject.toml:21` (add `athena` and `lambda` to the `boto3-stubs` extras)
- Test: `tests/batch_event_job_monitor/test_rollup.py`

**Interfaces:**

- Consumes: nothing.
- Produces: `AthenaQueryError(RuntimeError)`;
  `run_query(*, athena_client: Any, sql: str, workgroup: str, poll_seconds: float = 1.0, sleep: Callable[[float], None] = time.sleep) -> str`
  returning the query execution id.

- [ ] **Step 1: Write the failing test**

```python
from typing import Callable

import pytest

from batch_event_job_monitor.rollup import AthenaQueryError, run_query


class FakeAthena:
    """Minimal Athena client returning a scripted sequence of states."""

    def __init__(self, states: list[str]) -> None:
        self.states = states
        self.started: list[str] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, str]:
        self.started.append(kwargs["QueryString"])
        return {"QueryExecutionId": "qid-1"}

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        state = self.states.pop(0)
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": "boom"}
            }
        }


def test_run_query_returns_the_execution_id_on_success() -> None:
    client = FakeAthena(["RUNNING", "SUCCEEDED"])
    query_id = run_query(
        athena_client=client,
        sql="SELECT 1",
        workgroup="wg",
        sleep=lambda _: None,
    )
    assert query_id == "qid-1"
    assert client.started == ["SELECT 1"]


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_run_query_raises_on_a_terminal_failure(state: str) -> None:
    client = FakeAthena([state])
    with pytest.raises(AthenaQueryError, match="boom"):
        run_query(
            athena_client=client,
            sql="SELECT 1",
            workgroup="wg",
            sleep=lambda _: None,
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ImportError: cannot import name 'AthenaQueryError'`

- [ ] **Step 3: Write the implementation**

In `pyproject.toml`, change the `boto3-stubs` dev dependency line to:

```toml
    "boto3-stubs[athena,batch,lambda,s3,sqs]>=1.37.36",
```

Append to `rollup.py`:

```python
import time
from collections.abc import Callable

_TERMINAL_FAILURES = ("FAILED", "CANCELLED")


class AthenaQueryError(RuntimeError):
    """Raised when an Athena query reaches a terminal failure state."""


def run_query(
    *,
    athena_client: Any,
    sql: str,
    workgroup: str,
    poll_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Start an Athena query and block until it reaches a terminal state.

    Parameters
    ----------
    athena_client : Any
        Boto3 Athena client.
    sql : str
        Statement to execute.
    workgroup : str
        Athena workgroup, which supplies the result location.
    poll_seconds : float, optional
        Delay between status polls. Defaults to 1.0.
    sleep : Callable[[float], None], optional
        Sleep function, injected so tests need not wait.

    Returns
    -------
    str
        The query execution id.

    Raises
    ------
    AthenaQueryError
        If the query ends in FAILED or CANCELLED.
    """
    query_id = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
    )["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return str(query_id)
        if state in _TERMINAL_FAILURES:
            reason = status.get("StateChangeReason", state)
            raise AthenaQueryError(f"Athena query {query_id} {state}: {reason}")
        sleep(poll_seconds)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv sync && ./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 17 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add pyproject.toml uv.lock src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/test_rollup.py
git commit -m "feat: add blocking Athena query runner"
```

---

## Task 7: Rollup orchestration, metrics, and chaining

**Files:**

- Modify: `src/batch_event_job_monitor/rollup.py`
- Test: `tests/batch_event_job_monitor/test_rollup.py`

**Interfaces:**

- Consumes: everything from Tasks 2-6.
- Produces: `RollupConfig` frozen dataclass with fields `bucket`, `queue_url`, `staging_prefix`, `database`,
  `iceberg_table`, `staging_table`, `inventory_table`, `workgroup`, `partition_key_names`, `max_keys`, `max_depth`,
  `function_name`, `metric_namespace`;
  `run_rollup(*, config: RollupConfig, clients: Clients, depth: int) -> dict[str, int]`; `Clients` dataclass holding
  `s3`, `sqs`, `athena`, `lambda_`; `emit_metrics(*, namespace: str, metrics: dict[str, int]) -> None`.

`run_rollup` must delete SQS messages **only after** the merge succeeds, and must self-invoke only when the queue still
has messages and `depth + 1 < max_depth`.

- [ ] **Step 1: Write the failing test**

```python
from batch_event_job_monitor.rollup import Clients, RollupConfig, run_rollup


class FakeLambda:
    def __init__(self) -> None:
        self.invocations: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, int]:
        self.invocations.append(kwargs)
        return {"StatusCode": 202}


def _config(bucket: str, queue_url: str, **overrides: Any) -> RollupConfig:
    defaults: dict[str, Any] = {
        "bucket": bucket,
        "queue_url": queue_url,
        "staging_prefix": "staging/",
        "database": "test_db",
        "iceberg_table": "records_iceberg",
        "staging_table": "records_staging",
        "inventory_table": "records_inventory",
        "workgroup": "wg",
        "partition_key_names": PARTITION_KEY_NAMES,
        "max_keys": 100,
        "max_depth": 3,
        "function_name": "rollup-fn",
        "metric_namespace": "BatchEventJobMonitor",
    }
    defaults.update(overrides)
    return RollupConfig(**defaults)


def test_rollup_merges_and_deletes_messages_on_success(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)
    athena = FakeAthena(["SUCCEEDED"])
    lambda_ = FakeLambda()

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=lambda_),
        depth=0,
    )

    assert metrics["RolledUpRecords"] == 1
    assert "MERGE INTO" in athena.started[0]
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    assert lambda_.invocations == []


def test_rollup_retains_messages_when_the_merge_fails(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)

    with pytest.raises(AthenaQueryError):
        run_rollup(
            config=_config(bucket, rollup_queue_url),
            clients=Clients(
                s3=s3, sqs=sqs, athena=FakeAthena(["FAILED"]), lambda_=FakeLambda()
            ),
            depth=0,
        )

    attributes = sqs.get_queue_attributes(
        QueueUrl=rollup_queue_url,
        AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 1


def test_rollup_skips_the_merge_when_nothing_was_drained(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthena([])

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=0,
    )

    assert metrics["RolledUpRecords"] == 0
    assert athena.started == []


def test_rollup_chains_while_the_queue_is_not_empty(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    for index in range(3):
        key = f"records/job_type=monthly-composite/tile_id=12TVK/year_month=2024-06/input_entity_id=e{index}/001.json"
        body = _body()
        body["input_entity_id"] = f"e{index}"
        _put_record(s3, bucket, key, body)
        _send(sqs, rollup_queue_url, key)

    lambda_ = FakeLambda()
    run_rollup(
        config=_config(bucket, rollup_queue_url, max_keys=1),
        clients=Clients(
            s3=s3, sqs=sqs, athena=FakeAthena(["SUCCEEDED"]), lambda_=lambda_
        ),
        depth=0,
    )

    assert len(lambda_.invocations) == 1
    payload = json.loads(lambda_.invocations[0]["Payload"])
    assert payload == {"mode": "rollup", "depth": 1}
    assert lambda_.invocations[0]["InvocationType"] == "Event"


def test_rollup_does_not_chain_past_max_depth(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    for index in range(3):
        _send(sqs, rollup_queue_url, f"records/job_type=a/{index:03d}.json")

    lambda_ = FakeLambda()
    run_rollup(
        config=_config(bucket, rollup_queue_url, max_keys=1, max_depth=1),
        clients=Clients(
            s3=s3, sqs=sqs, athena=FakeAthena(["SUCCEEDED"]), lambda_=lambda_
        ),
        depth=0,
    )

    assert lambda_.invocations == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ImportError: cannot import name 'Clients'`

- [ ] **Step 3: Write the implementation**

Append to `rollup.py`:

```python
import uuid
from datetime import datetime, timezone

from batch_event_job_monitor.rollup_schema import merge_sql

_DELETE_BATCH = 10


@dataclass(frozen=True)
class RollupConfig:
    """Everything the rollup and reconcile modes read from the environment.

    Attributes
    ----------
    bucket : str
        Processing bucket holding records/, staging/, and the Iceberg data.
    queue_url : str
        Rollup queue URL.
    staging_prefix : str
        Key prefix for staging objects, with a trailing slash.
    database : str
        Glue database holding every table below.
    iceberg_table : str
        Rolled-up Iceberg table name.
    staging_table : str
        NDJSON staging table name.
    inventory_table : str
        S3-inventory table over records/.
    workgroup : str
        Athena workgroup supplying the query result location.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.
    max_keys : int
        Per-run cap on distinct keys.
    max_depth : int
        Maximum self-invocation chain length.
    function_name : str
        This function's own name, used for self-invocation.
    metric_namespace : str
        CloudWatch namespace for the EMF records.
    """

    bucket: str
    queue_url: str
    staging_prefix: str
    database: str
    iceberg_table: str
    staging_table: str
    inventory_table: str
    workgroup: str
    partition_key_names: list[str]
    max_keys: int
    max_depth: int
    function_name: str
    metric_namespace: str


@dataclass
class Clients:
    """Boto3 clients the rollup needs, grouped so tests can substitute fakes.

    Attributes
    ----------
    s3 : Any
        S3 client.
    sqs : Any
        SQS client.
    athena : Any
        Athena client.
    lambda_ : Any
        Lambda client, used only for self-invocation.
    """

    s3: Any
    sqs: Any
    athena: Any
    lambda_: Any


def emit_metrics(*, namespace: str, metrics: dict[str, int]) -> None:
    """Print one Embedded Metric Format record covering every metric.

    Parameters
    ----------
    namespace : str
        CloudWatch namespace.
    metrics : dict[str, int]
        Metric name to value.
    """
    record: dict[str, Any] = {
        "_aws": {
            "Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [[]],
                    "Metrics": [{"Name": name} for name in metrics],
                }
            ],
        },
        **metrics,
    }
    print(json.dumps(record))


def _delete_messages(*, sqs_client: Any, queue_url: str, handles: list[str]) -> None:
    for start in range(0, len(handles), _DELETE_BATCH):
        batch = handles[start : start + _DELETE_BATCH]
        sqs_client.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(index), "ReceiptHandle": handle}
                for index, handle in enumerate(batch)
            ],
        )


def _chain(*, config: RollupConfig, clients: Clients, mode: str, depth: int, **extra: Any) -> bool:
    if depth >= config.max_depth:
        logger.warning("Chain depth %d reached the maximum; stopping", depth)
        return False
    clients.lambda_.invoke(
        FunctionName=config.function_name,
        InvocationType="Event",
        Payload=json.dumps({"mode": mode, "depth": depth, **extra}),
    )
    return True


def run_rollup(*, config: RollupConfig, clients: Clients, depth: int) -> dict[str, int]:
    """Drain the queue, merge one batch, and chain if work remains.

    Messages are deleted only after the merge succeeds, so an invocation that
    dies part-way leaves them to reappear. Every operation is idempotent on
    the natural key, so replay is safe.

    Parameters
    ----------
    config : RollupConfig
        Resolved configuration.
    clients : Clients
        Boto3 clients.
    depth : int
        This invocation's position in the self-invocation chain.

    Returns
    -------
    dict[str, int]
        Metrics emitted for this run.
    """
    drained = drain_keys(
        sqs_client=clients.sqs,
        queue_url=config.queue_url,
        max_keys=config.max_keys,
    )

    metrics = {
        "RolledUpRecords": 0,
        "MissingSourceObjects": 0,
        "MalformedRecords": 0,
        "ChainDepth": depth,
    }

    if not drained.keys:
        emit_metrics(namespace=config.metric_namespace, metrics=metrics)
        return metrics

    fetched = fetch_rows(
        s3_client=clients.s3,
        bucket=config.bucket,
        keys=drained.keys,
        partition_key_names=config.partition_key_names,
    )
    metrics["MissingSourceObjects"] = fetched.missing
    metrics["MalformedRecords"] = fetched.malformed

    if fetched.rows:
        run_id = str(uuid.uuid4())
        staging_key = write_staging_object(
            s3_client=clients.s3,
            bucket=config.bucket,
            staging_prefix=config.staging_prefix,
            run_id=run_id,
            rows=fetched.rows,
        )
        run_query(
            athena_client=clients.athena,
            sql=merge_sql(
                database=config.database,
                iceberg_table=config.iceberg_table,
                staging_table=config.staging_table,
                run_id=run_id,
                partition_key_names=config.partition_key_names,
            ),
            workgroup=config.workgroup,
        )
        clients.s3.delete_object(Bucket=config.bucket, Key=staging_key)
        metrics["RolledUpRecords"] = len(fetched.rows)

    _delete_messages(
        sqs_client=clients.sqs,
        queue_url=config.queue_url,
        handles=drained.receipt_handles,
    )

    if queue_has_messages(sqs_client=clients.sqs, queue_url=config.queue_url):
        _chain(config=config, clients=clients, mode="rollup", depth=depth + 1)

    emit_metrics(namespace=config.metric_namespace, metrics=metrics)
    return metrics
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 22 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/test_rollup.py
git commit -m "feat: orchestrate the rollup batch with metrics and self-chaining"
```

---

## Task 8: Reconcile mode with result paging

**Files:**

- Modify: `src/batch_event_job_monitor/rollup.py`
- Test: `tests/batch_event_job_monitor/test_rollup.py`

**Interfaces:**

- Consumes: `RollupConfig`, `Clients`, `_chain`, `run_query`, `emit_metrics` (Task 7); `reconcile_sql` (Task 2).
- Produces:
  `run_reconcile(*, config: RollupConfig, clients: Clients, depth: int, query_execution_id: str | None = None, next_token: str | None = None, time_remaining_ms: Callable[[], int] = lambda: 900_000) -> dict[str, int]`.

Reconcile starts a new query when `query_execution_id` is `None`, otherwise continues paging the existing result. It
stops when fewer than `RECONCILE_TIME_BUDGET_MS` remain and chains with the current token.

- [ ] **Step 1: Write the failing test**

```python
from batch_event_job_monitor.rollup import run_reconcile


class FakeAthenaResults(FakeAthena):
    """Athena fake that also serves paged query results."""

    def __init__(self, states: list[str], pages: list[dict[str, Any]]) -> None:
        super().__init__(states)
        self.pages = pages
        self.requested_tokens: list[str | None] = []

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.requested_tokens.append(kwargs.get("NextToken"))
        return self.pages.pop(0)


def _page(keys: list[str], next_token: str | None, header: bool) -> dict[str, Any]:
    rows = [{"Data": [{"VarCharValue": "source_key"}]}] if header else []
    rows += [{"Data": [{"VarCharValue": key}]} for key in keys]
    page: dict[str, Any] = {"ResultSet": {"Rows": rows}}
    if next_token is not None:
        page["NextToken"] = next_token
    return page


def test_reconcile_enqueues_keys_and_skips_the_header_row(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults(
        ["SUCCEEDED"], [_page(["records/a.json", "records/b.json"], None, True)]
    )

    metrics = run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=0,
    )

    assert metrics["ReconcileDrift"] == 2
    assert "LEFT JOIN" in athena.started[0]
    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=10)
    assert sorted(drained.keys) == ["records/a.json", "records/b.json"]


def test_reconcile_chains_when_the_time_budget_runs_out(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults(
        ["SUCCEEDED"], [_page(["records/a.json"], "token-2", True)]
    )
    lambda_ = FakeLambda()

    run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=lambda_),
        depth=0,
        time_remaining_ms=lambda: 1_000,
    )

    payload = json.loads(lambda_.invocations[0]["Payload"])
    assert payload == {
        "mode": "reconcile",
        "depth": 1,
        "query_execution_id": "qid-1",
        "next_token": "token-2",
    }


def test_reconcile_continues_an_existing_result_without_a_new_query(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults([], [_page(["records/c.json"], None, False)])

    run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=1,
        query_execution_id="qid-1",
        next_token="token-2",
    )

    assert athena.started == []
    assert athena.requested_tokens == ["token-2"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: FAIL with
`ImportError: cannot import name 'run_reconcile'`

- [ ] **Step 3: Write the implementation**

Append to `rollup.py`:

```python
from batch_event_job_monitor.rollup_schema import reconcile_sql

# Leave enough of the invocation budget to chain before the timeout lands.
RECONCILE_TIME_BUDGET_MS = 120_000

_SEND_BATCH = 10


def _enqueue_keys(*, sqs_client: Any, queue_url: str, keys: list[str]) -> None:
    for start in range(0, len(keys), _SEND_BATCH):
        batch = keys[start : start + _SEND_BATCH]
        sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": str(index),
                    "MessageBody": json.dumps({"detail": {"object": {"key": key}}}),
                }
                for index, key in enumerate(batch)
            ],
        )


def run_reconcile(
    *,
    config: RollupConfig,
    clients: Clients,
    depth: int,
    query_execution_id: str | None = None,
    next_token: str | None = None,
    time_remaining_ms: Callable[[], int] = lambda: 900_000,
) -> dict[str, int]:
    """Enqueue every record key missing from or stale in the Iceberg table.

    Convergent: each enqueued key gets merged, so the next pass matches
    strictly fewer. Run against a cold table it is also the backfill.

    Parameters
    ----------
    config : RollupConfig
        Resolved configuration.
    clients : Clients
        Boto3 clients.
    depth : int
        This invocation's position in the self-invocation chain.
    query_execution_id : str or None, optional
        Existing Athena result to continue paging. A new query is started
        when this is None.
    next_token : str or None, optional
        Result page token to resume from.
    time_remaining_ms : Callable[[], int], optional
        Milliseconds left in this invocation, from the Lambda context.

    Returns
    -------
    dict[str, int]
        Metrics emitted for this run.
    """
    if query_execution_id is None:
        query_execution_id = run_query(
            athena_client=clients.athena,
            sql=reconcile_sql(
                database=config.database,
                iceberg_table=config.iceberg_table,
                inventory_table=config.inventory_table,
            ),
            workgroup=config.workgroup,
        )
        skip_header = True
    else:
        skip_header = False

    enqueued = 0
    while True:
        kwargs: dict[str, Any] = {"QueryExecutionId": query_execution_id}
        if next_token is not None:
            kwargs["NextToken"] = next_token
        page = clients.athena.get_query_results(**kwargs)

        rows = page["ResultSet"]["Rows"]
        if skip_header:
            rows = rows[1:]
            skip_header = False

        keys = [row["Data"][0]["VarCharValue"] for row in rows]
        if keys:
            _enqueue_keys(
                sqs_client=clients.sqs, queue_url=config.queue_url, keys=keys
            )
            enqueued += len(keys)

        next_token = page.get("NextToken")
        if next_token is None:
            break
        if time_remaining_ms() < RECONCILE_TIME_BUDGET_MS:
            _chain(
                config=config,
                clients=clients,
                mode="reconcile",
                depth=depth + 1,
                query_execution_id=query_execution_id,
                next_token=next_token,
            )
            break

    metrics = {"ReconcileDrift": enqueued, "ChainDepth": depth}
    emit_metrics(namespace=config.metric_namespace, metrics=metrics)
    return metrics
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup.py -v` Expected: 25 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/rollup.py tests/batch_event_job_monitor/test_rollup.py
git commit -m "feat: add reconcile mode with paged results and self-chaining"
```

---

## Task 9: Lambda handler

**Files:**

- Create: `src/batch_event_job_monitor/handlers/rollup_handler.py`
- Test: `tests/batch_event_job_monitor/test_rollup_handler.py`

**Interfaces:**

- Consumes: `RollupConfig`, `Clients`, `run_rollup`, `run_reconcile` (Tasks 7-8).
- Produces: `handler(event: dict[str, Any], context: Any) -> dict[str, int]`;
  `config_from_environment() -> RollupConfig`.

Environment variables the CDK construct sets: `PROCESSING_BUCKET_NAME`, `ROLLUP_QUEUE_URL`, `ROLLUP_STAGING_PREFIX`,
`ROLLUP_DATABASE`, `ROLLUP_ICEBERG_TABLE`, `ROLLUP_STAGING_TABLE`, `ROLLUP_INVENTORY_TABLE`, `ROLLUP_WORKGROUP`,
`ROLLUP_PARTITION_KEY_NAMES` (comma-separated), `ROLLUP_MAX_KEYS`, `ROLLUP_MAX_DEPTH`, `AWS_LAMBDA_FUNCTION_NAME`,
`ROLLUP_METRIC_NAMESPACE` (optional).

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the bundled rollup Lambda handler."""

from __future__ import annotations

import os
from typing import Any

import pytest

from batch_event_job_monitor.handlers import rollup_handler

ENVIRONMENT = {
    "PROCESSING_BUCKET_NAME": "test-processing",
    "ROLLUP_QUEUE_URL": "https://sqs.us-west-2.amazonaws.com/123456789012/q",
    "ROLLUP_STAGING_PREFIX": "staging/",
    "ROLLUP_DATABASE": "test_db",
    "ROLLUP_ICEBERG_TABLE": "records_iceberg",
    "ROLLUP_STAGING_TABLE": "records_staging",
    "ROLLUP_INVENTORY_TABLE": "records_inventory",
    "ROLLUP_WORKGROUP": "wg",
    "ROLLUP_PARTITION_KEY_NAMES": "job_type,tile_id,year_month",
    "ROLLUP_MAX_KEYS": "25000",
    "ROLLUP_MAX_DEPTH": "1000",
    "AWS_LAMBDA_FUNCTION_NAME": "rollup-fn",
}


@pytest.fixture
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)


def test_config_reads_every_setting_from_the_environment(environment: None) -> None:
    config = rollup_handler.config_from_environment()
    assert config.bucket == "test-processing"
    assert config.partition_key_names == ["job_type", "tile_id", "year_month"]
    assert config.max_keys == 25000
    assert config.max_depth == 1000
    assert config.function_name == "rollup-fn"
    assert config.metric_namespace == "BatchEventJobMonitor"


def test_handler_dispatches_rollup_by_default(
    environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    monkeypatch.setattr(
        rollup_handler,
        "run_rollup",
        lambda **kwargs: calls.append(kwargs["depth"]) or {"RolledUpRecords": 0},
    )

    rollup_handler.handler({}, _context())

    assert calls == [0]


def test_handler_dispatches_reconcile_with_continuation(
    environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        rollup_handler,
        "run_reconcile",
        lambda **kwargs: captured.update(kwargs) or {"ReconcileDrift": 0},
    )

    rollup_handler.handler(
        {
            "mode": "reconcile",
            "depth": 2,
            "query_execution_id": "qid-1",
            "next_token": "token-2",
        },
        _context(),
    )

    assert captured["depth"] == 2
    assert captured["query_execution_id"] == "qid-1"
    assert captured["next_token"] == "token-2"


def test_handler_rejects_an_unknown_mode(environment: None) -> None:
    with pytest.raises(ValueError, match="unknown rollup mode"):
        rollup_handler.handler({"mode": "nonsense"}, _context())


class _Context:
    def get_remaining_time_in_millis(self) -> int:
        return 900_000


def _context() -> Any:
    return _Context()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_handler.py -v` Expected: FAIL with
`ModuleNotFoundError: No module named 'batch_event_job_monitor.handlers.rollup_handler'`

- [ ] **Step 3: Write the implementation**

```python
"""Bundled Lambda handler for RecordsRollupFunction.

One handler serves both modes. The rollup mode is the scheduled default;
the reconcile mode is invoked on a separate schedule and by both modes'
self-invocation chains, which carry their continuation state in the event.
"""

from __future__ import annotations

import os
from typing import Any

import boto3

from batch_event_job_monitor.rollup import (
    Clients,
    RollupConfig,
    run_reconcile,
    run_rollup,
)
from batch_event_job_monitor.untracked import DEFAULT_METRIC_NAMESPACE

_clients = Clients(
    s3=boto3.client("s3"),
    sqs=boto3.client("sqs"),
    athena=boto3.client("athena"),
    lambda_=boto3.client("lambda"),
)


def config_from_environment() -> RollupConfig:
    """Build the rollup configuration from the construct's environment.

    Returns
    -------
    RollupConfig
        Resolved configuration.
    """
    return RollupConfig(
        bucket=os.environ["PROCESSING_BUCKET_NAME"],
        queue_url=os.environ["ROLLUP_QUEUE_URL"],
        staging_prefix=os.environ["ROLLUP_STAGING_PREFIX"],
        database=os.environ["ROLLUP_DATABASE"],
        iceberg_table=os.environ["ROLLUP_ICEBERG_TABLE"],
        staging_table=os.environ["ROLLUP_STAGING_TABLE"],
        inventory_table=os.environ["ROLLUP_INVENTORY_TABLE"],
        workgroup=os.environ["ROLLUP_WORKGROUP"],
        partition_key_names=os.environ["ROLLUP_PARTITION_KEY_NAMES"].split(","),
        max_keys=int(os.environ["ROLLUP_MAX_KEYS"]),
        max_depth=int(os.environ["ROLLUP_MAX_DEPTH"]),
        function_name=os.environ["AWS_LAMBDA_FUNCTION_NAME"],
        metric_namespace=os.environ.get(
            "ROLLUP_METRIC_NAMESPACE", DEFAULT_METRIC_NAMESPACE
        ),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Dispatch one rollup or reconcile invocation.

    Parameters
    ----------
    event : dict[str, Any]
        Scheduler event, or a self-invocation payload carrying mode, depth,
        and any reconcile continuation state.
    context : Any
        Lambda context, read for the remaining time budget.

    Returns
    -------
    dict[str, int]
        Metrics emitted for this run.

    Raises
    ------
    ValueError
        If the event names a mode other than rollup or reconcile.
    """
    config = config_from_environment()
    mode = event.get("mode", "rollup")
    depth = int(event.get("depth", 0))

    if mode == "rollup":
        return run_rollup(config=config, clients=_clients, depth=depth)
    if mode == "reconcile":
        return run_reconcile(
            config=config,
            clients=_clients,
            depth=depth,
            query_execution_id=event.get("query_execution_id"),
            next_token=event.get("next_token"),
            time_remaining_ms=context.get_remaining_time_in_millis,
        )
    raise ValueError(f"unknown rollup mode: {mode!r}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_rollup_handler.py -v` Expected: 4 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/handlers/rollup_handler.py tests/batch_event_job_monitor/test_rollup_handler.py
git commit -m "feat: add bundled rollup Lambda handler"
```

---

## Task 10: Iceberg DDL custom resource handler

**Files:**

- Create: `src/batch_event_job_monitor/handlers/iceberg_ddl_handler.py`
- Test: `tests/batch_event_job_monitor/test_iceberg_ddl_handler.py`

**Interfaces:**

- Consumes: `create_table_sql`, `iceberg_columns` (Task 1); `run_query` (Task 6).
- Produces: `handler(event: dict[str, Any], context: Any) -> dict[str, str]`;
  `added_column_sql(*, database: str, table: str, old_columns: list[str], new_columns: list[tuple[str, str]]) -> list[str]`;
  `IncompatibleSchemaChange(RuntimeError)`.

Resource properties: `Database`, `Table`, `Location`, `PartitionKeyNames` (comma-separated), `Workgroup`,
`RemovalPolicy` (`"destroy"` or `"retain"`).

- [ ] **Step 1: Write the failing test**

```python
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


def test_added_column_sql_emits_one_alter_per_new_column() -> None:
    statements = added_column_sql(
        database="test_db",
        table="records_iceberg",
        old_columns=["job_type", "attempt"],
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
            old_columns=["job_type", "attempt"],
            new_columns=[("job_type", "string")],
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor/test_iceberg_ddl_handler.py -v` Expected: FAIL with
`ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
"""CloudFormation custom resource handler creating the Iceberg records table.

Athena DDL rather than a Glue CfnTable: a Glue catalog entry for an Iceberg
table is only valid once the table's metadata file exists, which CREATE TABLE
writes and CloudFormation cannot.
"""

from __future__ import annotations

import os
from typing import Any

import boto3

from batch_event_job_monitor.rollup import run_query
from batch_event_job_monitor.rollup_schema import create_table_sql, iceberg_columns

_athena_client = boto3.client("athena")


class IncompatibleSchemaChange(RuntimeError):
    """Raised when an update would remove or retype an existing column."""


def added_column_sql(
    *,
    database: str,
    table: str,
    old_columns: list[str],
    new_columns: list[tuple[str, str]],
) -> list[str]:
    """Build ALTER statements adding columns introduced since the last deploy.

    Parameters
    ----------
    database : str
        Glue database name.
    table : str
        Iceberg table name.
    old_columns : list[str]
        Column names the deployed table already has.
    new_columns : list[tuple[str, str]]
        (name, type) pairs the new schema declares.

    Returns
    -------
    list[str]
        One ALTER TABLE ADD COLUMNS statement per added column.

    Raises
    ------
    IncompatibleSchemaChange
        If any deployed column is absent from the new schema.
    """
    new_names = {name for name, _ in new_columns}
    removed = [name for name in old_columns if name not in new_names]
    if removed:
        raise IncompatibleSchemaChange(
            f"columns cannot be dropped automatically: {', '.join(removed)}"
        )
    return [
        f'ALTER TABLE "{database}"."{table}" ADD COLUMNS ("{name}" {col_type})'
        for name, col_type in new_columns
        if name not in set(old_columns)
    ]


def handler(event: dict[str, Any], context: Any) -> dict[str, str]:
    """Create, update, or drop the Iceberg records table.

    Parameters
    ----------
    event : dict[str, Any]
        CloudFormation custom resource event.
    context : Any
        Lambda context. Unused.

    Returns
    -------
    dict[str, str]
        The physical resource id, stable across updates.
    """
    properties = event["ResourceProperties"]
    database = properties["Database"]
    table = properties["Table"]
    workgroup = properties["Workgroup"]
    partition_key_names = properties["PartitionKeyNames"].split(",")
    physical_id = f"{database}.{table}"

    request_type = event["RequestType"]

    if request_type == "Delete":
        if properties.get("RemovalPolicy") == "destroy":
            run_query(
                athena_client=_athena_client,
                sql=f'DROP TABLE IF EXISTS "{database}"."{table}"',
                workgroup=workgroup,
            )
        return {"PhysicalResourceId": physical_id}

    if request_type == "Update":
        old_properties = event.get("OldResourceProperties", properties)
        old_names = [
            name
            for name, _ in iceberg_columns(
                old_properties["PartitionKeyNames"].split(",")
            )
        ]
        for statement in added_column_sql(
            database=database,
            table=table,
            old_columns=old_names,
            new_columns=iceberg_columns(partition_key_names),
        ):
            run_query(
                athena_client=_athena_client, sql=statement, workgroup=workgroup
            )
        return {"PhysicalResourceId": physical_id}

    run_query(
        athena_client=_athena_client,
        sql=create_table_sql(
            database=database,
            table=table,
            location=properties["Location"],
            partition_key_names=partition_key_names,
        ),
        workgroup=workgroup,
    )
    return {"PhysicalResourceId": physical_id}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor/test_iceberg_ddl_handler.py -v` Expected: 5 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor/handlers/iceberg_ddl_handler.py tests/batch_event_job_monitor/test_iceberg_ddl_handler.py
git commit -m "feat: add Iceberg table DDL custom resource handler"
```

---

## Task 11: IcebergRecordsTable construct

**Files:**

- Create: `src/batch_event_job_monitor_cdk/iceberg_records_table.py`
- Test: `tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py`

**Interfaces:**

- Consumes: `staging_columns` (Task 1); `iceberg_ddl_handler` (Task 10); existing
  `athena_common.create_inventory_table`, `JSON_INPUT_FORMAT`, `JSON_SERDE`, `HIVE_TEXT_OUTPUT_FORMAT`;
  `partition_key_spec.PartitionKeySpec`.
- Produces: `IcebergRecordsTable` construct exposing `iceberg_table_name: str`, `staging_table_name: str`,
  `inventory_table_name: str`, `table_location: str`.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py -v` Expected: FAIL with
`ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
"""CDK construct for the rolled-up Iceberg records table.

Creates the Iceberg table itself through an Athena DDL custom resource, the
NDJSON staging table the rollup merges from, the S3-inventory table reconcile
anti-joins against, and the Glue table optimizer that compacts the Iceberg
table.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from aws_cdk import (
    Aws,
    CustomResource,
    Duration,
    RemovalPolicy,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    custom_resources as cr,
)
from constructs import Construct

from batch_event_job_monitor.rollup_schema import staging_columns

from .athena_common import (
    HIVE_TEXT_OUTPUT_FORMAT,
    JSON_INPUT_FORMAT,
    JSON_SERDE,
    create_inventory_table,
)
from .partition_key_spec import PartitionKeySpec

ICEBERG_PREFIX = "iceberg/records/"
STAGING_PREFIX = "staging/"


class IcebergRecordsTable(Construct):
    """Iceberg records table, its staging table, and its inventory table.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    database : glue.CfnDatabase
        Glue database every table is created in.
    database_name : str
        Literal name of ``database`` (not a CDK token).
    processing_bucket_name : str
        Name of the bucket holding records/, staging/, and the Iceberg data.
    records_inventory_location_s3path : str
        s3:// URI of the records prefix's S3 Inventory Hive symlink manifests.
    inventory_datetime_start : dt.datetime
        Anchor for the inventory table's dt partition projection. Its
        time-of-day must match the S3 delivery hour.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading job_type entry.
    workgroup_name : str
        Athena workgroup the DDL runs in.
    iceberg_table_name : str, optional
        Name of the Iceberg table. Defaults to "records_iceberg".
    staging_table_name : str, optional
        Name of the staging table. Defaults to "records_staging".
    inventory_table_name : str, optional
        Name of the inventory table. Defaults to "records_inventory".
    removal_policy : RemovalPolicy, optional
        Removal policy for the catalog entries. Defaults to
        RemovalPolicy.RETAIN.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    iceberg_table_name : str
        Name of the Iceberg table.
    staging_table_name : str
        Name of the staging table.
    inventory_table_name : str
        Name of the inventory table.
    table_location : str
        s3:// URI of the Iceberg table's data location.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        processing_bucket_name: str,
        records_inventory_location_s3path: str,
        inventory_datetime_start: dt.datetime,
        partition_keys: list[PartitionKeySpec],
        workgroup_name: str = "primary",
        iceberg_table_name: str = "records_iceberg",
        staging_table_name: str = "records_staging",
        inventory_table_name: str = "records_inventory",
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.iceberg_table_name = iceberg_table_name
        self.staging_table_name = staging_table_name
        self.inventory_table_name = inventory_table_name
        self.table_location = f"s3://{processing_bucket_name}/{ICEBERG_PREFIX}"

        partition_key_names = [key.name for key in partition_keys]

        self.inventory_table = create_inventory_table(
            self,
            "RecordsInventoryTable",
            database=database,
            table_name=inventory_table_name,
            location=records_inventory_location_s3path,
            datetime_start=inventory_datetime_start,
        )

        self.staging_table = self._create_staging_table(
            database=database,
            table_name=staging_table_name,
            location=f"s3://{processing_bucket_name}/{STAGING_PREFIX}",
            partition_key_names=partition_key_names,
        )

        self.ddl_resource = self._create_ddl_resource(
            database=database,
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
            partition_key_names=partition_key_names,
            workgroup_name=workgroup_name,
            removal_policy=removal_policy,
        )

        self._create_table_optimizer(
            database_name=database_name,
            processing_bucket_name=processing_bucket_name,
        )

    def _create_staging_table(
        self,
        *,
        database: glue.CfnDatabase,
        table_name: str,
        location: str,
        partition_key_names: list[str],
    ) -> glue.CfnTable:
        columns = [
            glue.CfnTable.ColumnProperty(name=name, type=col_type)
            for name, col_type in staging_columns(partition_key_names)
        ]
        table = glue.CfnTable(
            self,
            "StagingTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name=table_name,
                table_type="EXTERNAL_TABLE",
                parameters={
                    "EXTERNAL": "TRUE",
                    "projection.enabled": "true",
                    "projection.run_id.type": "injected",
                    "storage.location.template": f"{location}run_id=${{run_id}}/",
                },
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="run_id", type="string")
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=columns,
                    location=location,
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
        table.add_resource_dependency(database)
        return table

    def _create_ddl_resource(
        self,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        processing_bucket_name: str,
        partition_key_names: list[str],
        workgroup_name: str,
        removal_policy: RemovalPolicy,
    ) -> CustomResource:
        ddl_function = lambda_.Function(
            self,
            "DdlFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="batch_event_job_monitor.handlers.iceberg_ddl_handler.handler",
            code=lambda_.Code.from_asset("src"),
            timeout=Duration.minutes(10),
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                ],
                resources=["*"],
            )
        )
        ddl_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{processing_bucket_name}",
                    f"arn:aws:s3:::{processing_bucket_name}/*",
                ],
            )
        )

        provider = cr.Provider(self, "DdlProvider", on_event_handler=ddl_function)
        resource = CustomResource(
            self,
            "IcebergTable",
            service_token=provider.service_token,
            resource_type="Custom::IcebergRecordsTable",
            properties={
                "Database": database_name,
                "Table": self.iceberg_table_name,
                "Location": self.table_location,
                "PartitionKeyNames": ",".join(partition_key_names),
                "Workgroup": workgroup_name,
                "RemovalPolicy": (
                    "destroy" if removal_policy is RemovalPolicy.DESTROY else "retain"
                ),
            },
        )
        resource.node.add_dependency(database)
        return resource

    def _create_table_optimizer(
        self, *, database_name: str, processing_bucket_name: str
    ) -> None:
        optimizer_role = iam.Role(
            self,
            "TableOptimizerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        optimizer_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::{processing_bucket_name}",
                    f"arn:aws:s3:::{processing_bucket_name}/*",
                ],
            )
        )
        optimizer_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetTable", "glue:UpdateTable"],
                resources=["*"],
            )
        )

        optimizer = glue.CfnTableOptimizer(
            self,
            "CompactionOptimizer",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_name=self.iceberg_table_name,
            type="compaction",
            table_optimizer_configuration=(
                glue.CfnTableOptimizer.TableOptimizerConfigurationProperty(
                    enabled=True,
                    role_arn=optimizer_role.role_arn,
                )
            ),
        )
        optimizer.node.add_dependency(self.ddl_resource)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py -v` Expected: 5 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor_cdk/iceberg_records_table.py tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py
git commit -m "feat: add IcebergRecordsTable construct"
```

---

## Task 12: RecordsRollupFunction construct

**Files:**

- Create: `src/batch_event_job_monitor_cdk/records_rollup_function.py`
- Test: `tests/batch_event_job_monitor_cdk/test_records_rollup_function.py`

**Interfaces:**

- Consumes: `IcebergRecordsTable` (Task 11); `rollup_handler` (Task 9).
- Produces: `RecordsRollupFunction` construct exposing `queue: sqs.Queue`, `dlq: sqs.Queue`,
  `rollup_function: lambda_.Function`, `reconcile_function: lambda_.Function`.

**Both functions must set `recursive_loop=lambda_.RecursiveLoop.ALLOW`.** The default terminates a self-invoking chain
at 16 invocations, which would silently stall backfill. This is the highest-value assertion in the task.

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the RecordsRollupFunction CDK construct."""

from __future__ import annotations

from aws_cdk.assertions import Match, Template


def test_both_functions_allow_recursive_invocation() -> None:
    template = _template()
    # rollup + reconcile + the DDL handler + the custom-resource provider's
    # own framework function.
    template.resource_count_is("AWS::Lambda::Function", 4)
    functions = template.find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"RecursiveLoop": "Allow"}},
    )
    assert len(functions) == 2


def test_both_functions_are_serialized_by_reserved_concurrency() -> None:
    functions = _template().find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"ReservedConcurrentExecutions": 1}},
    )
    assert len(functions) == 2


def test_async_invocations_are_not_retried() -> None:
    _template().has_resource_properties(
        "AWS::Lambda::EventInvokeConfig",
        Match.object_like({"MaximumRetryAttempts": 0}),
    )


def test_queue_visibility_exceeds_the_rollup_timeout() -> None:
    _template().has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like({"VisibilityTimeout": 960}),
    )


def test_dead_letter_queue_is_wired_with_a_receive_count() -> None:
    _template().has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like(
            {"RedrivePolicy": Match.object_like({"maxReceiveCount": 5})}
        ),
    )


def test_eventbridge_rule_matches_only_the_records_prefix() -> None:
    _template().has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like(
            {
                "EventPattern": Match.object_like(
                    {
                        "source": ["aws.s3"],
                        "detail": Match.object_like(
                            {
                                "object": {
                                    "key": [{"prefix": "records/"}]
                                }
                            }
                        ),
                    }
                )
            }
        ),
    )


def test_rollup_is_scheduled_hourly_and_reconcile_weekly() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(1 hour)"}),
    )
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(7 days)"}),
    )
```

Add the shared `_template()` helper at the top of the file:

```python
import datetime as dt

from aws_cdk import App, Stack, aws_glue as glue, aws_s3 as s3

from batch_event_job_monitor_cdk.iceberg_records_table import IcebergRecordsTable
from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec
from batch_event_job_monitor_cdk.records_rollup_function import RecordsRollupFunction

PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "injected"),
]


def _template() -> Template:
    app = App()
    stack = Stack(app, "TestStack")
    bucket = s3.Bucket(stack, "ProcessingBucket", bucket_name="test-bucket")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    table = IcebergRecordsTable(
        stack,
        "IcebergRecords",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path="s3://test-bucket/inv/test-bucket/records/hive/",
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
    )
    RecordsRollupFunction(
        stack,
        "Rollup",
        processing_bucket=bucket,
        database_name="test_db",
        iceberg_table=table,
        partition_keys=PARTITION_KEYS,
    )
    return Template.from_stack(stack)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_records_rollup_function.py -v` Expected: FAIL with
`ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
"""CDK construct for the records rollup and reconcile Lambdas.

Two functions share one handler asset. Splitting them keeps a long reconcile
chain from starving the hourly rollup, since each carries its own reserved
concurrency of 1.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import (
    Aws,
    Duration,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct

from .iceberg_records_table import STAGING_PREFIX, IcebergRecordsTable
from .partition_key_spec import PartitionKeySpec

RECORDS_PREFIX = "records/"
_ROLLUP_TIMEOUT = Duration.minutes(15)


class RecordsRollupFunction(Construct):
    """Change-capture queue and the two Lambdas that maintain the table.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    processing_bucket : s3.IBucket
        Bucket holding records/, staging/, and the Iceberg data.
    database_name : str
        Glue database holding the rollup tables.
    iceberg_table : IcebergRecordsTable
        The table this rollup maintains.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading job_type entry.
    workgroup_name : str, optional
        Athena workgroup. Defaults to "primary".
    rollup_schedule : events.Schedule, optional
        Rollup cadence. Defaults to hourly.
    reconcile_schedule : events.Schedule, optional
        Reconcile cadence. Defaults to weekly.
    max_keys_per_run : int, optional
        Per-run cap on distinct keys. Defaults to 25000.
    max_chain_depth : int, optional
        Maximum self-invocation chain length. Defaults to 1000.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    queue : sqs.Queue
        Rollup queue fed by S3 EventBridge notifications.
    dlq : sqs.Queue
        Dead-letter queue for poison notifications.
    rollup_function : lambda_.Function
        The scheduled rollup Lambda.
    reconcile_function : lambda_.Function
        The scheduled reconcile Lambda.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.IBucket,
        database_name: str,
        iceberg_table: IcebergRecordsTable,
        partition_keys: list[PartitionKeySpec],
        workgroup_name: str = "primary",
        rollup_schedule: events.Schedule | None = None,
        reconcile_schedule: events.Schedule | None = None,
        max_keys_per_run: int = 25000,
        max_chain_depth: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dlq = sqs.Queue(self, "RollupDlq", retention_period=Duration.days(14))
        self.queue = sqs.Queue(
            self,
            "RollupQueue",
            visibility_timeout=_ROLLUP_TIMEOUT.plus(Duration.minutes(1)),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=self.dlq),
        )

        events.Rule(
            self,
            "RecordWrittenRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [processing_bucket.bucket_name]},
                    "object": {"key": [{"prefix": RECORDS_PREFIX}]},
                },
            ),
            targets=[targets.SqsQueue(self.queue)],
        )

        environment = {
            "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            "ROLLUP_QUEUE_URL": self.queue.queue_url,
            "ROLLUP_STAGING_PREFIX": STAGING_PREFIX,
            "ROLLUP_DATABASE": database_name,
            "ROLLUP_ICEBERG_TABLE": iceberg_table.iceberg_table_name,
            "ROLLUP_STAGING_TABLE": iceberg_table.staging_table_name,
            "ROLLUP_INVENTORY_TABLE": iceberg_table.inventory_table_name,
            "ROLLUP_WORKGROUP": workgroup_name,
            "ROLLUP_PARTITION_KEY_NAMES": ",".join(key.name for key in partition_keys),
            "ROLLUP_MAX_KEYS": str(max_keys_per_run),
            "ROLLUP_MAX_DEPTH": str(max_chain_depth),
        }

        self.rollup_function = self._create_function(
            "RollupFunction", environment=environment
        )
        self.reconcile_function = self._create_function(
            "ReconcileFunction", environment=environment
        )

        for function in (self.rollup_function, self.reconcile_function):
            self._grant(
                function,
                processing_bucket=processing_bucket,
                database_name=database_name,
            )

        self.queue.grant_consume_messages(self.rollup_function)
        self.queue.grant_send_messages(self.reconcile_function)

        events.Rule(
            self,
            "RollupSchedule",
            schedule=rollup_schedule or events.Schedule.rate(Duration.hours(1)),
            targets=[
                targets.LambdaFunction(
                    self.rollup_function,
                    event=events.RuleTargetInput.from_object(
                        {"mode": "rollup", "depth": 0}
                    ),
                )
            ],
        )
        events.Rule(
            self,
            "ReconcileSchedule",
            schedule=reconcile_schedule or events.Schedule.rate(Duration.days(7)),
            targets=[
                targets.LambdaFunction(
                    self.reconcile_function,
                    event=events.RuleTargetInput.from_object(
                        {"mode": "reconcile", "depth": 0}
                    ),
                )
            ],
        )

    def _create_function(
        self, construct_id: str, *, environment: dict[str, str]
    ) -> lambda_.Function:
        function = lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="batch_event_job_monitor.handlers.rollup_handler.handler",
            code=lambda_.Code.from_asset("src"),
            timeout=_ROLLUP_TIMEOUT,
            memory_size=1024,
            environment=environment,
            reserved_concurrent_executions=1,
            # The default, TERMINATE, cuts a self-invoking chain off at 16
            # invocations, which silently stalls a backfill drain.
            recursive_loop=lambda_.RecursiveLoop.ALLOW,
            retry_attempts=0,
        )
        function.grant_invoke(function)
        return function

    def _grant(
        self,
        function: lambda_.Function,
        *,
        processing_bucket: s3.IBucket,
        database_name: str,
    ) -> None:
        processing_bucket.grant_read_write(function)
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{database_name}",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{database_name}/*",
                ],
            )
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_records_rollup_function.py -v` Expected: 7 passed

- [ ] **Step 5: Lint, typecheck, commit**

```bash
./scripts/lint && ./scripts/typecheck
git add src/batch_event_job_monitor_cdk/records_rollup_function.py tests/batch_event_job_monitor_cdk/test_records_rollup_function.py
git commit -m "feat: add RecordsRollupFunction construct"
```

---

## Task 13: Export the constructs and document them

**Files:**

- Modify: `src/batch_event_job_monitor_cdk/__init__.py`
- Modify: `docs/constructs.md`
- Modify: `README.md`
- Test: `tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py`

- [ ] **Step 1: Write the failing test**

```python
def test_constructs_are_exported_from_the_package() -> None:
    import batch_event_job_monitor_cdk as package

    assert package.IcebergRecordsTable is IcebergRecordsTable
    assert hasattr(package, "RecordsRollupFunction")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py -v` Expected: FAIL with
`AttributeError: module 'batch_event_job_monitor_cdk' has no attribute 'IcebergRecordsTable'`

- [ ] **Step 3: Update the exports and docs**

In `src/batch_event_job_monitor_cdk/__init__.py`, add the two imports alongside the existing construct exports and add
both names to `__all__`:

```python
from .iceberg_records_table import IcebergRecordsTable
from .records_rollup_function import RecordsRollupFunction
```

Add a `## IcebergRecordsTable` and a `## RecordsRollupFunction` section to `docs/constructs.md`, matching the structure
of the existing sections (what it creates, what it needs from you, how it wires together). Cover:

- `IcebergRecordsTable` creates the Iceberg table via an Athena DDL custom resource, the NDJSON staging table, the
  `records/` inventory table, and the compaction optimizer.
- The caller must add `("records", "records/")` to the `ProcessingBucket` `inventories` list, and
  `inventory_datetime_start` must match the S3 delivery hour.
- `RecordsRollupFunction` creates the queue, its DLQ, the EventBridge rule on the `records/` prefix, and both Lambdas,
  scheduled hourly and weekly.
- Freshness bound for consumers is `SELECT max(rolled_up_at) FROM <database>.<iceberg_table>`.
- Backfill: invoke the reconcile function until the `ReconcileDrift` metric reaches 0.
- The table has no declared sort order because Athena cannot declare one, so Glue optimization binpacks only.
- Alarms are the consumer's to create, matching how `JobMonitorFunction` documents the `UntrackedJobs` metric. Name the
  ones worth alarming on: `ReconcileDrift` sustained non-zero, `ChainDepth` approaching `max_chain_depth`, DLQ depth,
  and queue message age.

In `README.md`, add a short section after the existing Athena table paragraph pointing at the two new constructs and
stating that `records` is queried through the Iceberg table.

- [ ] **Step 4: Run tests and lint to verify they pass**

Run: `./scripts/test && ./scripts/lint` Expected: all tests pass; prettier reports no formatting issues

If prettier complains, run `npx --yes prettier@3 --write "docs/**/*.md" README.md` and re-run.

- [ ] **Step 5: Commit**

```bash
./scripts/typecheck
git add src/batch_event_job_monitor_cdk/__init__.py docs/constructs.md README.md tests/batch_event_job_monitor_cdk/test_iceberg_records_table.py
git commit -m "docs: document the Iceberg rollup constructs"
```

---

## Task 14: Retire AthenaRecordsTable

> **Gate:** Do not start this task until the Iceberg table has been verified complete in a deployed environment - the
> reconcile function run to `ReconcileDrift == 0`, and row counts per `job_type` compared against object counts per
> `job_type` in the inventory table. This is the only task in the plan whose precondition lives outside the repo.

**Files:**

- Delete: `src/batch_event_job_monitor_cdk/athena_records_table.py`
- Modify: `src/batch_event_job_monitor_cdk/__init__.py`
- Modify: `tests/batch_event_job_monitor_cdk/test_athena_tables.py`
- Modify: `docs/constructs.md`
- Modify: `README.md`

- [ ] **Step 1: Remove the construct's tests**

Delete the `AthenaRecordsTable` import, the `AthenaRecordsTable(...)` block inside `_make_stack`, and every test
function that asserts on the records table, from `tests/batch_event_job_monitor_cdk/test_athena_tables.py`. Leave the
state and outputs table tests untouched.

- [ ] **Step 2: Run tests to verify the suite still passes**

Run: `./scripts/test tests/batch_event_job_monitor_cdk/test_athena_tables.py -v` Expected: all remaining tests pass

- [ ] **Step 3: Delete the construct and its references**

```bash
git rm src/batch_event_job_monitor_cdk/athena_records_table.py
```

Remove the `AthenaRecordsTable` import and `__all__` entry from `src/batch_event_job_monitor_cdk/__init__.py`. Remove
its section from `docs/constructs.md` and its mention from the `README.md` construct list, replacing both with a pointer
to `IcebergRecordsTable`.

- [ ] **Step 4: Run the full suite, lint, and typecheck**

Run: `./scripts/test && ./scripts/lint && ./scripts/typecheck` Expected: all pass, and no reference to
`AthenaRecordsTable` remains:

```bash
! grep -rn "AthenaRecordsTable" src tests docs README.md
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: retire AthenaRecordsTable in favour of the Iceberg rollup"
```

---

## Integration test (opt-in, deferred)

The spec calls for an integration test against a deployed dev stack. MERGE semantics cannot be verified any other way,
and it needs real infrastructure, so it is not part of the task sequence above. Add it as a separately marked suite once
a dev stack exists, covering:

1. Insert - merge one new record, assert one row.
2. Update in place - re-merge the same key with an extra event, assert still one row with the longer `events` array.
3. Stale replay - merge a record whose `last_event_timestamp` is older than the stored row, assert the row is unchanged.
4. Reconcile - withhold one key from the queue, run reconcile, assert it is enqueued and subsequently merged. This is
   also the backfill test.
