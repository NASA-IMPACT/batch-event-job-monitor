"""Column definitions and SQL generation for the Iceberg records rollup.

Shared by the rollup Lambda and the CDK constructs so the rolled-up schema
is declared exactly once. Partition keys arrive as plain names rather than
PartitionKeySpec objects, keeping this module free of any CDK import.
"""

from __future__ import annotations

import re

EVENTS_TYPE = (
    "array<struct<state:string,timestamp:string,batch_job_id:string,exit_code:int>>"
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
    ("log_stream_name", "string"),
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


def records_columns(partition_key_names: list[str]) -> list[tuple[str, str]]:
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
    column_sql = ",\n".join(
        f"{name} {col_type}" for name, col_type in records_columns(partition_key_names)
    )
    return f"""
        CREATE TABLE IF NOT EXISTS "{database}"."{table}" (
            {column_sql}
        )
        PARTITIONED BY ({partition_key_names[0]})
        LOCATION '{location}'
        TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
    """


_RUN_ID_PATTERN = re.compile(r"\A[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


class InvalidRunId(ValueError):
    """Raised when a run id is not a lowercase hyphenated UUID."""


def _staging_expression(name: str) -> str:
    if name == "last_event_timestamp":
        # The Iceberg column is a plain (zone-naive) timestamp, but
        # from_iso8601_timestamp() returns timestamp(3) with time zone.
        # Trino coerces naive -> zoned implicitly, not the reverse, so the
        # explicit CAST is required for this assignment to type-check.
        return 'CAST(from_iso8601_timestamp(s."last_event_timestamp") AS timestamp(6))'
    return f's."{name}"'


# current_timestamp is timestamp(3) with time zone; rolled_up_at is a plain
# (zone-naive) Iceberg column, so every assignment to it needs the same CAST.
_ROLLED_UP_AT_EXPRESSION = "CAST(current_timestamp AS timestamp(6))"


def merge_sql(
    *,
    database: str,
    records_table: str,
    staging_table: str,
    run_id: str,
    partition_key_names: list[str],
) -> str:
    """Build the MERGE upserting one staging run into the Iceberg table.

    Parameters
    ----------
    database : str
        Glue database holding both tables.
    records_table : str
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
    on_clause = "\nAND ".join(f't."{column}" = s."{column}"' for column in keys)

    updatable = [
        name
        for name, _ in records_columns(partition_key_names)
        if name not in keys and name != "rolled_up_at"
    ]
    set_clause = ",\n".join(
        f'"{name}" = {_staging_expression(name)}' for name in updatable
    )

    insert_names = [name for name, _ in records_columns(partition_key_names)]
    insert_columns = ", ".join(f'"{name}"' for name in insert_names)
    insert_values = ", ".join(
        _ROLLED_UP_AT_EXPRESSION
        if name == "rolled_up_at"
        else _staging_expression(name)
        for name in insert_names
    )

    return f"""
        MERGE INTO "{database}"."{records_table}" t
        USING (
            SELECT * FROM "{database}"."{staging_table}" WHERE run_id = '{run_id}'
        ) s
            ON {on_clause}
        WHEN MATCHED AND from_iso8601_timestamp(s."last_event_timestamp")
                         >= t."last_event_timestamp" THEN
            UPDATE SET {set_clause},
                       "rolled_up_at" = {_ROLLED_UP_AT_EXPRESSION}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
    """


def reconcile_sql(
    *,
    database: str,
    records_table: str,
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
    records_table : str
        Iceberg table name.
    inventory_table : str
        S3-inventory table over the records/ prefix.

    Returns
    -------
    str
        A SELECT returning one source_key column.
    """
    return f"""
        SELECT inv."key" AS source_key
        FROM "{database}"."{inventory_table}" inv
        LEFT JOIN "{database}"."{records_table}" t
            ON t."source_key" = inv."key"
        WHERE inv."dt" = (SELECT max("dt") FROM "{database}"."{inventory_table}")
          AND inv."is_latest"
          AND NOT inv."is_delete_marker"
          AND (
            t."source_key" IS NULL
            OR inv."last_modified_date" > t."rolled_up_at"
          )
    """
