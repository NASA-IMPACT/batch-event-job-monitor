"""Column definitions and SQL generation for the Iceberg records rollup.

Shared by the rollup Lambda and the CDK constructs so the rolled-up schema
is declared exactly once. Partition keys arrive as plain names rather than
PartitionKeySpec objects, keeping this module free of any CDK import.
"""

from __future__ import annotations

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
        f'"{name}" {col_type}'
        for name, col_type in iceberg_columns(partition_key_names)
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
