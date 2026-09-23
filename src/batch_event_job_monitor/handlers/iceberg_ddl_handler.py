"""CloudFormation custom resource handler creating the Iceberg records table.

Athena DDL rather than a Glue CfnTable: a Glue catalog entry for an Iceberg
table is only valid once the table's metadata file exists, which CREATE TABLE
writes and CloudFormation cannot.
"""

from __future__ import annotations

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
    old_columns: list[tuple[str, str]],
    new_columns: list[tuple[str, str]],
) -> list[str]:
    """Build ALTER statements adding columns introduced since the last deploy.

    A column present in both schemas keeps its declared type: an Iceberg
    ADD COLUMN cannot retype an existing column, so a name that stayed but
    changed type is caught here rather than being silently dropped from the
    generated statements.

    Parameters
    ----------
    database : str
        Glue database name.
    table : str
        Iceberg table name.
    old_columns : list[tuple[str, str]]
        (name, type) pairs the deployed table already has.
    new_columns : list[tuple[str, str]]
        (name, type) pairs the new schema declares.

    Returns
    -------
    list[str]
        One ALTER TABLE ADD COLUMNS statement per added column.

    Raises
    ------
    IncompatibleSchemaChange
        If any deployed column is absent from the new schema, or present
        under a different type.
    """
    old_by_name = dict(old_columns)
    new_by_name = dict(new_columns)

    removed = [name for name in old_by_name if name not in new_by_name]
    retyped = [
        name
        for name, old_type in old_by_name.items()
        if name in new_by_name and new_by_name[name] != old_type
    ]
    if removed or retyped:
        problems = [f"{name!r} removed" for name in removed]
        problems += [
            f"{name!r} retyped {old_by_name[name]!r} -> {new_by_name[name]!r}"
            for name in retyped
        ]
        raise IncompatibleSchemaChange(
            "manual migration required for incompatible columns: " + ", ".join(problems)
        )
    return [
        f'ALTER TABLE "{database}"."{table}" ADD COLUMNS ("{name}" {col_type})'
        for name, col_type in new_columns
        if name not in old_by_name
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
        old_properties = event["OldResourceProperties"]
        old_columns = iceberg_columns(old_properties["PartitionKeyNames"].split(","))
        for statement in added_column_sql(
            database=database,
            table=table,
            old_columns=old_columns,
            new_columns=iceberg_columns(partition_key_names),
        ):
            run_query(athena_client=_athena_client, sql=statement, workgroup=workgroup)
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
