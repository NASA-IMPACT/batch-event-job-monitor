"""Shared helpers for the Athena/Glue constructs.

Centralizes the Hive format strings, the S3 Inventory column schema, and the two
CfnTable factories (inventory table + Presto view) reused by the state, outputs,
and records databases.
"""

from __future__ import annotations

import base64
import datetime as dt
import json

from aws_cdk import Aws, RemovalPolicy, aws_glue as glue
from constructs import Construct

# Hive formats. Presto/Trino views and S3-inventory tables all read via the
# symlink input format; the "output format" is nominal for reads but required.
SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
HIVE_TEXT_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

# JSON formats (records/ table over Hive-partitioned JSON objects).
JSON_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
JSON_SERDE = "org.openx.data.jsonserde.JsonSerDe"

# The dt partition of an S3-Inventory table: one report per delivery datetime.
DT_PARTITION_KEY = glue.CfnTable.ColumnProperty(
    name="dt",
    type="string",
    comment="Inventory report datetime (yyyy-MM-dd-HH-mm).",
)

# S3 Inventory Parquet report columns -- identical for every source prefix.
INVENTORY_COLUMNS = [
    glue.CfnTable.ColumnProperty(
        name="bucket", type="string", comment="Source bucket name."
    ),
    glue.CfnTable.ColumnProperty(name="key", type="string", comment="Object key."),
    glue.CfnTable.ColumnProperty(
        name="version_id", type="string", comment="Object version ID."
    ),
    glue.CfnTable.ColumnProperty(
        name="is_latest", type="boolean", comment="True if this is the current version."
    ),
    glue.CfnTable.ColumnProperty(
        name="is_delete_marker",
        type="boolean",
        comment="True if this is a delete marker.",
    ),
    glue.CfnTable.ColumnProperty(
        name="last_modified_date",
        type="timestamp",
        comment="Object creation or last-modified date.",
    ),
]


def athena_to_presto(athena_type: str | None) -> str:
    """Map a Glue/Athena column type to its Presto/Trino equivalent.

    Used when building the column list embedded in a Presto view spec.
    """
    if athena_type is None:
        raise ValueError("Cannot convert null Athena type")
    return {
        "string": "varchar",
        "struct": "row",
        "float": "real",
        "binary": "varbinary",
    }.get(athena_type.lower(), athena_type.lower())


def create_inventory_table(
    scope: Construct,
    construct_id: str,
    *,
    database: glue.CfnDatabase,
    table_name: str,
    location: str,
    datetime_start: dt.datetime,
) -> glue.CfnTable:
    """Create an EXTERNAL_TABLE over an S3 Inventory (symlink Parquet).

    Uses daily partition projection over ``dt`` so new inventory reports are
    queryable immediately -- no Glue crawler or MSCK REPAIR. ``datetime_start``
    anchors the projection and its time-of-day must match the S3 delivery hour.
    """
    table = glue.CfnTable(
        scope,
        construct_id,
        catalog_id=Aws.ACCOUNT_ID,
        database_name=database.ref,
        table_input=glue.CfnTable.TableInputProperty(
            name=table_name,
            table_type="EXTERNAL_TABLE",
            parameters={
                "EXTERNAL": "TRUE",
                "projection.enabled": "true",
                "projection.dt.type": "date",
                "projection.dt.format": "yyyy-MM-dd-HH-mm",
                "projection.dt.range": f"{datetime_start:%Y-%m-%d-%H-%M},NOW",
                "projection.dt.interval": "1",
                "projection.dt.interval.unit": "DAYS",
            },
            partition_keys=[DT_PARTITION_KEY],
            storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                columns=INVENTORY_COLUMNS,
                location=location,
                input_format=SYMLINK_INPUT_FORMAT,
                output_format=HIVE_TEXT_OUTPUT_FORMAT,
                serde_info=glue.CfnTable.SerdeInfoProperty(
                    serialization_library=PARQUET_SERDE,
                    parameters={"serialization.format": "1"},
                ),
            ),
        ),
    )
    table.apply_removal_policy(RemovalPolicy.DESTROY)
    table.add_dependency(database)
    return table


def create_presto_view(
    scope: Construct,
    construct_id: str,
    *,
    database: glue.CfnDatabase,
    database_name: str,
    view_name: str,
    sql: str,
    columns: list[glue.CfnTable.ColumnProperty],
    partition_keys: list[glue.CfnTable.ColumnProperty] | None = None,
    depends_on: glue.CfnTable | None = None,
) -> glue.CfnTable:
    """Create a Glue VIRTUAL_VIEW encoding an Athena/Presto view definition.

    The view spec is base64-encoded here at synth time, so CloudFormation cannot
    substitute tokens inside it. ``catalog`` is therefore the literal Athena
    catalog name ("awsdatacatalog", not the account id) and ``database_name``
    must be the literal Glue database name -- passing tokens would bake
    unresolved ``${Token[...]}`` placeholders into the stored view.
    """
    view_spec = {
        "originalSql": sql,
        "catalog": "awsdatacatalog",
        "schema": database_name,
        "columns": [
            {"name": col.name, "type": athena_to_presto(col.type)} for col in columns
        ],
    }
    sql_b64 = base64.b64encode(json.dumps(view_spec).encode()).decode()

    view = glue.CfnTable(
        scope,
        construct_id,
        catalog_id=Aws.ACCOUNT_ID,
        database_name=database.ref,
        table_input=glue.CfnTable.TableInputProperty(
            name=view_name,
            table_type="VIRTUAL_VIEW",
            parameters={"presto_view": "true", "comment": "Presto View"},
            partition_keys=partition_keys or [],
            storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                columns=columns,
                input_format=SYMLINK_INPUT_FORMAT,
                output_format=HIVE_TEXT_OUTPUT_FORMAT,
            ),
            view_original_text=f"/* Presto View: {sql_b64} */",
            view_expanded_text="/* Presto View */",
        ),
    )
    view.apply_removal_policy(RemovalPolicy.DESTROY)
    if depends_on is not None:
        view.add_dependency(depends_on)
    return view
