"""CDK construct for the Athena output-index table.

Creates an S3-inventory table over the ``outputs/`` prefix of the processing
bucket, in a Glue database the caller owns, plus a view that parses each
key into structured columns.

Key schema:
  ``outputs/state={STATE}/job_type={job_type}/{partition_fields...}/{output_entity_id}``

Output-index entries are written by the job monitor at terminal state for
all outcomes, keyed by ``output_entity_id``. This makes the inventory the
source for downstream reconciliation queries:

  - Compare SUCCESS output entity IDs against a downstream catalog
  - Coverage by partition: count produced outputs by partition key
  - Non-success rates over time
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from aws_cdk import aws_glue as glue
from constructs import Construct

from .athena_common import (
    DT_PARTITION_KEY,
    create_inventory_table,
    create_presto_view,
)
from .partition_key_spec import PartitionKeySpec, regexp_extract_columns


class AthenaOutputsTable(Construct):
    """Athena database for reconciliation queries over outputs/ index objects.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    database : glue.CfnDatabase
        Glue database the inventory table and view are created in.
    database_name : str
        Literal name of ``database`` (not a CDK token).
    inventory_location_s3path : str
        s3:// URI of the outputs prefix's S3 Inventory Hive symlink
        manifests.
    table_datetime_start : dt.datetime
        Anchor datetime for the inventory table's ``dt`` partition
        projection. Its time-of-day must match the S3 delivery hour.
    table_name : str
        Name of the inventory table.
    view_name : str
        Name of the current-outputs view.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading ``job_type`` entry.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    database : glue.CfnDatabase
        The Glue database passed in.
    inventory_table : glue.CfnTable
        The S3-inventory table over the outputs/ prefix.
    outputs_view : glue.CfnTable
        The current-outputs Presto view.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database: glue.CfnDatabase,
        database_name: str,
        inventory_location_s3path: str,
        table_datetime_start: dt.datetime,
        table_name: str,
        view_name: str,
        partition_keys: list[PartitionKeySpec],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.database = database

        self.inventory_table = create_inventory_table(
            self,
            "InventoryTable",
            database=database,
            table_name=table_name,
            location=inventory_location_s3path,
            datetime_start=table_datetime_start,
        )

        self.outputs_view = create_presto_view(
            self,
            "OutputsView",
            database=database,
            database_name=database_name,
            view_name=view_name,
            sql=self._view_sql(table_name, partition_keys),
            columns=self._view_columns(partition_keys),
            partition_keys=[DT_PARTITION_KEY],
            depends_on=self.inventory_table,
        )

    @staticmethod
    def _view_columns(
        partition_keys: list[PartitionKeySpec],
    ) -> list[glue.CfnTable.ColumnProperty]:
        return [
            glue.CfnTable.ColumnProperty(
                name="state",
                type="string",
                comment="Terminal state the output was indexed under.",
            ),
            *(
                glue.CfnTable.ColumnProperty(name=key.name, type="string")
                for key in partition_keys
            ),
            glue.CfnTable.ColumnProperty(
                name="output_entity_id",
                type="string",
                comment="Output entity identifier.",
            ),
            glue.CfnTable.ColumnProperty(
                name="last_modified_date",
                type="timestamp",
                comment="When the output-index entry was written.",
            ),
            glue.CfnTable.ColumnProperty(
                name="key",
                type="string",
                comment="Full S3 key of the output-index object.",
            ),
        ]

    @staticmethod
    def _view_sql(table_name: str, partition_keys: list[PartitionKeySpec]) -> str:
        partition_columns = regexp_extract_columns(partition_keys)
        # output_entity_id is the bare trailing path segment (no "key="
        # prefix), so it is captured directly rather than via a named
        # regexp_extract group keyed off the last partition key's name.
        return f"""
        SELECT
            regexp_extract(key, '/state=([^/]+)/', 1) AS state,
            {partition_columns},
            regexp_extract(key, '/([^/]+)$', 1) AS output_entity_id,
            last_modified_date,
            key
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
          AND is_latest
          AND NOT is_delete_marker
        """
