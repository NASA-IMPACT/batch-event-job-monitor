"""CDK construct for the Athena state-inventory database.

Creates a Glue database + S3-inventory table over the ``state/`` prefix of
the processing bucket, plus a view that parses each key into structured
columns.

Key schema:
  ``state/state={STATE}/job_type={job_type}/{partition_fields...}/entity_id={entity_id}/{attempt:03d}``

The inventory snapshot (daily Parquet) is cheap to query and supports
reconciliation queries such as:

  - Count entities by state / partition
  - Find entities stuck in SUBMITTED or AWAITING for a given partition
  - Diff today vs yesterday to measure throughput
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


class AthenaStateDatabase(Construct):
    """Athena database for reconciliation queries over state/ pointer objects.

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
        s3:// URI of the state prefix's S3 Inventory Hive symlink manifests.
    table_datetime_start : dt.datetime
        Anchor datetime for the inventory table's ``dt`` partition
        projection. Its time-of-day must match the S3 delivery hour.
    table_name : str
        Name of the inventory table.
    view_name : str
        Name of the current-state view.
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading ``job_type`` entry.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    database : glue.CfnDatabase
        The Glue database passed in.
    inventory_table : glue.CfnTable
        The S3-inventory table over the state/ prefix.
    state_view : glue.CfnTable
        The current-state Presto view.
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

        self.state_view = create_presto_view(
            self,
            "StateView",
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
                comment=(
                    "Processing state (SUBMITTED, AWAITING, SUCCESS, "
                    "FAILURE_RETRYABLE, FAILURE_NONRETRYABLE)."
                ),
            ),
            *(
                glue.CfnTable.ColumnProperty(name=key.name, type="string")
                for key in partition_keys
            ),
            glue.CfnTable.ColumnProperty(
                name="entity_id",
                type="string",
                comment="Processed entity identifier.",
            ),
            glue.CfnTable.ColumnProperty(
                name="attempt",
                type="int",
                comment="Attempt number (0-indexed).",
            ),
            glue.CfnTable.ColumnProperty(
                name="last_modified_date",
                type="timestamp",
                comment="When the state pointer was last written.",
            ),
            glue.CfnTable.ColumnProperty(
                name="key",
                type="string",
                comment="Full S3 key of the state pointer object.",
            ),
        ]

    @staticmethod
    def _view_sql(table_name: str, partition_keys: list[PartitionKeySpec]) -> str:
        partition_columns = regexp_extract_columns(partition_keys)
        return f"""
        SELECT
            regexp_extract(key, '/state=([^/]+)/', 1) AS state,
            {partition_columns},
            regexp_extract(key, '/entity_id=([^/]+)/', 1) AS entity_id,
            CAST(regexp_extract(key, '/([0-9]{{3}})$', 1) AS INT) AS attempt,
            last_modified_date,
            key
        FROM {table_name}
        WHERE dt = (SELECT max(dt) FROM {table_name})
          AND is_latest
          AND NOT is_delete_marker
        """
