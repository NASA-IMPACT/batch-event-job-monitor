"""Bundled Lambda handler for RecordsRollupFunction.

One handler serves both modes. The rollup mode is the scheduled default;
the reconcile mode is invoked on a separate schedule and by both modes'
self-invocation chains, which carry their continuation state in the event.
"""

from __future__ import annotations

import os
from typing import Any

import boto3
from botocore.config import Config

from batch_event_job_monitor.rollup import (
    Clients,
    RollupConfig,
    run_reconcile,
    run_rollup,
)
from batch_event_job_monitor.untracked import DEFAULT_METRIC_NAMESPACE

# fetch_rows' thread pool defaults to 32 workers; boto3's default connection
# pool is 10, so without this every run above 10 in-flight GETs blocks
# threads on pool checkout and urllib3 logs "Connection pool is full" on
# every run at the per-run cap.
_S3_CLIENT_CONFIG = Config(max_pool_connections=32)

_clients = Clients(
    s3=boto3.client("s3", config=_S3_CLIENT_CONFIG),
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
