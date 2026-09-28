"""Rolling canonical record objects up into the Iceberg records table."""

from __future__ import annotations

import gzip
import json
import logging
import os
import tempfile
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from botocore.exceptions import ClientError

from batch_event_job_monitor.rollup_schema import key_columns, merge_sql, reconcile_sql

logger = logging.getLogger(__name__)

_RECEIVE_BATCH = 10
_DELETE_BATCH = 10

_MISSING_OBJECT_CODES = ("NoSuchKey", "404", "NotFound")


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
        # Any message SQS returns goes invisible immediately, whether or not
        # this loop uses it. Capping the request at the remaining need keeps
        # every returned message on a path to either a captured receipt
        # handle or staying visible for the queue-depth check that follows.
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(_RECEIVE_BATCH, max_keys - len(seen)),
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
            if len(seen) >= max_keys:
                break

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

    A 404 on a source object is not an error -- the object was deleted
    between notification and read -- and is counted as missing rather than
    raised. Malformed JSON is likewise counted rather than raised, because
    the weekly reconcile pass re-surfaces it.

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
        except (
            json.JSONDecodeError,
            MalformedRecord,
            KeyError,
            TypeError,
            AttributeError,
        ):
            logger.warning("Skipping unreadable record %s", key)
            return "malformed"

    result = FetchResult()
    if not keys:
        return result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for outcome in pool.map(fetch_one, keys):
            if isinstance(outcome, str):
                if outcome == "missing":
                    result.missing += 1
                else:
                    result.malformed += 1
            else:
                result.rows.append(outcome)
    return result


def dedupe_rows_by_natural_key(
    rows: list[dict[str, Any]], partition_key_names: list[str]
) -> list[dict[str, Any]]:
    """Collapse rows sharing a natural key, keeping the newest by event time.

    drain_keys deduplicates by S3 object key, but the MERGE joins on the
    natural key (key_columns): partition_key_names plus input_entity_id plus
    attempt, all read from the object body. Those coincide only when
    partition_fields contains exactly the declared partition keys -- any
    extra partition field is part of the S3 key but dropped from the
    natural key, so two distinct objects can collapse to one target row.
    Iceberg MERGE requires at most one source row per target row, so this
    must run before the staging object is written.

    Parameters
    ----------
    rows : list[dict[str, Any]]
        Flattened staging rows, possibly repeating a natural key.
    partition_key_names : list[str]
        Ordered partition key names, job_type first.

    Returns
    -------
    list[dict[str, Any]]
        One row per distinct natural key: the one with the greatest
        last_event_timestamp. ISO 8601 timestamps sharing a fixed UTC
        offset and digit width sort lexicographically in chronological
        order, so no parsing is needed to compare them.
    """
    newest: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[name] for name in key_columns(partition_key_names))
        current = newest.get(key)
        if current is None or (
            row["last_event_timestamp"] > current["last_event_timestamp"]
        ):
            newest[key] = row
    return list(newest.values())


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
    memory, so a large backlog (up to 25,000 objects in one run) cannot
    exhaust the Lambda heap.

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


_METRIC_UNITS = {"MergeDurationMs": "Milliseconds"}
_DEFAULT_METRIC_UNIT = "Count"


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
                    "Metrics": [
                        {
                            "Name": name,
                            "Unit": _METRIC_UNITS.get(name, _DEFAULT_METRIC_UNIT),
                        }
                        for name in metrics
                    ],
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


def _chain(
    *, config: RollupConfig, clients: Clients, mode: str, depth: int, **extra: Any
) -> bool:
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
    the natural key, so replay is safe. Metrics are emitted on both the
    success and failure paths, since a failed merge is itself signal.

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

    Raises
    ------
    AthenaQueryError
        If the merge reaches a terminal failure state. Raised after metrics
        for this run are emitted, and before any SQS message or the staging
        object is deleted.
    """
    drained = drain_keys(
        sqs_client=clients.sqs,
        queue_url=config.queue_url,
        max_keys=config.max_keys,
    )

    metrics: dict[str, int] = {
        "RolledUpRecords": 0,
        "MergeDurationMs": 0,
        "MissingSourceObjects": 0,
        "MalformedRecords": 0,
        "RollupChainDepth": depth,
        "RollupFailures": 0,
    }

    fetched = fetch_rows(
        s3_client=clients.s3,
        bucket=config.bucket,
        keys=drained.keys,
        partition_key_names=config.partition_key_names,
    )
    metrics["MissingSourceObjects"] = fetched.missing
    metrics["MalformedRecords"] = fetched.malformed

    if fetched.rows:
        deduped_rows = dedupe_rows_by_natural_key(
            fetched.rows, config.partition_key_names
        )
        run_id = str(uuid.uuid4())
        staging_key = write_staging_object(
            s3_client=clients.s3,
            bucket=config.bucket,
            staging_prefix=config.staging_prefix,
            run_id=run_id,
            rows=deduped_rows,
        )
        sql = merge_sql(
            database=config.database,
            iceberg_table=config.iceberg_table,
            staging_table=config.staging_table,
            run_id=run_id,
            partition_key_names=config.partition_key_names,
        )
        merge_start = time.monotonic()
        try:
            run_query(
                athena_client=clients.athena,
                sql=sql,
                workgroup=config.workgroup,
            )
        except AthenaQueryError:
            metrics["MergeDurationMs"] = int((time.monotonic() - merge_start) * 1000)
            metrics["RollupFailures"] = 1
            emit_metrics(namespace=config.metric_namespace, metrics=metrics)
            raise
        metrics["MergeDurationMs"] = int((time.monotonic() - merge_start) * 1000)
        clients.s3.delete_object(Bucket=config.bucket, Key=staging_key)
        metrics["RolledUpRecords"] = len(deduped_rows)

    _delete_messages(
        sqs_client=clients.sqs,
        queue_url=config.queue_url,
        handles=drained.receipt_handles,
    )

    if queue_has_messages(sqs_client=clients.sqs, queue_url=config.queue_url):
        _chain(config=config, clients=clients, mode="rollup", depth=depth + 1)

    emit_metrics(namespace=config.metric_namespace, metrics=metrics)
    return metrics


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

    Paging continues an existing result set rather than re-running the
    query: when query_execution_id is passed in, this resumes
    GetQueryResults from next_token instead of issuing a new query, which
    keeps the snapshot consistent across chained hops.

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

    # The header row only appears on a result set's first page. next_token
    # absent means this call is fetching page one, whether or not the query
    # was just started here.
    skip_header = next_token is None

    enqueued = 0
    truncated = 0
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
            _enqueue_keys(sqs_client=clients.sqs, queue_url=config.queue_url, keys=keys)
            enqueued += len(keys)

        next_token = page.get("NextToken")
        if next_token is None:
            break
        if time_remaining_ms() < RECONCILE_TIME_BUDGET_MS:
            chained = _chain(
                config=config,
                clients=clients,
                mode="reconcile",
                depth=depth + 1,
                query_execution_id=query_execution_id,
                next_token=next_token,
            )
            # _chain returns False only at max_depth: rows remain
            # (next_token is not None) but nothing was invoked to enqueue
            # them, so this pass gave up rather than converged.
            truncated = 0 if chained else 1
            break

    metrics = {
        "ReconcileDrift": enqueued,
        "ReconcileChainDepth": depth,
        "ReconcileTruncated": truncated,
    }
    emit_metrics(namespace=config.metric_namespace, metrics=metrics)
    return metrics
