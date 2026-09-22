"""Rolling canonical record objects up into the Iceberg records table."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_RECEIVE_BATCH = 10


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
