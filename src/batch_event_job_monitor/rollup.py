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
