"""Tests for the records rollup runtime."""

from __future__ import annotations

from typing import Any

import pytest

from batch_event_job_monitor.rollup import MalformedRecord, record_to_row

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]

SOURCE_KEY = (
    "records/job_type=monthly-composite/tile_id=12TVK/year_month=2024-06/"
    "input_entity_id=12TVK_2024-06_source/001.json"
)


def _body() -> dict[str, Any]:
    return {
        "input_entity_id": "12TVK_2024-06_source",
        "output_entity_id": "HLS.COMPOSITE.T12TVK.202406.v2.0",
        "job_type": "monthly-composite",
        "partition_fields": {"tile_id": "12TVK", "year_month": "2024-06"},
        "attempt": 1,
        "batch_job_id": "abc-123",
        "current_state": "SUCCESS",
        "events": [
            {"state": "SUBMITTED", "timestamp": "2026-09-22T10:00:00+00:00"},
            {
                "state": "SUCCESS",
                "timestamp": "2026-09-22T10:05:00+00:00",
                "batch_job_id": "abc-123",
                "exit_code": 0,
            },
        ],
    }


def test_row_flattens_partition_fields_into_columns() -> None:
    row = record_to_row(
        source_key=SOURCE_KEY, body=_body(), partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["job_type"] == "monthly-composite"
    assert row["tile_id"] == "12TVK"
    assert row["year_month"] == "2024-06"


def test_row_takes_last_event_timestamp_from_the_final_event() -> None:
    row = record_to_row(
        source_key=SOURCE_KEY, body=_body(), partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["last_event_timestamp"] == "2026-09-22T10:05:00+00:00"


def test_row_carries_source_key_and_preserves_events_verbatim() -> None:
    body = _body()
    row = record_to_row(
        source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["source_key"] == SOURCE_KEY
    assert row["events"] == body["events"]


def test_row_tolerates_events_missing_optional_fields() -> None:
    body = _body()
    body["events"] = [{"state": "SUBMITTED", "timestamp": "2026-09-22T10:00:00+00:00"}]
    row = record_to_row(
        source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
    )
    assert row["events"][0] == {
        "state": "SUBMITTED",
        "timestamp": "2026-09-22T10:00:00+00:00",
    }


def test_row_rejects_a_record_with_no_events() -> None:
    body = _body()
    body["events"] = []
    with pytest.raises(MalformedRecord):
        record_to_row(
            source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
        )


def test_row_rejects_a_record_missing_a_declared_partition_field() -> None:
    body = _body()
    del body["partition_fields"]["year_month"]
    with pytest.raises(MalformedRecord):
        record_to_row(
            source_key=SOURCE_KEY, body=body, partition_key_names=PARTITION_KEY_NAMES
        )
