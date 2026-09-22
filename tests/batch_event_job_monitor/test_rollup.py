"""Tests for the records rollup runtime."""

from __future__ import annotations

import gzip
import json
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from batch_event_job_monitor.rollup import (
    AthenaQueryError,
    MalformedRecord,
    drain_keys,
    fetch_rows,
    queue_has_messages,
    record_to_row,
    run_query,
    write_staging_object,
)

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]

RUN_ID = "0f9d7a1c-2b3e-4c5d-8e9f-0a1b2c3d4e5f"

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


def _send(sqs: SQSClient, queue_url: str, key: str) -> None:
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"detail": {"object": {"key": key}}}),
    )


def test_drain_deduplicates_repeated_keys(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    for _ in range(3):
        _send(sqs, rollup_queue_url, "records/job_type=a/001.json")

    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=100)

    assert drained.keys == ["records/job_type=a/001.json"]
    assert len(drained.receipt_handles) == 3


def test_drain_stops_at_the_cap(sqs: SQSClient, rollup_queue_url: str) -> None:
    for index in range(12):
        _send(sqs, rollup_queue_url, f"records/job_type=a/{index:03d}.json")

    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=5)

    assert len(drained.keys) <= 5


def test_drain_of_an_empty_queue_returns_nothing(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=100)
    assert drained.keys == []
    assert drained.receipt_handles == []


def test_queue_has_messages_reflects_queue_state(
    sqs: SQSClient, rollup_queue_url: str
) -> None:
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    _send(sqs, rollup_queue_url, "records/job_type=a/001.json")
    assert queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)


def _put_record(s3: S3Client, bucket: str, key: str, body: dict[str, Any]) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body).encode())


def test_fetch_returns_one_row_per_object(s3: S3Client, bucket: str) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=[SOURCE_KEY],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert len(result.rows) == 1
    assert result.rows[0]["source_key"] == SOURCE_KEY
    assert result.missing == 0
    assert result.malformed == 0


def test_fetch_skips_and_counts_a_missing_object(s3: S3Client, bucket: str) -> None:
    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=["records/job_type=a/gone.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.missing == 1


def test_fetch_skips_and_counts_malformed_json(s3: S3Client, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key="records/bad.json", Body=b"{not json")

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=["records/bad.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.malformed == 1


@pytest.mark.parametrize("body", [b"42", b"[1, 2, 3]"])
def test_fetch_skips_and_counts_valid_json_that_is_not_an_object(
    s3: S3Client, bucket: str, body: bytes
) -> None:
    s3.put_object(Bucket=bucket, Key="records/not-an-object.json", Body=body)

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=["records/not-an-object.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.malformed == 1
    assert result.missing == 0


def test_fetch_counts_a_non_object_record_without_discarding_a_good_row(
    s3: S3Client, bucket: str
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    s3.put_object(Bucket=bucket, Key="records/not-an-object.json", Body=b"42")

    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=[SOURCE_KEY, "records/not-an-object.json"],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert len(result.rows) == 1
    assert result.rows[0]["source_key"] == SOURCE_KEY
    assert result.malformed == 1


def test_fetch_returns_nothing_for_an_empty_key_list(s3: S3Client, bucket: str) -> None:
    result = fetch_rows(
        s3_client=s3,
        bucket=bucket,
        keys=[],
        partition_key_names=PARTITION_KEY_NAMES,
    )

    assert result.rows == []
    assert result.missing == 0
    assert result.malformed == 0


def test_staging_object_is_gzipped_ndjson(s3: S3Client, bucket: str) -> None:
    rows = [
        record_to_row(
            source_key=SOURCE_KEY,
            body=_body(),
            partition_key_names=PARTITION_KEY_NAMES,
        )
    ]

    key = write_staging_object(
        s3_client=s3,
        bucket=bucket,
        staging_prefix="staging/",
        run_id=RUN_ID,
        rows=rows,
    )

    assert key == f"staging/run_id={RUN_ID}/part.ndjson.gz"
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    lines = gzip.decompress(raw).decode().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["tile_id"] == "12TVK"


class FakeAthena:
    """Minimal Athena client returning a scripted sequence of states."""

    def __init__(self, states: list[str]) -> None:
        self.states = states
        self.started: list[str] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, str]:
        self.started.append(kwargs["QueryString"])
        return {"QueryExecutionId": "qid-1"}

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]:
        state = self.states.pop(0)
        return {
            "QueryExecution": {"Status": {"State": state, "StateChangeReason": "boom"}}
        }


def test_run_query_returns_the_execution_id_on_success() -> None:
    client = FakeAthena(["RUNNING", "SUCCEEDED"])
    sleep_calls: list[float] = []
    query_id = run_query(
        athena_client=client,
        sql="SELECT 1",
        workgroup="wg",
        poll_seconds=0.5,
        sleep=sleep_calls.append,
    )
    assert query_id == "qid-1"
    assert client.started == ["SELECT 1"]
    assert sleep_calls == [0.5]


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_run_query_raises_on_a_terminal_failure(state: str) -> None:
    client = FakeAthena([state])
    sleep_calls: list[float] = []
    with pytest.raises(AthenaQueryError, match="boom"):
        run_query(
            athena_client=client,
            sql="SELECT 1",
            workgroup="wg",
            sleep=sleep_calls.append,
        )
    assert sleep_calls == []
