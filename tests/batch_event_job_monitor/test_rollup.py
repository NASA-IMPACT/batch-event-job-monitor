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
    Clients,
    MalformedRecord,
    RollupConfig,
    drain_keys,
    fetch_rows,
    queue_has_messages,
    record_to_row,
    run_query,
    run_reconcile,
    run_rollup,
    write_staging_object,
)

PARTITION_KEY_NAMES = ["job_type", "tile_id", "year_month"]

_EXPECTED_METRIC_ENVELOPE = [
    {"Name": "RolledUpRecords", "Unit": "Count"},
    {"Name": "MergeDurationMs", "Unit": "Milliseconds"},
    {"Name": "MissingSourceObjects", "Unit": "Count"},
    {"Name": "MalformedRecords", "Unit": "Count"},
    {"Name": "ChainDepth", "Unit": "Count"},
    {"Name": "RollupFailures", "Unit": "Count"},
]

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


class FakeLambda:
    """Minimal Lambda client recording the invocations it was asked to make."""

    def __init__(self) -> None:
        self.invocations: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, int]:
        self.invocations.append(kwargs)
        return {"StatusCode": 202}


def _config(bucket: str, queue_url: str, **overrides: Any) -> RollupConfig:
    defaults: dict[str, Any] = {
        "bucket": bucket,
        "queue_url": queue_url,
        "staging_prefix": "staging/",
        "database": "test_db",
        "iceberg_table": "records_iceberg",
        "staging_table": "records_staging",
        "inventory_table": "records_inventory",
        "workgroup": "wg",
        "partition_key_names": PARTITION_KEY_NAMES,
        "max_keys": 100,
        "max_depth": 3,
        "function_name": "rollup-fn",
        "metric_namespace": "BatchEventJobMonitor",
    }
    defaults.update(overrides)
    return RollupConfig(**defaults)


def test_rollup_merges_and_deletes_messages_on_success(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)
    athena = FakeAthena(["SUCCEEDED"])
    lambda_ = FakeLambda()

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=lambda_),
        depth=0,
    )

    assert metrics["RolledUpRecords"] == 1
    assert "MERGE INTO" in athena.started[0]
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    attributes = sqs.get_queue_attributes(
        QueueUrl=rollup_queue_url,
        AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 0
    assert lambda_.invocations == []


def test_rollup_retains_messages_when_the_merge_fails(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)

    with pytest.raises(AthenaQueryError):
        run_rollup(
            config=_config(bucket, rollup_queue_url),
            clients=Clients(
                s3=s3, sqs=sqs, athena=FakeAthena(["FAILED"]), lambda_=FakeLambda()
            ),
            depth=0,
        )

    attributes = sqs.get_queue_attributes(
        QueueUrl=rollup_queue_url,
        AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 1


def test_rollup_skips_the_merge_when_nothing_was_drained(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthena([])

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=0,
    )

    assert metrics["RolledUpRecords"] == 0
    assert athena.started == []


def test_rollup_deletes_messages_when_every_drained_object_is_missing(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    _send(sqs, rollup_queue_url, "records/job_type=a/gone.json")
    athena = FakeAthena([])

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=0,
    )

    assert athena.started == []
    assert metrics["RolledUpRecords"] == 0
    assert metrics["MissingSourceObjects"] == 1
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    attributes = sqs.get_queue_attributes(
        QueueUrl=rollup_queue_url,
        AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 0


def test_rollup_deletes_a_message_with_no_extractable_key(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    sqs.send_message(QueueUrl=rollup_queue_url, MessageBody=json.dumps({"detail": {}}))

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=FakeAthena([]), lambda_=FakeLambda()),
        depth=0,
    )

    assert metrics["RolledUpRecords"] == 0
    assert not queue_has_messages(sqs_client=sqs, queue_url=rollup_queue_url)
    attributes = sqs.get_queue_attributes(
        QueueUrl=rollup_queue_url,
        AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attributes["ApproximateNumberOfMessagesNotVisible"]) == 0


def test_rollup_chains_while_the_queue_is_not_empty(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    for index in range(3):
        key = (
            "records/job_type=monthly-composite/tile_id=12TVK/"
            f"year_month=2024-06/input_entity_id=e{index}/001.json"
        )
        body = _body()
        body["input_entity_id"] = f"e{index}"
        _put_record(s3, bucket, key, body)
        _send(sqs, rollup_queue_url, key)

    lambda_ = FakeLambda()
    run_rollup(
        config=_config(bucket, rollup_queue_url, max_keys=1),
        clients=Clients(
            s3=s3, sqs=sqs, athena=FakeAthena(["SUCCEEDED"]), lambda_=lambda_
        ),
        depth=0,
    )

    assert len(lambda_.invocations) == 1
    payload = json.loads(lambda_.invocations[0]["Payload"])
    assert payload == {"mode": "rollup", "depth": 1}
    assert lambda_.invocations[0]["InvocationType"] == "Event"


def test_rollup_does_not_chain_past_max_depth(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    for index in range(3):
        _send(sqs, rollup_queue_url, f"records/job_type=a/{index:03d}.json")

    lambda_ = FakeLambda()
    run_rollup(
        config=_config(bucket, rollup_queue_url, max_keys=1, max_depth=1),
        clients=Clients(
            s3=s3, sqs=sqs, athena=FakeAthena(["SUCCEEDED"]), lambda_=lambda_
        ),
        depth=0,
    )

    assert lambda_.invocations == []


def test_rollup_emits_merge_duration_and_zero_failures_on_success(
    s3: S3Client,
    bucket: str,
    sqs: SQSClient,
    rollup_queue_url: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)

    metrics = run_rollup(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(
            s3=s3, sqs=sqs, athena=FakeAthena(["SUCCEEDED"]), lambda_=FakeLambda()
        ),
        depth=0,
    )

    assert metrics["RollupFailures"] == 0
    assert metrics["MergeDurationMs"] >= 0
    emitted = json.loads(capsys.readouterr().out.strip())
    assert emitted["RollupFailures"] == 0
    assert emitted["MergeDurationMs"] >= 0
    cloudwatch_metrics = emitted["_aws"]["CloudWatchMetrics"][0]
    assert cloudwatch_metrics["Namespace"] == "BatchEventJobMonitor"
    assert cloudwatch_metrics["Metrics"] == _EXPECTED_METRIC_ENVELOPE


def test_rollup_counts_a_failure_and_still_emits_metrics_on_a_failed_merge(
    s3: S3Client,
    bucket: str,
    sqs: SQSClient,
    rollup_queue_url: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _put_record(s3, bucket, SOURCE_KEY, _body())
    _send(sqs, rollup_queue_url, SOURCE_KEY)

    with pytest.raises(AthenaQueryError):
        run_rollup(
            config=_config(bucket, rollup_queue_url),
            clients=Clients(
                s3=s3, sqs=sqs, athena=FakeAthena(["FAILED"]), lambda_=FakeLambda()
            ),
            depth=0,
        )

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    emitted = json.loads(lines[0])
    assert emitted["RollupFailures"] == 1
    assert emitted["MergeDurationMs"] >= 0
    assert emitted["RolledUpRecords"] == 0
    cloudwatch_metrics = emitted["_aws"]["CloudWatchMetrics"][0]
    assert cloudwatch_metrics["Namespace"] == "BatchEventJobMonitor"
    assert cloudwatch_metrics["Metrics"] == _EXPECTED_METRIC_ENVELOPE


class FakeAthenaResults(FakeAthena):
    """Athena fake that also serves paged query results."""

    def __init__(self, states: list[str], pages: list[dict[str, Any]]) -> None:
        super().__init__(states)
        self.pages = pages
        self.requested_tokens: list[str | None] = []

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.requested_tokens.append(kwargs.get("NextToken"))
        return self.pages.pop(0)


def _page(keys: list[str], next_token: str | None, header: bool) -> dict[str, Any]:
    rows = [{"Data": [{"VarCharValue": "source_key"}]}] if header else []
    rows += [{"Data": [{"VarCharValue": key}]} for key in keys]
    page: dict[str, Any] = {"ResultSet": {"Rows": rows}}
    if next_token is not None:
        page["NextToken"] = next_token
    return page


def test_reconcile_enqueues_keys_and_skips_the_header_row(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults(
        ["SUCCEEDED"], [_page(["records/a.json", "records/b.json"], None, True)]
    )

    metrics = run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=0,
    )

    assert metrics["ReconcileDrift"] == 2
    assert "LEFT JOIN" in athena.started[0]
    drained = drain_keys(sqs_client=sqs, queue_url=rollup_queue_url, max_keys=10)
    assert sorted(drained.keys) == ["records/a.json", "records/b.json"]


def test_reconcile_chains_when_the_time_budget_runs_out(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults(
        ["SUCCEEDED"], [_page(["records/a.json"], "token-2", True)]
    )
    lambda_ = FakeLambda()

    run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=lambda_),
        depth=0,
        time_remaining_ms=lambda: 1_000,
    )

    payload = json.loads(lambda_.invocations[0]["Payload"])
    assert payload == {
        "mode": "reconcile",
        "depth": 1,
        "query_execution_id": "qid-1",
        "next_token": "token-2",
    }


def test_reconcile_continues_an_existing_result_without_a_new_query(
    s3: S3Client, bucket: str, sqs: SQSClient, rollup_queue_url: str
) -> None:
    athena = FakeAthenaResults([], [_page(["records/c.json"], None, False)])

    run_reconcile(
        config=_config(bucket, rollup_queue_url),
        clients=Clients(s3=s3, sqs=sqs, athena=athena, lambda_=FakeLambda()),
        depth=1,
        query_execution_id="qid-1",
        next_token="token-2",
    )

    assert athena.started == []
    assert athena.requested_tokens == ["token-2"]
