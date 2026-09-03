"""Tests for recording Batch jobs submitted without the bejm_* contract."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.untracked import (
    DEFAULT_METRIC_NAMESPACE,
    UNTRACKED_JOBS_METRIC,
    record_untracked_job,
    untracked_job_metric_record,
)

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/processing"
_NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)


def _detail(**overrides: Any) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "jobId": "job-abc123",
        "jobName": "manual-backfill",
        "jobQueue": _JOB_QUEUE_ARN,
        "jobDefinition": (
            "arn:aws:batch:us-west-2:123456789012:job-definition/composite:7"
        ),
        "status": "FAILED",
        "container": {"exitCode": 1},
    }
    detail.update(overrides)
    return detail


def _job(**overrides: Any) -> JobDetails:
    return JobDetails.from_event(_detail(**overrides))


class TestIsTracked:
    def test_untracked_without_the_job_type_parameter(self) -> None:
        assert _job().is_tracked is False

    def test_untracked_with_unrelated_parameters(self) -> None:
        assert _job(parameters={"tile_id": "14TPN"}).is_tracked is False

    def test_tracked_with_the_job_type_parameter(self) -> None:
        assert _job(parameters={"bejm_job_type": "composite"}).is_tracked is True

    def test_tracked_even_when_the_rest_of_the_contract_is_missing(self) -> None:
        # The Lambda's tracked/untracked split must match the EventBridge
        # rule's, which tests only bejm_job_type. A job past this point but
        # missing the rest fails loudly in decode_job_group.
        job = _job(parameters={"bejm_job_type": "composite"})
        assert job.is_tracked is True


class TestUntrackedJobMetricRecord:
    def test_emits_one_count_of_the_untracked_metric(self) -> None:
        record = untracked_job_metric_record(_job(), now=lambda: _NOW)
        assert record[UNTRACKED_JOBS_METRIC] == 1
        [metrics] = record["_aws"]["CloudWatchMetrics"]
        assert metrics["Namespace"] == DEFAULT_METRIC_NAMESPACE
        assert metrics["Metrics"] == [{"Name": UNTRACKED_JOBS_METRIC, "Unit": "Count"}]

    def test_dimensioned_by_job_queue_name(self) -> None:
        record = untracked_job_metric_record(_job(), now=lambda: _NOW)
        [metrics] = record["_aws"]["CloudWatchMetrics"]
        assert metrics["Dimensions"] == [["JobQueue"]]
        assert record["JobQueue"] == "processing"

    def test_falls_back_to_unknown_queue(self) -> None:
        detail = _detail()
        del detail["jobQueue"]
        record = untracked_job_metric_record(
            JobDetails.from_event(detail), now=lambda: _NOW
        )
        assert record["JobQueue"] == "unknown"

    def test_timestamp_is_epoch_milliseconds(self) -> None:
        record = untracked_job_metric_record(_job(), now=lambda: _NOW)
        assert record["_aws"]["Timestamp"] == int(_NOW.timestamp() * 1000)

    def test_carries_the_job_identity(self) -> None:
        record = untracked_job_metric_record(_job(), now=lambda: _NOW)
        assert record["jobId"] == "job-abc123"
        assert record["jobName"] == "manual-backfill"
        assert record["jobQueueArn"] == _JOB_QUEUE_ARN
        assert record["status"] == "FAILED"
        assert record["exitCode"] == 1

    def test_namespace_is_overridable(self) -> None:
        record = untracked_job_metric_record(
            _job(), namespace="MyNamespace", now=lambda: _NOW
        )
        [metrics] = record["_aws"]["CloudWatchMetrics"]
        assert metrics["Namespace"] == "MyNamespace"


class TestRecordUntrackedJob:
    def test_emits_one_json_line(self) -> None:
        emitted: list[str] = []
        record = record_untracked_job(_job(), now=lambda: _NOW, emit=emitted.append)
        assert len(emitted) == 1
        assert json.loads(emitted[0]) == record

    def test_message_names_the_missing_contract(self) -> None:
        record = record_untracked_job(_job(), now=lambda: _NOW, emit=lambda _: None)
        assert "bejm_" in record["message"]
        assert "job-abc123" in record["message"]
