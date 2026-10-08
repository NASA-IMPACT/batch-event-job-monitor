"""Tests for the bundled job-monitor Lambda handler's rule-path split."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from mypy_boto3_s3 import S3Client

from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import JobGroup, JobTypeConfig


def _event(**detail_overrides: Any) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "jobId": "job-abc123",
        "jobName": "manual-backfill",
        "jobQueue": "arn:aws:batch:us-west-2:123456789012:job-queue/processing",
        "jobDefinition": (
            "arn:aws:batch:us-west-2:123456789012:job-definition/composite:7"
        ),
        "status": "SUCCEEDED",
    }
    detail.update(detail_overrides)
    return {"detail": detail}


def _handler(aws_credentials: None) -> Any:
    from batch_event_job_monitor.handlers import job_monitor_handler

    return job_monitor_handler


class TestUntrackedJobs:
    """A job with no bejm_job_type parameter is recorded, not decoded."""

    def test_returns_the_untracked_state(
        self, aws_credentials: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        handler_module = _handler(aws_credentials)
        result = handler_module.handler(cast(Any, _event()), cast(Any, None))
        assert result == {"state": handler_module.UNTRACKED}

    def test_emits_the_untracked_metric(
        self, aws_credentials: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        handler_module = _handler(aws_credentials)
        handler_module.handler(cast(Any, _event()), cast(Any, None))
        record = json.loads(capsys.readouterr().out.strip())
        assert record["UntrackedJobs"] == 1
        assert record["jobId"] == "job-abc123"
        assert record["JobQueue"] == "processing"

    def test_metric_namespace_comes_from_the_environment(
        self,
        aws_credentials: None,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("MONITOR_METRIC_NAMESPACE", "MyNamespace")
        handler_module = _handler(aws_credentials)
        handler_module.handler(cast(Any, _event()), cast(Any, None))
        record = json.loads(capsys.readouterr().out.strip())
        [metrics] = record["_aws"]["CloudWatchMetrics"]
        assert metrics["Namespace"] == "MyNamespace"

    def test_no_bucket_or_config_lookup_is_needed(
        self,
        aws_credentials: None,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The untracked path runs before any env var the tracked path needs,
        # so a catch-all event never fails on missing configuration.
        monkeypatch.delenv("PROCESSING_BUCKET_NAME", raising=False)
        monkeypatch.delenv("PROCESSING_JOB_TYPE_CONFIGS", raising=False)
        handler_module = _handler(aws_credentials)
        handler_module.handler(cast(Any, _event()), cast(Any, None))


class TestTrackedButMalformedJobs:
    """A job carrying bejm_job_type but not the rest still fails loudly."""

    def test_missing_parameters_raise(self, aws_credentials: None) -> None:
        handler_module = _handler(aws_credentials)
        event = _event(parameters={"bejm_job_type": "composite"})
        with pytest.raises(ValueError, match="missing required monitoring parameters"):
            handler_module.handler(cast(Any, event), cast(Any, None))


class TestTrackedJobs:
    """A tracked job is decoded and recorded at the event's own time."""

    def test_records_the_eventbridge_event_time(
        self,
        s3: S3Client,
        bucket: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_group = JobGroup.new(
            job_type="composite",
            partition_fields={"year_month": "2024-06"},
            input_entity_ids=["entity-1"],
            output_entity_id="output-1",
        )
        config = JobTypeConfig(
            job_queue_arn="arn:aws:batch:us-west-2:123456789012:job-queue/processing",
            job_definition_arn=(
                "arn:aws:batch:us-west-2:123456789012:job-definition/composite"
            ),
        )
        monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)
        monkeypatch.setenv(
            "PROCESSING_JOB_TYPE_CONFIGS",
            json.dumps({"composite": config.to_dict()}),
        )
        event = {
            **_event(parameters=job_group.to_batch_parameters()),
            "time": "2020-01-02T03:04:05Z",
        }

        handler_module = _handler(None)
        result = handler_module.handler(cast(Any, event), cast(Any, None))

        assert result == {"state": "SUCCESS"}
        [context] = job_group.contexts()
        key = S3RecordStore(bucket=bucket).canonical_key(context)
        record = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        assert record["events"][0]["timestamp"] == "2020-01-02T03:04:05+00:00"
