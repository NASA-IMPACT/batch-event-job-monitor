"""Tests for the bundled job-monitor Lambda handler's rule-path split."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import JobGroup, JobTypeConfig, ProcessingState


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

    def test_deletes_the_job_types_submitter_pointers(
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
            submitter_states=("AWAITING_ANCILLARY",),
        )
        monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)
        monkeypatch.setenv(
            "PROCESSING_JOB_TYPE_CONFIGS",
            json.dumps({"composite": config.to_dict()}),
        )
        store = S3RecordStore(bucket=bucket)
        [context] = job_group.contexts()
        awaiting_ancillary = ProcessingState.submitter("AWAITING_ANCILLARY")
        store.write_state_pointer_conditional(context=context, state=awaiting_ancillary)
        event = {
            **_event(status="RUNNABLE", parameters=job_group.to_batch_parameters()),
            "time": "2020-01-02T03:04:05Z",
        }

        _handler(None).handler(cast(Any, event), cast(Any, None))

        key = store.state_pointer_key(awaiting_ancillary, context)
        assert s3.list_objects_v2(Bucket=bucket, Prefix=key).get("KeyCount", 0) == 0


_SHADOW_CONFIG = JobTypeConfig(
    job_queue_arn="arn:aws:batch:us-west-2:123456789012:job-queue/legacy",
    job_definition_arn="arn:aws:batch:us-west-2:123456789012:job-definition/legacy",
    requires_bejm_parameters=False,
    route_failures=False,
)
_CONTRACT_CONFIG = JobTypeConfig(
    job_queue_arn="arn:aws:batch:us-west-2:123456789012:job-queue/processing",
    job_definition_arn="arn:aws:batch:us-west-2:123456789012:job-definition/composite",
)


def _resolve_legacy(job: JobDetails, log_store: S3RecordStore) -> JobGroup | None:
    granule = job.environment.get("GRANULE")
    if granule is None:
        return None
    return JobGroup.new(
        job_type="legacy",
        partition_fields={"year_month": "2024-06"},
        input_entity_ids=[granule],
        output_entity_id=f"out-{granule}",
    )


def _legacy_event(
    status: str = "SUCCEEDED", exit_code: int | None = None
) -> dict[str, Any]:
    container: dict[str, Any] = {"environment": [{"name": "GRANULE", "value": "g1"}]}
    if exit_code is not None:
        container["exitCode"] = exit_code
    return {
        **_event(status=status, container=container),
        "time": "2020-01-02T03:04:05Z",
    }


class TestUntrackedJobResolver:
    """make_handler(resolve_untracked=...) tracks jobs without bejm_* params."""

    @pytest.fixture
    def env(
        self,
        bucket: str,
        sqs: SQSClient,
        dlq_url: str,
        retry_queue_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PROCESSING_BUCKET_NAME", bucket)
        monkeypatch.setenv("JOB_RETRY_QUEUE_URL", retry_queue_url)
        monkeypatch.setenv("JOB_FAILURE_DLQ_URL", dlq_url)
        monkeypatch.setenv(
            "PROCESSING_JOB_TYPE_CONFIGS",
            json.dumps(
                {
                    "legacy": _SHADOW_CONFIG.to_dict(),
                    "composite": _CONTRACT_CONFIG.to_dict(),
                }
            ),
        )

    def _make_handler(self, resolver: Any = _resolve_legacy) -> Any:
        return _handler(None).make_handler(resolve_untracked=resolver)

    def test_records_the_resolved_job_group(
        self, env: None, s3: S3Client, bucket: str
    ) -> None:
        result = self._make_handler()(cast(Any, _legacy_event()), cast(Any, None))

        assert result == {"state": "SUCCESS"}
        [context] = _resolve_legacy(
            JobDetails.from_event(_legacy_event()["detail"]), cast(Any, None)
        ).contexts()  # type: ignore[union-attr]
        key = S3RecordStore(bucket=bucket).canonical_key(context)
        record = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        assert record["input_entity_id"] == "g1"
        assert record["job_type"] == "legacy"

    def test_unrecognized_job_is_recorded_as_untracked(
        self, env: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        handler = self._make_handler()
        result = handler(cast(Any, _event()), cast(Any, None))
        assert result == {"state": _handler(None).UNTRACKED}
        record = json.loads(capsys.readouterr().out.strip())
        assert record["UntrackedJobs"] == 1

    def test_not_consulted_for_jobs_carrying_the_parameters(self, env: None) -> None:
        def _fail(job: JobDetails, log_store: S3RecordStore) -> JobGroup | None:
            raise AssertionError("resolver consulted for a tracked job")

        job_group = JobGroup.new(
            job_type="composite",
            partition_fields={"year_month": "2024-06"},
            input_entity_ids=["entity-1"],
            output_entity_id="output-1",
        )
        event = {
            **_event(parameters=job_group.to_batch_parameters()),
            "time": "2020-01-02T03:04:05Z",
        }
        result = self._make_handler(_fail)(cast(Any, event), cast(Any, None))
        assert result == {"state": "SUCCESS"}

    def test_resolving_to_a_job_type_requiring_the_parameters_raises(
        self, env: None
    ) -> None:
        def _to_composite(job: JobDetails, log_store: S3RecordStore) -> JobGroup:
            return JobGroup.new(
                job_type="composite",
                partition_fields={},
                input_entity_ids=["g1"],
                output_entity_id="out",
            )

        with pytest.raises(ValueError, match="requires them"):
            self._make_handler(_to_composite)(
                cast(Any, _legacy_event()), cast(Any, None)
            )

    def test_resolving_to_an_unconfigured_job_type_raises(self, env: None) -> None:
        def _to_unknown(job: JobDetails, log_store: S3RecordStore) -> JobGroup:
            return JobGroup.new(
                job_type="unknown",
                partition_fields={},
                input_entity_ids=["g1"],
                output_entity_id="out",
            )

        with pytest.raises(ValueError, match="has no JobTypeConfig"):
            self._make_handler(_to_unknown)(cast(Any, _legacy_event()), cast(Any, None))

    def test_route_failures_false_sends_nothing_to_the_dlq(
        self, env: None, sqs: SQSClient, dlq_url: str
    ) -> None:
        event = _legacy_event(status="FAILED", exit_code=1)
        result = self._make_handler()(cast(Any, event), cast(Any, None))

        assert result == {"state": "FAILURE_NONRETRYABLE"}
        messages = sqs.receive_message(QueueUrl=dlq_url).get("Messages", [])
        assert messages == []

    def test_route_failures_true_sends_terminal_failures_to_the_dlq(
        self, env: None, sqs: SQSClient, dlq_url: str
    ) -> None:
        job_group = JobGroup.new(
            job_type="composite",
            partition_fields={"year_month": "2024-06"},
            input_entity_ids=["entity-1"],
            output_entity_id="output-1",
        )
        event = {
            **_event(
                status="FAILED",
                container={"exitCode": 1},
                parameters=job_group.to_batch_parameters(),
            ),
            "time": "2020-01-02T03:04:05Z",
        }
        self._make_handler()(cast(Any, event), cast(Any, None))

        messages = sqs.receive_message(QueueUrl=dlq_url).get("Messages", [])
        assert len(messages) == 1
