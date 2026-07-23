"""Tests for monitor_job (reusable job-monitor orchestration)."""

import dataclasses
import json
from datetime import datetime, timezone
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

from batch_event_job_monitor.lambda_core import monitor_job
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    ExitCodeOutcome,
    ExitCodeOutcomesBuilder,
    JobContext,
    ProcessingState,
    ProcessingStates,
    RetryPolicy,
)

JOB_TYPE = "monthly-composite"
INPUT_ENTITY_ID = "12TVK_2024-06_source"
OUTPUT_ENTITY_ID = "12TVK_2024-06_output"
PARTITION_FIELDS = {"tile_id": "12TVK", "year_month": "2024-06"}
FIXED_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

CONTEXT = JobContext(
    job_type=JOB_TYPE,
    partition_fields=PARTITION_FIELDS,
    input_entity_id=INPUT_ENTITY_ID,
    output_entity_id=OUTPUT_ENTITY_ID,
    attempt=0,
)


def make_detail(**overrides: Any) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "jobId": "batch-job-123",
        "jobName": "monthly-composite-job",
        "jobQueue": "queue-arn",
        "status": "SUCCEEDED",
        "startedAt": 1700000000,
        "jobDefinition": "job-def-arn",
    }
    detail.update(overrides)
    return detail


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


def _fixed_now() -> datetime:
    return FIXED_NOW


def _seed_awaiting(store: S3RecordStore, context: JobContext = CONTEXT) -> None:
    store.write_state_pointer(
        context=context, new_state=ProcessingStates.AWAITING, old_state=None
    )


def _receive_all(sqs: SQSClient, queue_url: str) -> list[MessageTypeDef]:
    resp = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
    )
    return resp.get("Messages", [])


def _output_index_exists(s3: S3Client, bucket: str, state: ProcessingState) -> bool:
    key = S3RecordStore.output_index_key(state, CONTEXT)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    return resp.get("KeyCount", 0) == 1


class TestSuccessPath:
    def test_returns_success_state(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        result = monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingStates.SUCCESS

    def test_writes_output_index(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert _output_index_exists(s3, bucket, ProcessingStates.SUCCESS)

    def test_no_sqs_messages_sent(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert _receive_all(sqs, retry_queue_url) == []
        assert _receive_all(sqs, dlq_url) == []

    def test_appends_canonical_event_with_injected_timestamp(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        key = S3RecordStore.canonical_key(CONTEXT)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["current_state"] == "SUCCESS"
        assert len(record["events"]) == 1
        event = record["events"][0]
        assert event["state"] == "SUCCESS"
        assert event["timestamp"] == FIXED_NOW.isoformat()
        assert event["batch_job_id"] == "batch-job-123"

    def test_writes_state_pointer_and_removes_old(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        success_key = S3RecordStore.state_pointer_key(ProcessingStates.SUCCESS, CONTEXT)
        assert (
            s3.list_objects_v2(Bucket=bucket, Prefix=success_key).get("KeyCount", 0)
            == 1
        )

        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingStates.AWAITING, CONTEXT
        )
        assert (
            s3.list_objects_v2(Bucket=bucket, Prefix=awaiting_key).get("KeyCount", 0)
            == 0
        )


class TestFailureRetryableWithAttemptsRemaining:
    def test_sends_to_retry_queue(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        result = monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingStates.FAILURE_RETRYABLE

        retry_messages = _receive_all(sqs, retry_queue_url)
        assert len(retry_messages) == 1
        body = json.loads(retry_messages[0]["Body"])
        assert body["job_type"] == JOB_TYPE
        assert body["partition_fields"] == PARTITION_FIELDS
        assert body["input_entity_id"] == INPUT_ENTITY_ID
        assert body["output_entity_id"] == OUTPUT_ENTITY_ID
        assert body["attempt"] == 0
        assert body["batch_job_id"] == "batch-job-123"

        assert _receive_all(sqs, dlq_url) == []

    def test_does_not_write_output_index(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert not _output_index_exists(s3, bucket, ProcessingStates.FAILURE_RETRYABLE)

    def test_no_retry_queue_url_sends_nothing(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=None,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert _receive_all(sqs, retry_queue_url) == []
        assert _receive_all(sqs, dlq_url) == []


class TestFailureRetryableAttemptsExhausted:
    def test_writes_output_index_and_sends_to_dlq_not_retry_queue(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        context = dataclasses.replace(CONTEXT, attempt=3)
        _seed_awaiting(store, context)
        retry_policy = RetryPolicy(max_attempts=3)
        result = monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            context=context,
            retry_policy=retry_policy,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingStates.FAILURE_RETRYABLE
        assert _output_index_exists(s3, bucket, ProcessingStates.FAILURE_RETRYABLE)

        assert _receive_all(sqs, retry_queue_url) == []
        dlq_messages = _receive_all(sqs, dlq_url)
        assert len(dlq_messages) == 1
        body = json.loads(dlq_messages[0]["Body"])
        assert body["attempt"] == 3
        assert body["input_entity_id"] == INPUT_ENTITY_ID


class TestFailureNonretryable:
    def test_writes_output_index_and_sends_to_dlq(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        result = monitor_job(
            detail=make_detail(
                status="FAILED",
                statusReason="Essential container exited",
                container={"exitCode": 1},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingStates.FAILURE_NONRETRYABLE
        assert _output_index_exists(s3, bucket, ProcessingStates.FAILURE_NONRETRYABLE)

        assert _receive_all(sqs, retry_queue_url) == []
        dlq_messages = _receive_all(sqs, dlq_url)
        assert len(dlq_messages) == 1
        body = json.loads(dlq_messages[0]["Body"])
        assert body["input_entity_id"] == INPUT_ENTITY_ID
        assert body["output_entity_id"] == OUTPUT_ENTITY_ID

    def test_no_dlq_url_sends_nothing(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED",
                statusReason="Essential container exited",
                container={"exitCode": 1},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=None,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert _receive_all(sqs, retry_queue_url) == []
        assert _receive_all(sqs, dlq_url) == []


class TestDefaultNow:
    def test_uses_current_time_when_now_not_provided(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        before = datetime.now(timezone.utc)
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
        )
        after = datetime.now(timezone.utc)

        key = S3RecordStore.canonical_key(CONTEXT)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        timestamp = datetime.fromisoformat(record["events"][0]["timestamp"])
        assert before <= timestamp <= after


class TestFullLifecycle:
    """monitor_job now derives old_state itself, so it must handle every
    Batch status, not just terminal SUCCEEDED/FAILED."""

    def test_submitted_then_awaiting_then_success(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        retry_policy = RetryPolicy(max_attempts=3)
        kwargs: dict[str, Any] = dict(
            log_store=store,
            context=CONTEXT,
            retry_policy=retry_policy,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )

        submitted = monitor_job(detail=make_detail(status="SUBMITTED"), **kwargs)
        assert submitted is ProcessingStates.SUBMITTED
        assert (
            s3.list_objects_v2(
                Bucket=bucket,
                Prefix=S3RecordStore.state_pointer_key(
                    ProcessingStates.SUBMITTED, CONTEXT
                ),
            ).get("KeyCount", 0)
            == 1
        )

        runnable = monitor_job(detail=make_detail(status="RUNNABLE"), **kwargs)
        assert runnable is ProcessingStates.AWAITING
        assert (
            s3.list_objects_v2(
                Bucket=bucket,
                Prefix=S3RecordStore.state_pointer_key(
                    ProcessingStates.SUBMITTED, CONTEXT
                ),
            ).get("KeyCount", 0)
            == 0
        )
        assert (
            s3.list_objects_v2(
                Bucket=bucket,
                Prefix=S3RecordStore.state_pointer_key(
                    ProcessingStates.AWAITING, CONTEXT
                ),
            ).get("KeyCount", 0)
            == 1
        )

        success = monitor_job(detail=make_detail(status="SUCCEEDED"), **kwargs)
        assert success is ProcessingStates.SUCCESS
        assert _output_index_exists(s3, bucket, ProcessingStates.SUCCESS)

        key = S3RecordStore.canonical_key(CONTEXT)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert [e["state"] for e in record["events"]] == [
            "SUBMITTED",
            "AWAITING",
            "SUCCESS",
        ]

    def test_awaiting_progression_does_not_rewrite_pointer(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        """PENDING -> RUNNABLE -> STARTING -> RUNNING all map to AWAITING;
        the redundant same-state pointer rewrite should be skipped."""
        kwargs: dict[str, Any] = dict(
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        for status in ["PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
            result = monitor_job(detail=make_detail(status=status), **kwargs)
            assert result is ProcessingStates.AWAITING

        record = store.find_state_pointer(context=CONTEXT)
        assert record is ProcessingStates.AWAITING

    def test_resubmitted_attempt_retires_prior_attempt_pointer(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        """A new attempt's first SUBMITTED event must retire the prior
        (exhausted) attempt's FAILURE_RETRYABLE pointer -- a cross-attempt
        transition, since state pointers are keyed per attempt."""
        old_context = dataclasses.replace(CONTEXT, attempt=3)
        _seed_awaiting(store, old_context)
        retry_policy = RetryPolicy(max_attempts=3)
        monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            context=old_context,
            retry_policy=retry_policy,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        old_key = S3RecordStore.state_pointer_key(
            ProcessingStates.FAILURE_RETRYABLE, old_context
        )
        assert s3.list_objects_v2(Bucket=bucket, Prefix=old_key).get("KeyCount", 0) == 1

        new_context = dataclasses.replace(CONTEXT, attempt=4)
        result = monitor_job(
            detail=make_detail(status="SUBMITTED"),
            log_store=store,
            context=new_context,
            retry_policy=retry_policy,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingStates.SUBMITTED
        assert s3.list_objects_v2(Bucket=bucket, Prefix=old_key).get("KeyCount", 0) == 0
        new_key = S3RecordStore.state_pointer_key(
            ProcessingStates.SUBMITTED, new_context
        )
        assert s3.list_objects_v2(Bucket=bucket, Prefix=new_key).get("KeyCount", 0) == 1


class TestMonotonicityGuard:
    def test_stale_event_does_not_rewrite_pointer(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        """EventBridge does not guarantee delivery order: a same-attempt
        event ranked below the recorded state (e.g. a straggling SUBMITTED
        arriving after SUCCEEDED already landed) must not clobber the
        pointer, though it is still appended to the canonical record."""
        kwargs: dict[str, Any] = dict(
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        monitor_job(detail=make_detail(status="SUCCEEDED"), **kwargs)
        result = monitor_job(detail=make_detail(status="SUBMITTED"), **kwargs)
        assert result is ProcessingStates.SUBMITTED

        assert store.find_state_pointer(context=CONTEXT) is ProcessingStates.SUCCESS

        key = S3RecordStore.canonical_key(CONTEXT)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert [e["state"] for e in record["events"]] == ["SUCCESS", "SUBMITTED"]

    def test_straggler_for_superseded_attempt_does_not_resurrect_pointer(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        """A late event for an attempt a newer attempt has already
        superseded (its own pointer already retired) must not write a new
        pointer or fire SQS side effects for the dead attempt, even though
        find_state_pointer at that attempt now returns None -- the same
        signal a brand-new entity would give."""
        kwargs: dict[str, Any] = dict(
            log_store=store,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        context_1 = dataclasses.replace(CONTEXT, attempt=1)
        context_2 = dataclasses.replace(CONTEXT, attempt=2)

        spot_detail = make_detail(
            status="FAILED", statusReason="Host EC2 instance terminated"
        )
        monitor_job(detail=make_detail(status="SUBMITTED"), context=context_1, **kwargs)
        monitor_job(detail=spot_detail, context=context_1, **kwargs)
        monitor_job(detail=make_detail(status="SUBMITTED"), context=context_2, **kwargs)
        _receive_all(sqs, retry_queue_url)  # drain the attempt-1 retry message

        result = monitor_job(detail=spot_detail, context=context_1, **kwargs)
        assert result is ProcessingStates.FAILURE_RETRYABLE

        assert store.find_state_pointer(context=context_1) is None
        assert store.find_state_pointer(context=context_2) is ProcessingStates.SUBMITTED
        assert _receive_all(sqs, retry_queue_url) == []

        key = S3RecordStore.canonical_key(context_1)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert [e["state"] for e in record["events"]] == [
            "SUBMITTED",
            "FAILURE_RETRYABLE",
            "FAILURE_RETRYABLE",
        ]


class TestExitCodeOutcomeRouting:
    def test_nonretryable_outcome_skips_dlq_when_dlq_false(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY", dlq=False).build()
        _seed_awaiting(store)
        result = monitor_job(
            detail=make_detail(
                status="FAILED",
                container={"exitCode": 4},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            exit_code_outcomes=outcomes,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result.name == "CLOUDY"
        assert result.retryable is False
        assert result.dlq is False
        assert _receive_all(sqs, dlq_url) == []
        assert _receive_all(sqs, retry_queue_url) == []

    def test_nonretryable_outcome_still_sends_dlq_when_dlq_true(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY", dlq=True).build()
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED",
                container={"exitCode": 4},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            exit_code_outcomes=outcomes,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert len(_receive_all(sqs, dlq_url)) == 1

    def test_label_appears_in_canonical_event(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY", dlq=False).build()
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED",
                container={"exitCode": 4},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            exit_code_outcomes=outcomes,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        key = S3RecordStore.canonical_key(CONTEXT)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["events"][-1]["state"] == "CLOUDY"

    def test_custom_state_appears_in_output_index_key(
        self,
        store: S3RecordStore,
        s3: S3Client,
        bucket: str,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY", dlq=False).build()
        _seed_awaiting(store)
        monitor_job(
            detail=make_detail(
                status="FAILED",
                container={"exitCode": 4},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            exit_code_outcomes=outcomes,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        cloudy_key = S3RecordStore.output_index_key(cloudy, CONTEXT)
        assert (
            s3.list_objects_v2(Bucket=bucket, Prefix=cloudy_key).get("KeyCount", 0) == 1
        )
        assert not _output_index_exists(
            s3, bucket, ProcessingStates.FAILURE_NONRETRYABLE
        )

    def test_retryable_outcome_routes_to_retry_queue(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        outcomes = (
            ExitCodeOutcomesBuilder()
            .add(42, "TRANSIENT_TOOL_ERROR", retryable=True, dlq=True)
            .build()
        )
        _seed_awaiting(store)
        result = monitor_job(
            detail=make_detail(
                status="FAILED",
                container={"exitCode": 42},
            ),
            log_store=store,
            context=CONTEXT,
            retry_policy=RetryPolicy(max_attempts=3),
            exit_code_outcomes=outcomes,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result.name == "TRANSIENT_TOOL_ERROR"
        assert result.retryable is True
        assert len(_receive_all(sqs, retry_queue_url)) == 1
