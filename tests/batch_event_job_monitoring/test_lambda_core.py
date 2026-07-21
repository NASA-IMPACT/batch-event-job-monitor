"""Tests for monitor_job (reusable job-monitor orchestration)."""

import json
from datetime import datetime, timezone
from typing import Any

import pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

from batch_event_job_monitoring.lambda_core import monitor_job
from batch_event_job_monitoring.log_store import S3RecordStore
from batch_event_job_monitoring.models import ProcessingState, RetryPolicy

JOB_TYPE = "monthly-composite"
ENTITY_ID = "12TVK_2024-06_source"
OUTPUT_ENTITY_ID = "12TVK_2024-06_output"
PARTITION_FIELDS = {"tile_id": "12TVK", "year_month": "2024-06"}
FIXED_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


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


def _receive_all(sqs: SQSClient, queue_url: str) -> list[MessageTypeDef]:
    resp = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
    )
    return resp.get("Messages", [])


def _output_index_exists(
    s3: S3Client, bucket: str, state: ProcessingState
) -> bool:
    key = S3RecordStore.output_index_key(
        state, JOB_TYPE, PARTITION_FIELDS, OUTPUT_ENTITY_ID
    )
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
        result = monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingState.SUCCESS

    def test_writes_output_index(
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
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert _output_index_exists(s3, bucket, ProcessingState.SUCCESS)

    def test_no_sqs_messages_sent(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
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
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=None,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        key = S3RecordStore.canonical_key(JOB_TYPE, PARTITION_FIELDS, ENTITY_ID, 0)
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
        monitor_job(
            detail=make_detail(status="SUCCEEDED"),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        success_key = S3RecordStore.state_pointer_key(
            ProcessingState.SUCCESS, JOB_TYPE, PARTITION_FIELDS, ENTITY_ID, 0
        )
        assert s3.list_objects_v2(
            Bucket=bucket, Prefix=success_key
        ).get("KeyCount", 0) == 1

        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, JOB_TYPE, PARTITION_FIELDS, ENTITY_ID, 0
        )
        assert s3.list_objects_v2(
            Bucket=bucket, Prefix=awaiting_key
        ).get("KeyCount", 0) == 0


class TestFailureRetryableWithAttemptsRemaining:
    def test_sends_to_retry_queue(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        result = monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingState.FAILURE_RETRYABLE

        retry_messages = _receive_all(sqs, retry_queue_url)
        assert len(retry_messages) == 1
        body = json.loads(retry_messages[0]["Body"])
        assert body["job_type"] == JOB_TYPE
        assert body["partition_fields"] == PARTITION_FIELDS
        assert body["entity_id"] == ENTITY_ID
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
        monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert not _output_index_exists(
            s3, bucket, ProcessingState.FAILURE_RETRYABLE
        )

    def test_no_retry_queue_url_sends_nothing(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
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
        retry_policy = RetryPolicy(max_attempts=3)
        result = monitor_job(
            detail=make_detail(
                status="FAILED", statusReason="Host EC2 instance terminated"
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=3,
            old_state=ProcessingState.AWAITING,
            retry_policy=retry_policy,
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingState.FAILURE_RETRYABLE
        assert _output_index_exists(
            s3, bucket, ProcessingState.FAILURE_RETRYABLE
        )

        assert _receive_all(sqs, retry_queue_url) == []
        dlq_messages = _receive_all(sqs, dlq_url)
        assert len(dlq_messages) == 1
        body = json.loads(dlq_messages[0]["Body"])
        assert body["attempt"] == 3
        assert body["entity_id"] == ENTITY_ID


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
        result = monitor_job(
            detail=make_detail(
                status="FAILED",
                statusReason="Essential container exited",
                container={"exitCode": 1},
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
            now=_fixed_now,
        )
        assert result is ProcessingState.FAILURE_NONRETRYABLE
        assert _output_index_exists(
            s3, bucket, ProcessingState.FAILURE_NONRETRYABLE
        )

        assert _receive_all(sqs, retry_queue_url) == []
        dlq_messages = _receive_all(sqs, dlq_url)
        assert len(dlq_messages) == 1
        body = json.loads(dlq_messages[0]["Body"])
        assert body["entity_id"] == ENTITY_ID
        assert body["output_entity_id"] == OUTPUT_ENTITY_ID

    def test_no_dlq_url_sends_nothing(
        self,
        store: S3RecordStore,
        sqs: SQSClient,
        retry_queue_url: str,
        dlq_url: str,
    ) -> None:
        monitor_job(
            detail=make_detail(
                status="FAILED",
                statusReason="Essential container exited",
                container={"exitCode": 1},
            ),
            log_store=store,
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=ProcessingState.AWAITING,
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
            job_type=JOB_TYPE,
            partition_fields=PARTITION_FIELDS,
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            attempt=0,
            old_state=None,
            retry_policy=RetryPolicy(max_attempts=3),
            retry_queue_url=retry_queue_url,
            dlq_url=dlq_url,
            sqs_client=sqs,
        )
        after = datetime.now(timezone.utc)

        key = S3RecordStore.canonical_key(JOB_TYPE, PARTITION_FIELDS, ENTITY_ID, 0)
        resp = s3.get_object(Bucket=bucket, Key=key)
        record = json.loads(resp["Body"].read())
        timestamp = datetime.fromisoformat(record["events"][0]["timestamp"])
        assert before <= timestamp <= after
