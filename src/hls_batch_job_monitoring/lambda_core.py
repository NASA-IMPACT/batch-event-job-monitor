"""Reusable orchestration for AWS Batch job monitor Lambdas."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from hls_batch_job_monitoring.job_details import JobDetails
from hls_batch_job_monitoring.log_store import S3RecordStore
from hls_batch_job_monitoring.models import (
    ProcessingEventRecord,
    ProcessingState,
    RetryPolicy,
    is_terminal,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def monitor_job(
    *,
    detail: dict[str, Any],
    log_store: S3RecordStore,
    job_type: str,
    partition_fields: dict[str, str],
    entity_id: str,
    output_entity_id: str,
    attempt: int,
    old_state: ProcessingState | None,
    retry_policy: RetryPolicy,
    retry_queue_url: str | None,
    dlq_url: str | None,
    sqs_client: Any,
    now: Callable[[], datetime] | None = None,
) -> ProcessingState:
    """Classify and record a Batch job state change event, routing failures.

    Classifies the job outcome from the event detail, appends a canonical
    event, updates the state pointer, writes the output index for terminal
    outcomes, and routes retryable failures to a retry queue or terminal
    non-success outcomes to a dead-letter queue.

    Parameters
    ----------
    detail : dict[str, Any]
        The EventBridge "detail" object for an aws.batch job state change
        event.
    log_store : S3RecordStore
        The log store used to record the canonical event, state pointer,
        and output index.
    job_type : str
        The job type.
    partition_fields : dict[str, str]
        Ordered partition key/value pairs.
    entity_id : str
        The processed entity identifier.
    output_entity_id : str
        The output entity identifier.
    attempt : int
        The current attempt number.
    old_state : ProcessingState or None
        The previous state whose pointer should be removed, if any.
    retry_policy : RetryPolicy
        The retry policy used to classify the outcome and to decide
        terminality.
    retry_queue_url : str or None
        SQS queue URL to notify when the outcome is retryable and attempts
        remain. No message is sent if this is None.
    dlq_url : str or None
        SQS queue URL to notify when the outcome is terminal and not
        SUCCESS. No message is sent if this is None.
    sqs_client : Any
        A boto3 SQS client used to send retry/DLQ messages.
    now : Callable[[], datetime] or None, optional
        Callable returning the current time, used for the recorded event
        timestamp. Defaults to `datetime.now(timezone.utc)`.

    Returns
    -------
    ProcessingState
        The classified processing state for this event.
    """
    current_time = now or _utcnow

    job = JobDetails.from_event(detail)
    new_state = job.classify(retry_policy)

    log_store.append_canonical_event(
        entity_id=entity_id,
        output_entity_id=output_entity_id,
        job_type=job_type,
        partition_fields=partition_fields,
        attempt=attempt,
        event=ProcessingEventRecord(
            state=new_state.value,
            timestamp=current_time().isoformat(),
            batch_job_id=job.job_id,
            exit_code=job.exit_code,
        ),
        batch_job_id=job.job_id,
    )

    log_store.write_state_pointer(
        job_type=job_type,
        partition_fields=partition_fields,
        entity_id=entity_id,
        attempt=attempt,
        new_state=new_state,
        old_state=old_state,
        output_entity_id=output_entity_id,
    )

    terminal = is_terminal(new_state, attempt, retry_policy)

    if terminal:
        log_store.write_output_index(
            job_type=job_type,
            partition_fields=partition_fields,
            output_entity_id=output_entity_id,
            state=new_state,
        )

    message_body = json.dumps(
        {
            "job_type": job_type,
            "partition_fields": partition_fields,
            "entity_id": entity_id,
            "output_entity_id": output_entity_id,
            "attempt": attempt,
            "batch_job_id": job.job_id,
        }
    )

    if new_state is ProcessingState.FAILURE_RETRYABLE and not terminal:
        if retry_queue_url is not None:
            sqs_client.send_message(
                QueueUrl=retry_queue_url, MessageBody=message_body
            )
    elif terminal and new_state is not ProcessingState.SUCCESS:
        if dlq_url is not None:
            sqs_client.send_message(QueueUrl=dlq_url, MessageBody=message_body)

    return new_state
