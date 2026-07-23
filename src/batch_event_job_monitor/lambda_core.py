"""Reusable orchestration for AWS Batch job monitor Lambdas."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    ExitCodeOutcomes,
    JobContext,
    ProcessingEventRecord,
    ProcessingState,
    ProcessingStates,
    RetryMessage,
    RetryPolicy,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def monitor_job(
    *,
    detail: dict[str, Any],
    log_store: S3RecordStore,
    context: JobContext,
    retry_policy: RetryPolicy,
    retry_queue_url: str | None,
    dlq_url: str | None,
    sqs_client: Any,
    exit_code_outcomes: ExitCodeOutcomes | None = None,
    now: Callable[[], datetime] | None = None,
) -> ProcessingState:
    """Classify and record a Batch job state change event, routing failures.

    Called for every aws.batch job state change event for a monitored job,
    not only terminal ones. Classifies the event's status, appends a
    canonical event, updates the state pointer, writes the output index for
    terminal outcomes, and routes retryable failures to a retry queue or
    terminal non-success outcomes to a dead-letter queue.

    This is the single owner of state tracking: it derives the previous
    state itself (see below) rather than accepting it from the caller, so
    ad hoc/backfill job submissions are tracked correctly as long as they
    set JobContext.to_batch_parameters() on their SubmitJobRequest, with no
    other library coordination required.

    Parameters
    ----------
    detail : dict[str, Any]
        The EventBridge "detail" object for an aws.batch job state change
        event.
    log_store : S3RecordStore
        The log store used to record the canonical event, state pointer,
        and output index.
    context : JobContext
        Identifying and partitioning fields for this job processing event,
        typically decoded from the event via
        `JobDetails.from_event(detail).decode_context()`.
    retry_policy : RetryPolicy
        The retry policy used to classify the outcome and to decide
        terminality.
    retry_queue_url : str or None
        SQS queue URL to notify when the outcome is retryable and attempts
        remain. No message is sent if this is None.
    dlq_url : str or None
        SQS queue URL to notify when the outcome is terminal, not SUCCESS,
        and not opted out of DLQ routing (see ExitCodeOutcome.dlq). No
        message is sent if this is None.
    sqs_client : Any
        A boto3 SQS client used to send retry/DLQ messages.
    exit_code_outcomes : ExitCodeOutcomes or None, optional
        Deploy-time, job_type-specific exit-code taxonomy (see
        JobTypeConfig) checked before the built-in spot-interruption
        classification fallback.
    now : Callable[[], datetime] or None, optional
        Callable returning the current time, used for the recorded event
        timestamp. Defaults to `datetime.now(timezone.utc)`.

    Returns
    -------
    ProcessingState
        The classified processing state for this event.
    """
    current_time = now or _utcnow
    outcomes = exit_code_outcomes or ExitCodeOutcomes()
    states = outcomes.states()

    job = JobDetails.from_event(detail)
    new_state = job.classify(retry_policy, outcomes)

    old_state = log_store.find_state_pointer(context=context, states=states)
    old_attempt = context.attempt

    if old_state is None and new_state == ProcessingStates.SUBMITTED:
        active = log_store.find_active_pointer(
            job_type=context.job_type,
            partition_fields=context.partition_fields,
            input_entity_id=context.input_entity_id,
            states=states,
        )
        if active is not None:
            old_state, old_attempt = active

    event = ProcessingEventRecord(
        state=new_state.name,
        timestamp=current_time().isoformat(),
        batch_job_id=job.job_id,
        exit_code=job.exit_code,
    )

    # Monotonicity guard (same attempt only): EventBridge does not guarantee
    # delivery order, so a same-attempt event ranked below the already
    # recorded state is stale. Still append it for audit history, but skip
    # the pointer/output-index/SQS side effects.
    if (
        old_attempt == context.attempt
        and old_state is not None
        and new_state.rank < old_state.rank
    ):
        log_store.append_canonical_event(
            context=context, event=event, batch_job_id=job.job_id
        )
        return new_state

    log_store.append_canonical_event(
        context=context, event=event, batch_job_id=job.job_id
    )

    # Skip the redundant pointer rewrite when nothing changed (e.g.
    # PENDING -> RUNNABLE -> STARTING -> RUNNING all map to AWAITING).
    if not (old_attempt == context.attempt and new_state == old_state):
        log_store.write_state_pointer(
            context=context,
            new_state=new_state,
            old_state=old_state,
            old_attempt=old_attempt,
        )

    terminal = new_state.is_terminal(context.attempt, retry_policy)

    if terminal:
        log_store.write_output_index(context=context, state=new_state)

    message_body = RetryMessage.from_context(
        context, batch_job_id=job.job_id, state=new_state.name
    ).to_json()

    if new_state.retryable and not terminal:
        if retry_queue_url is not None:
            sqs_client.send_message(QueueUrl=retry_queue_url, MessageBody=message_body)
    elif terminal and new_state != ProcessingStates.SUCCESS and new_state.dlq:
        if dlq_url is not None:
            sqs_client.send_message(QueueUrl=dlq_url, MessageBody=message_body)

    return new_state
