"""Reusable orchestration for AWS Batch job monitor Lambdas."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    ExitCodeOutcomes,
    JobGroup,
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
    job_group: JobGroup,
    retry_policy: RetryPolicy,
    retry_queue_url: str | None,
    dlq_url: str | None,
    sqs_client: Any,
    exit_code_outcomes: ExitCodeOutcomes | None = None,
    now: Callable[[], datetime] | None = None,
) -> ProcessingState:
    """Classify and record a Batch job state change event, routing failures.

    Called for every aws.batch job state change event for a monitored job,
    not only terminal ones. Classifies the event once for the whole
    job_group (one Batch job, one exit code), then appends a canonical
    event and updates the state pointer for each of the group's entities
    individually (see JobGroup.contexts()) -- a twin-granule or
    many-source-to-one-output job still gets one classification/one retry
    decision, but a canonical record and state pointer per entity. Writes
    the output index for terminal outcomes (once per group -- its key
    doesn't vary by entity), and routes retryable failures to a retry
    queue or terminal non-success outcomes to a dead-letter queue.

    This is the single owner of state tracking: it derives each entity's
    previous state itself (see below) rather than accepting it from the
    caller, so ad hoc/backfill job submissions are tracked correctly as
    long as they set JobGroup.to_batch_parameters() on their
    SubmitJobRequest, with no other library coordination required.

    Parameters
    ----------
    detail : dict[str, Any]
        The EventBridge "detail" object for an aws.batch job state change
        event.
    log_store : S3RecordStore
        The log store used to record the canonical event, state pointer,
        and output index.
    job_group : JobGroup
        Identifying and partitioning fields for this job processing event,
        typically decoded from the event via
        `JobDetails.from_event(detail).decode_job_group()`.
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

    contexts = job_group.contexts()
    any_fresh = False

    for context in contexts:
        old_state = log_store.find_state_pointer(context=context, states=states)
        old_attempt = context.attempt
        stale = False

        if old_state is None:
            active = log_store.find_active_pointer(
                job_type=context.job_type,
                partition_fields=context.partition_fields,
                input_entity_id=context.input_entity_id,
                states=states,
            )
            if active is not None:
                active_state, active_attempt = active
                if active_attempt > context.attempt:
                    # A straggler for an attempt a newer one has already
                    # superseded (that attempt's own pointer has since
                    # been retired, so find_state_pointer above found
                    # nothing).
                    stale = True
                else:
                    old_state, old_attempt = active_state, active_attempt

        event = ProcessingEventRecord(
            state=new_state.name,
            timestamp=current_time().isoformat(),
            batch_job_id=job.job_id,
            exit_code=job.exit_code,
        )
        log_store.append_canonical_event(
            context=context, event=event, batch_job_id=job.job_id
        )

        # Monotonicity guard: EventBridge does not guarantee delivery
        # order, so a stale/out-of-order event is possible two ways -- a
        # straggler for an already-superseded attempt (checked above), or
        # a same-attempt event ranked below the state already recorded
        # for this attempt. Either way, the canonical event above still
        # records it for audit history, but this entity's
        # pointer/routing side effects are skipped.
        if stale or (
            old_attempt == context.attempt
            and old_state is not None
            and new_state.rank < old_state.rank
        ):
            continue

        any_fresh = True

        # Skip the redundant pointer rewrite when nothing changed (e.g.
        # PENDING -> RUNNABLE -> STARTING -> RUNNING all map to AWAITING).
        if not (old_attempt == context.attempt and new_state == old_state):
            log_store.write_state_pointer(
                context=context,
                new_state=new_state,
                old_state=old_state,
                old_attempt=old_attempt,
            )

    # If every entity's event was stale, the whole group's event is stale
    # too -- skip the group-level output-index write and SQS routing.
    if not any_fresh:
        return new_state

    terminal = new_state.is_terminal(job_group.attempt, retry_policy)

    if terminal:
        # output_index_key only depends on job_type/partition_fields/
        # output_entity_id -- entity-invariant within a group -- so write
        # it once, not once per entity.
        log_store.write_output_index(context=contexts[0], state=new_state)

    message_body = RetryMessage.from_job_group(
        job_group, batch_job_id=job.job_id, state=new_state.name
    ).to_json()

    if new_state.retryable and not terminal:
        if retry_queue_url is not None:
            sqs_client.send_message(QueueUrl=retry_queue_url, MessageBody=message_body)
    elif terminal and new_state != ProcessingStates.SUCCESS and new_state.dlq:
        if dlq_url is not None:
            sqs_client.send_message(QueueUrl=dlq_url, MessageBody=message_body)

    return new_state
