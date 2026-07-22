"""Helpers for submitting AWS Batch jobs with monitoring parameters set.

Not Lambda-specific: submit_job/resubmit_job have no coupling to a Lambda
runtime and may be called from anywhere a boto3 Batch client is available
(a Lambda, a CLI backfill script, a Step Function task, etc).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from batch_event_job_monitor.models import JobContext

if TYPE_CHECKING:
    from mypy_boto3_batch import BatchClient


def submit_job(
    *,
    batch_client: BatchClient,
    build_submit_job_params: Callable[[JobContext], dict[str, Any]],
    context: JobContext,
) -> str:
    """Submit a job to AWS Batch with monitoring parameters set.

    For the original (non-retry) submission path, or ad hoc/backfill
    submissions: calls the consumer-supplied build_submit_job_params(context)
    for the base SubmitJobRequest kwargs (job queue, job definition,
    container overrides, etc -- this is inherently job-type specific and
    stays the consumer's responsibility), injects the bejm_* identity
    parameters, and calls batch_client.submit_job.

    Unlike resubmit_job, this does not bump context.attempt -- callers
    determine the attempt themselves: `JobContext.new(...)` for a
    brand-new entity (attempt 1), or `S3RecordStore.next_attempt(...)` if
    resubmitting an already-tracked one outside the retry-queue flow (e.g.
    a manual resubmission via the Batch console/CLI).

    Writes nothing to S3 -- the job-monitor Lambda derives all tracking
    state purely from watching this submission's own SUBMITTED event on
    the Batch event stream.

    Parameters
    ----------
    batch_client : BatchClient
        A boto3 Batch client.
    build_submit_job_params : Callable[[JobContext], dict[str, Any]]
        Consumer-supplied callback returning the kwargs for
        `batch_client.submit_job(**params)`, given the JobContext.
    context : JobContext
        Identifying and partitioning fields for the job being submitted.

    Returns
    -------
    str
        The new AWS Batch job id.
    """
    params = build_submit_job_params(context)
    params = {
        **params,
        "parameters": {
            **params.get("parameters", {}),
            **context.to_batch_parameters(),
        },
    }
    resp = batch_client.submit_job(**params)
    return resp["jobId"]


def resubmit_job(
    *,
    batch_client: BatchClient,
    build_submit_job_params: Callable[[JobContext], dict[str, Any]],
    context: JobContext,
) -> str:
    """Resubmit context's entity for its next attempt.

    Advances context via JobContext.next_attempt() and delegates to
    submit_job.

    Writes nothing to S3 -- the job-monitor Lambda derives all tracking
    state, including old_state, purely from watching this submission's own
    SUBMITTED event on the Batch event stream.

    Parameters
    ----------
    batch_client : BatchClient
        A boto3 Batch client.
    build_submit_job_params : Callable[[JobContext], dict[str, Any]]
        Consumer-supplied callback returning the kwargs for
        `batch_client.submit_job(**params)`, given the new attempt's
        JobContext.
    context : JobContext
        The just-exhausted attempt's context (context.attempt is the OLD
        attempt number; the new attempt is context.attempt + 1).

    Returns
    -------
    str
        The new AWS Batch job id.
    """
    new_context = context.next_attempt()
    return submit_job(
        batch_client=batch_client,
        build_submit_job_params=build_submit_job_params,
        context=new_context,
    )
