"""Bundled Lambda handler for JobMonitorFunction.

Fully generic -- no consumer-authored Python is required for the common
case. Every input comes from the EventBridge event itself (via
JobDetails.decode_job_group, decoding the bejm_* Batch parameters) or
environment variables the JobMonitorFunction CDK construct sets.

Handles both of JobMonitorFunction's rule paths: a tracked job's state
change, and a job that ran on a monitored queue without the bejm_*
parameters (see batch_event_job_monitor.untracked).

For jobs submitted by a system that cannot set the bejm_* parameters, build
a handler with make_handler(resolve_untracked=...) in a module of your own
and point JobMonitorFunction's entry/index at it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import TYPE_CHECKING

import boto3

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.lambda_core import monitor_job, parse_event_time
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import JobGroup, JobTypeConfig
from batch_event_job_monitor.untracked import (
    DEFAULT_METRIC_NAMESPACE,
    record_untracked_job,
)

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import EventBridgeEvent

    Handler = Callable[[EventBridgeEvent, Context], dict[str, str]]

_sqs_client = boto3.client("sqs")

# Reported for an untracked job instead of a ProcessingState name. Not a
# ProcessingState: an untracked job has no tracked state to be in.
UNTRACKED = "UNTRACKED"


UntrackedJobResolver = Callable[[JobDetails, S3RecordStore], JobGroup | None]
"""Infers the JobGroup of a job submitted without the bejm_* parameters.

Returns None for a job it does not recognize, which is then recorded as
untracked. The log store is given so a resolver can look up state it cannot
read from the job itself -- e.g. S3RecordStore.attempt_for_batch_job, for a
submitter that does not record attempts.
"""


def load_job_type_configs() -> dict[str, JobTypeConfig]:
    """Every monitored job_type's JobTypeConfig, set by JobMonitorFunction."""
    return {
        job_type: JobTypeConfig.from_dict(config)
        for job_type, config in json.loads(
            os.environ["PROCESSING_JOB_TYPE_CONFIGS"]
        ).items()
    }


def _log_store() -> S3RecordStore:
    return S3RecordStore(
        bucket=os.environ["PROCESSING_BUCKET_NAME"],
        key_prefix=os.environ.get("PROCESSING_KEY_PREFIX", ""),
    )


def _record_untracked(job: JobDetails) -> dict[str, str]:
    record_untracked_job(
        job,
        namespace=os.environ.get("MONITOR_METRIC_NAMESPACE", DEFAULT_METRIC_NAMESPACE),
    )
    return {"state": UNTRACKED}


def make_handler(resolve_untracked: UntrackedJobResolver | None = None) -> Handler:
    """Build a job-monitor Lambda handler.

    Parameters
    ----------
    resolve_untracked : UntrackedJobResolver or None, optional
        Infers the JobGroup of a job carrying no bejm_* parameters, for
        job types whose JobTypeConfig sets requires_bejm_parameters=False.
        Only consulted for such jobs: a job carrying the parameters is
        always decoded from them. Without one, every such job is recorded
        as untracked.

    Returns
    -------
    Handler
        The Lambda handler. Wrap it in a module of your own and point
        JobMonitorFunction's entry/index at it to use a resolver.
    """

    def handler(event: EventBridgeEvent, context: Context) -> dict[str, str]:
        """Classify and record a single aws.batch job state change event."""
        detail = event["detail"]
        job = JobDetails.from_event(detail)

        if job.is_tracked:
            job_group = job.decode_job_group()
            log_store = _log_store()
        elif resolve_untracked is None:
            return _record_untracked(job)
        else:
            log_store = _log_store()
            resolved = resolve_untracked(job, log_store)
            if resolved is None:
                return _record_untracked(job)
            job_group = resolved

        configs = load_job_type_configs()
        if job_group.job_type not in configs:
            raise ValueError(
                f"Batch job {job.job_id!r} resolved to job_type "
                f"{job_group.job_type!r}, which has no JobTypeConfig"
            )
        config = configs[job_group.job_type]
        if not job.is_tracked and config.requires_bejm_parameters:
            raise ValueError(
                f"Batch job {job.job_id!r} has no bejm_* parameters but resolved "
                f"to job_type {job_group.job_type!r}, whose JobTypeConfig "
                "requires them"
            )

        new_state = monitor_job(
            detail=detail,
            event_time=parse_event_time(event["time"]),
            log_store=log_store,
            job_group=job_group,
            retry_policy=config.retry_policy,
            exit_code_outcomes=config.exit_code_outcomes,
            retry_queue_url=(
                os.environ.get("JOB_RETRY_QUEUE_URL") if config.route_failures else None
            ),
            dlq_url=(
                os.environ.get("JOB_FAILURE_DLQ_URL") if config.route_failures else None
            ),
            sqs_client=_sqs_client,
        )
        return {"state": new_state.name}

    return handler


handler = make_handler()
