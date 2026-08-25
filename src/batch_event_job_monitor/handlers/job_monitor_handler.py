"""Bundled Lambda handler for JobMonitorFunction.

Fully generic -- no consumer-authored Python is required for the common
case. Every input comes from the EventBridge event itself (via
JobDetails.decode_job_group, decoding the bejm_* Batch parameters) or
environment variables the JobMonitorFunction CDK construct sets.

Handles both of JobMonitorFunction's rule paths: a tracked job's state
change, and a job that ran on a monitored queue without the bejm_*
parameters (see batch_event_job_monitor.untracked).
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import boto3

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.lambda_core import monitor_job
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import JobTypeConfig
from batch_event_job_monitor.untracked import (
    DEFAULT_METRIC_NAMESPACE,
    record_untracked_job,
)

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import EventBridgeEvent

_sqs_client = boto3.client("sqs")


def _job_type_config(job_type: str) -> JobTypeConfig:
    all_configs = json.loads(os.environ["PROCESSING_JOB_TYPE_CONFIGS"])
    return JobTypeConfig.from_dict(all_configs[job_type])


# Reported for an untracked job instead of a ProcessingState name. Not a
# ProcessingState: an untracked job has no tracked state to be in.
UNTRACKED = "UNTRACKED"


def handler(event: EventBridgeEvent, context: Context) -> dict[str, str]:
    """Classify and record a single aws.batch job state change event."""
    detail = event["detail"]
    job = JobDetails.from_event(detail)

    if not job.is_tracked:
        record_untracked_job(
            job,
            namespace=os.environ.get(
                "MONITOR_METRIC_NAMESPACE", DEFAULT_METRIC_NAMESPACE
            ),
        )
        return {"state": UNTRACKED}

    job_group = job.decode_job_group()

    config = _job_type_config(job_group.job_type)
    log_store = S3RecordStore(bucket=os.environ["PROCESSING_BUCKET_NAME"])

    new_state = monitor_job(
        detail=detail,
        log_store=log_store,
        job_group=job_group,
        retry_policy=config.retry_policy,
        exit_code_outcomes=config.exit_code_outcomes,
        retry_queue_url=os.environ.get("JOB_RETRY_QUEUE_URL"),
        dlq_url=os.environ.get("JOB_FAILURE_DLQ_URL"),
        sqs_client=_sqs_client,
    )
    return {"state": new_state.name}
