"""Bundled Lambda handler for JobMonitorFunction.

Fully generic -- no consumer-authored Python is required for the common
case. Every input comes from the EventBridge event itself (via
JobDetails.decode_context, decoding the bejm_* Batch parameters) or
environment variables the JobMonitorFunction CDK construct sets.
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

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import EventBridgeEvent

_DEFAULT_JOB_TYPE_CONFIG_KEY = "__default__"

_sqs_client = boto3.client("sqs")


def _job_type_config(job_type: str) -> JobTypeConfig:
    all_configs = json.loads(os.environ["PROCESSING_JOB_TYPE_CONFIGS"])
    raw = all_configs.get(job_type, all_configs[_DEFAULT_JOB_TYPE_CONFIG_KEY])
    return JobTypeConfig.from_dict(raw)


def handler(event: EventBridgeEvent, context: Context) -> dict[str, str]:
    """Classify and record a single aws.batch job state change event."""
    detail = event["detail"]
    job = JobDetails.from_event(detail)
    job_context = job.decode_context()

    config = _job_type_config(job_context.job_type)
    log_store = S3RecordStore(bucket=os.environ["PROCESSING_BUCKET_NAME"])

    new_state = monitor_job(
        detail=detail,
        log_store=log_store,
        context=job_context,
        retry_policy=config.retry_policy,
        exit_code_outcomes=config.exit_code_outcomes,
        retry_queue_url=os.environ.get("JOB_RETRY_QUEUE_URL"),
        dlq_url=os.environ.get("JOB_FAILURE_DLQ_URL"),
        sqs_client=_sqs_client,
    )
    return {"state": new_state.name}
