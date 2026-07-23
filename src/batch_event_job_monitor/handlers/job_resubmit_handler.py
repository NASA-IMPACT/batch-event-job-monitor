"""Bundled default Lambda handler for JobResubmitFunction.

Generic resubmission: reuses each job_type's own Batch job queue and job
definition every attempt, with no custom containerOverrides/command.
Covers the common case with zero consumer-authored Python, the same way
job_monitor_handler.py does for JobMonitorFunction.

For anything more complex (per-attempt container overrides, a computed
command, batch:DescribeJobs-driven introspection of the original job),
supply your own entry/index to JobResubmitFunction instead of using this
default -- see docs/resubmitting-jobs.md.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

import boto3

from batch_event_job_monitor import JobContext, RetryMessage, resubmit_job
from batch_event_job_monitor.models import JobTypeConfig

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

_batch_client = boto3.client("batch")


def _job_type_config(job_type: str) -> JobTypeConfig:
    all_configs = json.loads(os.environ["PROCESSING_JOB_TYPE_CONFIGS"])
    return JobTypeConfig.from_dict(all_configs[job_type])


def _build_submit_job_params(context: JobContext) -> dict[str, Any]:
    config = _job_type_config(context.job_type)
    return {
        "jobName": context.batch_job_name(),
        "jobQueue": config.job_queue_arn,
        "jobDefinition": config.job_definition_arn,
    }


def handler(event: SQSEvent, context: Context) -> dict[str, list[dict[str, str]]]:
    """Resubmit each failed job on the retry queue, reporting partial failures."""
    batch_item_failures: list[dict[str, str]] = []

    for record in event["Records"]:
        try:
            message = RetryMessage.from_json(record["body"])
            resubmit_job(
                batch_client=_batch_client,
                build_submit_job_params=_build_submit_job_params,
                context=message.context,
            )
        except Exception:
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": batch_item_failures}
