"""Bundled default Lambda handler for JobResubmitFunction.

Generic resubmission: reuses the same Batch job queue and job definition
every attempt, with no custom containerOverrides/command. Covers the
common case with zero consumer-authored Python, the same way
job_monitor_handler.py does for JobMonitorFunction.

For anything more complex (per-attempt container overrides, a computed
command, batch:DescribeJobs-driven introspection of the original job),
supply your own entry/index to JobResubmitFunction instead of using this
default -- see docs/resubmitting-jobs.md.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import boto3

from batch_event_job_monitor import JobContext, RetryMessage, resubmit_job

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

_batch_client = boto3.client("batch")


def _build_submit_job_params(context: JobContext) -> dict[str, Any]:
    return {
        "jobName": f"{context.job_type}-{context.input_entity_id}-{context.attempt}",
        "jobQueue": os.environ["BATCH_JOB_QUEUE_ARN"],
        "jobDefinition": os.environ["BATCH_JOB_DEFINITION_ARN"],
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
