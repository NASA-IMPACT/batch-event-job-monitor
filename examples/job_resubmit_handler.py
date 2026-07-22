"""Example Lambda handler for JobResubmitFunction.

This file ships as documentation only -- it is not part of the
batch-event-job-monitor package and is not importable. Copy it into your
own repo, point JobResubmitFunction's `entry`/`index` at it, and fill in
build_submit_job_params for your job type's AWS Batch job definition,
queue, and container overrides.
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


def build_submit_job_params(context: JobContext) -> dict[str, Any]:
    """Return batch_client.submit_job kwargs for the given (new) attempt.

    Fill this in for your job type: job queue, job definition, container
    overrides/command, etc. The bejm_* identity parameters are injected by
    resubmit_job automatically -- do not set them here.
    """
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
                build_submit_job_params=build_submit_job_params,
                context=message.context,
            )
        except Exception:
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": batch_item_failures}
