"""Bundled default Lambda handler for JobResubmitFunction.

Generic resubmission: replays what the failed job ran with (its command,
environment, and other submit options, as recorded on the RetryMessage) on
each job_type's own Batch job queue and the latest revision of its job
definition. Covers the common case with zero consumer-authored Python, the
same way job_monitor_handler.py does for JobMonitorFunction.

For anything more complex (per-attempt container overrides, a computed
command, batch:DescribeJobs-driven introspection of the original job),
supply your own entry/index to JobResubmitFunction instead of using this
default -- see docs/resubmitting-jobs.md.
"""

from __future__ import annotations

import json
import logging
import os
from functools import partial
from typing import TYPE_CHECKING, Any

import boto3

from batch_event_job_monitor import JobGroup, RetryMessage, resubmit_job
from batch_event_job_monitor.models import JobTypeConfig

if TYPE_CHECKING:
    from aws_lambda_typing.context import Context
    from aws_lambda_typing.events import SQSEvent

logger = logging.getLogger(__name__)

_batch_client = boto3.client("batch")


def _job_type_config(job_type: str) -> JobTypeConfig:
    all_configs = json.loads(os.environ["PROCESSING_JOB_TYPE_CONFIGS"])
    return JobTypeConfig.from_dict(all_configs[job_type])


def _build_submit_job_params(
    job_group: JobGroup, submit_params: dict[str, Any] | None
) -> dict[str, Any]:
    config = _job_type_config(job_group.job_type)
    return {
        **(submit_params or {}),
        "jobName": job_group.batch_job_name(),
        "jobQueue": config.job_queue_arn,
        "jobDefinition": config.job_definition_arn,
    }


def _failure_detail(body: str, message: RetryMessage | None, error: Exception) -> str:
    """Identify a failed record without logging the job environment it carries."""
    if message is not None:
        return message.batch_job_id
    if isinstance(error, json.JSONDecodeError):
        return body
    return "body withheld: not a RetryMessage"


def handler(event: SQSEvent, context: Context) -> dict[str, list[dict[str, str]]]:
    """Resubmit each failed job on the retry queue, reporting partial failures."""
    batch_item_failures: list[dict[str, str]] = []

    for record in event["Records"]:
        message: RetryMessage | None = None
        try:
            message = RetryMessage.from_json(record["body"])
            resubmit_job(
                batch_client=_batch_client,
                build_submit_job_params=partial(
                    _build_submit_job_params, submit_params=message.submit_params
                ),
                job_group=message.job_group,
            )
        except Exception as error:
            # Reported failures still count as a successful invocation, so this
            # log is the only record of why a message is being retried.
            logger.exception(
                "Failed to resubmit SQS message %s: %s",
                record["messageId"],
                _failure_detail(record["body"], message, error),
            )
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": batch_item_failures}
