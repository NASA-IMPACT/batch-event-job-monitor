# Resubmitting jobs

`JobResubmitFunction` consumes the retry queue `monitor_job` publishes to and calls AWS Batch `submit_job` for the next
attempt. It bundles a generic default handler covering the common case, and can wrap a consumer-authored handler instead
for anything the default can't express.

## The default: replay what the failed job ran

The default handler resubmits a job with what its failed attempt ran with and needs zero consumer-authored Python. Pass
the same `job_type_configs` mapping given to `JobMonitorFunction`:

```python
from batch_event_job_monitor_cdk import JobResubmitFunction

JobResubmitFunction(
    self,
    "JobResubmit",
    job_type_configs=job_type_configs,  # dict[str, JobTypeConfig], keyed by job_type
    retry_queue=retry_queue,
)
```

The default handler (`batch_event_job_monitor.handlers.job_resubmit_handler`) parses each SQS record as a
`RetryMessage`, looks up that message's `job_type` in the construct's `PROCESSING_JOB_TYPE_CONFIGS` environment
variable, calls `resubmit_job` with `jobQueue`/`jobDefinition` from that job_type's `JobTypeConfig`, and reports
per-record failures via `batchItemFailures` so a single bad message doesn't fail the whole batch. The `batch:SubmitJob`
IAM grant is scoped to the union of every listed job_type's queue/job definition, and `batch:TagResource` covers the
tags replayed onto new jobs.

### What gets replayed

`monitor_job` records two fields on every `RetryMessage`, on both the retry queue and the failure DLQ:

- `job_definition`: the job definition ARN the failed job ran, revision included.
- `submit_params`: `SubmitJob` kwargs built from the failure event's job detail: `containerOverrides` (`command`,
  `environment`), non-`bejm_*` `parameters`, `timeout`, `retryStrategy`, `propagateTags`, `tags`,
  `schedulingPriorityOverride`, and `shareIdentifier`.

The retry runs on the latest active revision of the job definition, so a new revision's image, CPU, memory, and newly
added environment variables take effect. Resources always come from the job definition, so a `resourceRequirements`
override on the original submission does not carry over to the retry. Batch reports a job's resolved container rather
than the overrides it was submitted with, so the failed job's command and environment are pinned: a command or
environment variable changed in a newer revision keeps its old value on retry.

`environment` values travel in the SQS message body. Put sensitive values in the job definition's `secrets`, which are
not recorded.

Only single-container jobs are replayed. `dependsOn` is not, since a retry should not wait on its original dependencies,
and neither are array, multi-node, ECS-properties, or EKS job settings.

Messages published before these fields existed resubmit with no overrides.

## Overriding: custom `containerOverrides`, a computed command, or job introspection

Custom handlers receive the same `RetryMessage` and can use `message.submit_params` directly. Some job types need more:
different container overrides per attempt, a command built from the job's identity fields, or
`batch:DescribeJobs`-driven introspection of the original job. For those, supply your own `entry`/`index` --
`JobResubmitFunction` wires the same SQS event source, IAM permissions, and `PROCESSING_JOB_TYPE_CONFIGS` environment
variable around your handler instead of the bundled default:

```python
JobResubmitFunction(
    self,
    "JobResubmit",
    job_type_configs=job_type_configs,
    retry_queue=retry_queue,
    entry="lambda/job_resubmit",
    index="handler.py",
)
```

A starting point for `lambda/job_resubmit/handler.py`:

```python
from __future__ import annotations

import json
import logging
import os
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


def build_submit_job_params(job_group: JobGroup) -> dict[str, Any]:
    """Return batch_client.submit_job kwargs for the given (new) attempt.

    The bejm_* identity parameters are injected by resubmit_job
    automatically -- do not set them here.
    """
    config = _job_type_config(job_group.job_type)
    return {
        "jobName": job_group.batch_job_name(),
        "jobQueue": config.job_queue_arn,
        "jobDefinition": config.job_definition_arn,
        # e.g. containerOverrides computed from job_group here
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
                job_group=message.job_group,
            )
        except Exception:
            # The body can carry the job's environment, so it stays out of the logs.
            logger.exception("Failed to resubmit SQS message %s", record["messageId"])
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": batch_item_failures}
```

`entry` and `index` must be given together -- `JobResubmitFunction` raises `ValueError` if only one is set. When both
are omitted, the bundled default handler is used.
