# Resubmitting jobs

`JobResubmitFunction` consumes the retry queue `monitor_job` publishes to and calls AWS Batch `submit_job` for the next
attempt. It bundles a generic default handler covering the common case, and can wrap a consumer-authored handler instead
for anything the default can't express.

## The default: each job_type's own job queue and job definition every attempt

If resubmission always reuses a job_type's own Batch job queue and job definition -- no per-attempt
`containerOverrides`, no computed command -- the default handler needs zero consumer-authored Python. Pass the same
`job_type_configs` mapping given to `JobMonitorFunction`:

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
IAM grant is scoped to the union of every listed job_type's queue/job definition.

## Overriding: custom `containerOverrides`, a computed command, or job introspection

Some job types need more: different container overrides per attempt, a command built from the job's identity fields, or
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


def build_submit_job_params(context: JobContext) -> dict[str, Any]:
    """Return batch_client.submit_job kwargs for the given (new) attempt.

    The bejm_* identity parameters are injected by resubmit_job
    automatically -- do not set them here.
    """
    config = _job_type_config(context.job_type)
    return {
        "jobName": context.batch_job_name(),
        "jobQueue": config.job_queue_arn,
        "jobDefinition": config.job_definition_arn,
        # e.g. containerOverrides computed from context here
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
```

`entry` and `index` must be given together -- `JobResubmitFunction` raises `ValueError` if only one is set. When both
are omitted, the bundled default handler is used.
