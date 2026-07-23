# Resubmitting jobs

`JobResubmitFunction` consumes the retry queue `monitor_job` publishes to
and calls AWS Batch `submit_job` for the next attempt. It bundles a
generic default handler covering the common case, and can wrap a
consumer-authored handler instead for anything the default can't express.

## The default: same job queue and job definition every attempt

If resubmission always reuses the same Batch job queue and job definition
-- no per-attempt `containerOverrides`, no computed command -- the default
handler needs zero consumer-authored Python:

```python
from aws_cdk import aws_batch as batch
from batch_event_job_monitor_cdk import JobResubmitFunction

JobResubmitFunction(
    self,
    "JobResubmit",
    job_queue=job_queue,            # batch.IJobQueue
    job_definition=job_definition,  # batch.IJobDefinition
    retry_queue=retry_queue,
)
```

The default handler (`batch_event_job_monitor.handlers.job_resubmit_handler`)
parses each SQS record as a `RetryMessage`, calls `resubmit_job` with
`jobQueue`/`jobDefinition` taken from the construct's `BATCH_JOB_QUEUE_ARN`/
`BATCH_JOB_DEFINITION_ARN` environment variables, and reports per-record
failures via `batchItemFailures` so a single bad message doesn't fail the
whole batch.

## Overriding: custom `containerOverrides`, a computed command, or job introspection

Some job types need more: different container overrides per attempt, a
command built from the job's identity fields, or `batch:DescribeJobs`-driven
introspection of the original job. For those, supply your own `entry`/`index`
-- `JobResubmitFunction` wires the same SQS event source and IAM permissions
around your handler instead of the bundled default:

```python
JobResubmitFunction(
    self,
    "JobResubmit",
    job_queue=job_queue,
    job_definition=job_definition,
    retry_queue=retry_queue,
    entry="lambda/job_resubmit",
    index="handler.py",
)
```

A starting point for `lambda/job_resubmit/handler.py`:

```python
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

    The bejm_* identity parameters are injected by resubmit_job
    automatically -- do not set them here.
    """
    return {
        "jobName": f"{context.job_type}-{context.input_entity_id}-{context.attempt}",
        "jobQueue": os.environ["BATCH_JOB_QUEUE_ARN"],
        "jobDefinition": os.environ["BATCH_JOB_DEFINITION_ARN"],
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

`entry` and `index` must be given together -- `JobResubmitFunction` raises
`ValueError` if only one is set. When both are omitted, the bundled default
handler is used.
