# batch-event-job-monitor

Reusable AWS Batch job-monitoring components:

- an S3-backed job log store
- a Lambda-based job monitor core, tracking a job's full lifecycle
  (submission through terminal outcome)
- CDK constructs for:
  - the supporting processing bucket
  - Athena/Glue databases used to query job logs
  - `JobMonitorFunction`: a batteries-included job-monitor Lambda
  - `JobResubmitFunction`: infrastructure for a retry-queue-driven resubmit
    Lambda

## The `bejm_*` parameters contract

The job monitor is the single owner of state tracking for a job: it derives
a job's previous state itself (from S3), rather than trusting a caller to
supply it, so any submission path -- your normal pipeline, `resubmit_job`,
or an ad hoc/backfill job submitted by hand -- is tracked correctly as long
as it sets a few identifying parameters on the AWS Batch
`SubmitJobRequest.parameters` (a flat `dict[str, str]`, echoed back into
every EventBridge job state-change event for that job's life):

| Parameter | Meaning |
|---|---|
| `bejm_job_type` | The job type. |
| `bejm_input_entity_id` | The processed (input) entity identifier. |
| `bejm_output_entity_id` | The output entity identifier. |
| `bejm_partition_fields` | JSON-encoded `dict[str, str]` of ordered partition key/value pairs. |
| `bejm_attempt` | 1-based attempt number, as a string. |

Build these with `JobContext.to_batch_parameters()`:

```python
from batch_event_job_monitor import JobContext, submit_job

context = JobContext.new(
    job_type="monthly-composite",
    partition_fields={"tile_id": "12TVK", "year_month": "2024-06"},
    input_entity_id="12TVK_2024-06_source",
    output_entity_id="HLS.COMPOSITE.T12TVK.202406.v2.0",
)
submit_job(
    batch_client=batch_client,
    build_submit_job_params=lambda ctx: {
        "jobName": f"composite-{ctx.input_entity_id}",
        "jobQueue": "...",
        "jobDefinition": "...",
    },
    context=context,
)
```

Resubmitting an already-tracked entity outside the retry-queue flow (e.g. a
manual resubmission via the Batch console/CLI)? Use
`S3RecordStore.next_attempt(...)` to get the correct next `attempt` without
hand-computing it.

## Batteries included: `JobMonitorFunction`

`JobMonitorFunction` bundles its own Lambda handler -- no consumer-authored
Python is required for the common case. It wires an EventBridge rule
matching every `aws.batch` job state-change event to the bundled handler,
which decodes the `bejm_*` identity parameters and calls `monitor_job`
internally.

```python
from batch_event_job_monitor.models import JobTypeConfig, RetryPolicy
from batch_event_job_monitor_cdk import JobMonitorFunction

JobMonitorFunction(
    self,
    "JobMonitor",
    processing_bucket=processing_bucket.bucket,
    default_job_type_config=JobTypeConfig(retry_policy=RetryPolicy(max_attempts=3)),
    retry_queue=retry_queue,
    dlq=dlq,
)
```

### Per-job_type classification config

Retry policy and exit-code handling are deploy-time configuration -- tied
to a container image/tag, not to any individual job -- so they're resolved
by `JobMonitorFunction` itself from a `JobTypeConfig`, not read from a
job's own Batch parameters. This also means retry behavior can differ by
job_type: some job types are flakier than others (e.g. ones calling
external services).

```python
from batch_event_job_monitor.models import (
    ExitCodeOutcomesBuilder,
    JobTypeConfig,
    RetryPolicy,
)
from batch_event_job_monitor_cdk import JobMonitorFunction

JobMonitorFunction(
    self,
    "JobMonitor",
    processing_bucket=processing_bucket.bucket,
    default_job_type_config=JobTypeConfig(retry_policy=RetryPolicy(max_attempts=3)),
    job_type_configs={
        "monthly-composite": JobTypeConfig(
            retry_policy=RetryPolicy(max_attempts=5),  # flakier: calls an external API
            exit_code_outcomes=(
                ExitCodeOutcomesBuilder()
                .add(3, "LOW_SUN_ANGLE", dlq=False)
                .add(4, "CLOUDY", dlq=False)
                .build()
            ),
        ),
    },
    retry_queue=retry_queue,
    dlq=dlq,
)
```

A job_type not listed in `job_type_configs` uses `default_job_type_config`.
Changing either requires a redeploy of `JobMonitorFunction` -- there is no
per-job or per-invocation override, consistent with this being
container-tied configuration, not job data.

Within `ExitCodeOutcome`, `retryable` (default `False`) selects
`FAILURE_RETRYABLE` vs `FAILURE_NONRETRYABLE` for routing/retry purposes;
`dlq` (default `True`) controls whether a terminal instance of that
outcome is sent to the DLQ. `label` is descriptive only -- it appears in
the canonical event and replaces the state name in the output-index key
(e.g. `outputs/state=CLOUDY/...` instead of
`outputs/state=FAILURE_NONRETRYABLE/...`) but never affects routing. State
*pointer* keys are unaffected by `label` -- they stay on the fixed
`ProcessingState` taxonomy, since `monitor_job`'s internal bounded state
lookups depend on enumerating a closed set.

## Library-only path

For consumers who need custom logic the bundled handler can't express, the
underlying functions are all directly importable: `monitor_job`,
`resubmit_job`, `submit_job`, `JobDetails`, `JobContext`, `S3RecordStore`.
Write your own Lambda handler and wire it up yourself.

## Resubmitting jobs: `JobResubmitFunction`

Unlike the job monitor, the resubmit Lambda can't be fully generic: the AWS
Batch `submit_job` parameters (job definition, container overrides, etc.)
are inherently job-type specific. `JobResubmitFunction` wires the supporting
infrastructure (the retry-queue event source, IAM permissions) around a
Lambda entry point you provide -- see `examples/job_resubmit_handler.py` for
a starting point. That file ships as documentation only; it is not part of
the installable package.

```python
from batch_event_job_monitor_cdk import JobResubmitFunction

JobResubmitFunction(
    self,
    "JobResubmit",
    entry="lambda/job_resubmit",
    index="handler.py",
    retry_queue=retry_queue,
    batch_job_queue_arn=job_queue.job_queue_arn,
    batch_job_definition_arn=job_definition.job_definition_arn,
)
```

## Origin

This package generalizes job-monitoring patterns first developed in `hls-vi-historical-orchestration` and
`hls-nextgen-orchestration` into a standalone, reusable library.
