# batch-event-job-monitor

Reusable AWS Batch job-monitoring components:

- an S3-backed job log store
- a Lambda-based job monitor core, tracking a job's full lifecycle (submission through terminal outcome)
- CDK constructs for:
  - the supporting processing bucket
  - Athena/Glue databases used to query job logs
  - `JobMonitorFunction`: a batteries-included job-monitor Lambda
  - `JobResubmitFunction`: infrastructure for a retry-queue-driven resubmit Lambda

## The `bejm_*` parameters contract

The job monitor is the single owner of state tracking for a job: it derives a job's previous state itself (from S3),
rather than trusting a caller to supply it, so any submission path -- your normal pipeline, `resubmit_job`, or an ad
hoc/backfill job submitted by hand -- is tracked correctly as long as it sets a few identifying parameters on the AWS
Batch `SubmitJobRequest.parameters` (a flat `dict[str, str]`, echoed back into every EventBridge job state-change event
for that job's life):

| Parameter               | Meaning                                                             |
| ----------------------- | ------------------------------------------------------------------- |
| `bejm_job_type`         | The job type.                                                       |
| `bejm_input_entity_ids` | JSON-encoded array of processed (input) entity identifiers.         |
| `bejm_output_entity_id` | The output entity identifier.                                       |
| `bejm_partition_fields` | JSON-encoded `dict[str, str]` of ordered partition key/value pairs. |
| `bejm_attempt`          | 1-based attempt number, as a string.                                |

`bejm_input_entity_ids` is a list, not a single id, because one Batch job can cover several input entities sharing one
output -- e.g. twin granules, or several source granules composited into one output. The common case is a single-element
list.

Build these with `JobGroup.to_batch_parameters()`:

```python
from batch_event_job_monitor import JobGroup, submit_job

job_group = JobGroup.new(
    job_type="monthly-composite",
    partition_fields={"tile_id": "12TVK", "year_month": "2024-06"},
    input_entity_ids=["12TVK_2024-06_source"],
    output_entity_id="HLS.COMPOSITE.T12TVK.202406.v2.0",
)
submit_job(
    batch_client=batch_client,
    build_submit_job_params=lambda group: {
        "jobName": group.batch_job_name(),
        "jobQueue": "...",
        "jobDefinition": "...",
    },
    job_group=job_group,
)
```

`JobGroup.batch_job_name()` builds a Batch-safe `jobName` (Batch caps this at 128 characters) from
`job_type`/`input_entity_ids`/`attempt`, truncating and appending a short hash so distinct groups never collide even
once truncated.

`monitor_job` classifies a Batch job's outcome once per event (one exit code, one retry/DLQ decision) but writes a
canonical record and state pointer for each of `input_entity_ids` individually -- see `JobGroup.contexts()`, which fans
a group out into one `JobContext` per entity.

Resubmitting an already-tracked entity outside the retry-queue flow (e.g. a manual resubmission via the Batch
console/CLI)? Use `S3RecordStore.next_attempt(...)` (single entity) or `S3RecordStore.next_group_attempt(...)` (a
multi-entity group -- the max `next_attempt` across all its entities, so a prior partial write failure is still handled
safely) to get the correct next `attempt` without hand-computing it.

## Batteries included: `JobMonitorFunction`

`JobMonitorFunction` bundles its own Lambda handler -- no consumer-authored Python is required for the common case. It
wires one EventBridge rule per job_type, each scoped to that job_type's own Batch job queue/job definition, to the
bundled handler, which decodes the `bejm_*` identity parameters and calls `monitor_job` internally.

Every job_type this construct monitors needs an entry in `job_type_configs` -- a job_type not listed there is never
matched by any rule the construct creates, so its events never reach the Lambda. `JobTypeConfig` bundles that job_type's
queue/job-definition identity alongside its retry policy and exit-code handling -- all deploy-time, tied to a container
image/tag, not read from a job's own Batch parameters. This also means retry behavior can differ by job_type: some job
types are flakier than others (e.g. ones calling external services).

```python
from aws_cdk import aws_batch as batch
from batch_event_job_monitor.models import ExitCodeOutcomesBuilder, RetryPolicy
from batch_event_job_monitor_cdk import JobMonitorFunction, job_type_config

JobMonitorFunction(
    self,
    "JobMonitor",
    processing_bucket=processing_bucket.bucket,
    job_type_configs={
        "monthly-composite": job_type_config(
            job_queue=job_queue,            # aws_cdk.aws_batch.IJobQueue
            job_definition=job_definition,  # aws_cdk.aws_batch.IJobDefinition
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

`job_type_config()` extracts `job_queue_arn`/`job_definition_arn` from typed CDK refs (stripping any job-definition
revision suffix -- Batch resolves a family ARN to whichever revision is currently `ACTIVE`, so a new revision is picked
up automatically without redeploying). Changing a `JobTypeConfig` requires a redeploy of `JobMonitorFunction` -- there
is no per-job or per-invocation override, consistent with this being container-tied configuration, not job data.

Within `ExitCodeOutcome`, `name` (e.g. `"CLOUDY"`) becomes the `ProcessingState.name` used everywhere a state name
appears -- the canonical event, the output-index key (e.g. `outputs/state=CLOUDY/...`), and the state _pointer_ key.
There's no separate closed `ProcessingState` taxonomy that custom outcomes fall back to for pointer purposes: a
job_type's declared `ExitCodeOutcomes` form the bounded set of states `monitor_job` scans, baseline states plus
whatever's declared. `retryable` (default `False`) selects retryable vs non-retryable routing; `dlq` is required (no
default) -- every outcome must state its routing intent explicitly.

## Library-only path

For consumers who need custom logic the bundled handler can't express, the underlying functions are all directly
importable: `monitor_job`, `resubmit_job`, `submit_job`, `JobDetails`, `JobGroup`, `JobContext`, `S3RecordStore`. Write
your own Lambda handler and wire it up yourself.

## Resubmitting jobs: `JobResubmitFunction`

`JobResubmitFunction` bundles a generic default handler for the common case (same job queue/job definition every
attempt, per job_type) -- no consumer-authored Lambda code needed. It takes the same `job_type_configs` mapping
`JobMonitorFunction` does, and routes each resubmission to its job_type's own queue/job definition:

```python
from batch_event_job_monitor_cdk import JobResubmitFunction

JobResubmitFunction(
    self,
    "JobResubmit",
    job_type_configs=job_type_configs,  # the same mapping passed to JobMonitorFunction
    retry_queue=retry_queue,
)
```

For per-attempt `containerOverrides`, a computed command, or `batch:DescribeJobs`-driven introspection of the original
job, supply your own `entry`/`index` instead -- see [`docs/resubmitting-jobs.md`](docs/resubmitting-jobs.md) for the
override path and a starting-point handler.

## Origin

This package generalizes job-monitoring patterns first developed in `hls-vi-historical-orchestration` and
`hls-nextgen-orchestration` into a standalone, reusable library.
