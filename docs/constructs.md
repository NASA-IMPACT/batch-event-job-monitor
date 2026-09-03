# CDK construct reference

<!--toc:start-->

- [Wiring it together](#wiring-it-together)
- [`ProcessingBucket`](#processingbucket)
  - [Naming: global vs account regional namespace](#naming-global-vs-account-regional-namespace)
- [`MonitoringQueues`](#monitoringqueues)
- [`JobMonitorFunction`](#jobmonitorfunction)
  - [Untracked jobs](#untracked-jobs)
- [`JobResubmitFunction`](#jobresubmitfunction)
- [`AthenaRecordsTable`](#athenarecordstable)
- [`AthenaStateTable` and `AthenaOutputsTable`](#athenastatetable-and-athenaoutputstable)
- [`PartitionKeySpec`](#partitionkeyspec)
- [`job_type_config()`](#jobtypeconfig)

<!--toc:end-->

Everything in `batch_event_job_monitor_cdk`, what it creates, and what it needs from you.

| Construct / helper                                               | Creates                                                                      | Needs                                                                                     |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [`ProcessingBucket`](#processingbucket)                          | S3 bucket + one daily Parquet S3 Inventory per prefix, lifecycle, self-grant | a bucket name or name prefix, an inventory prefix, `(inventory_id, objects_prefix)` pairs |
| [`MonitoringQueues`](#monitoringqueues)                          | the five SQS queues every failure path terminates in                         | nothing                                                                                   |
| [`JobMonitorFunction`](#jobmonitorfunction)                      | the monitor Lambda + its EventBridge rules (tracked and catch-all)           | the processing bucket, a `job_type_configs` mapping                                       |
| [`JobResubmitFunction`](#jobresubmitfunction)                    | the resubmit Lambda + its SQS event source and `batch:SubmitJob` grant       | the same `job_type_configs`, the retry queue                                              |
| [`AthenaRecordsTable`](#athenarecordstable)                      | a partition-projected Glue table over `records/`                             | a Glue database, the bucket name, partition keys                                          |
| [`AthenaStateTable`](#athenastatetable-and-athenaoutputstable)   | a Glue table over the `state/` S3 Inventory + a Presto view                  | a Glue database, the inventory location, partition keys                                   |
| [`AthenaOutputsTable`](#athenastatetable-and-athenaoutputstable) | a Glue table over the `outputs/` S3 Inventory + a Presto view                | a Glue database, the inventory location, partition keys                                   |
| [`PartitionKeySpec`](#partitionkeyspec)                          | nothing -- a value type describing one partition key                         | --                                                                                        |
| [`job_type_config()`](#job_type_config)                          | nothing -- a `JobTypeConfig` from typed CDK Batch refs                       | an `IJobQueue`, an `IJobDefinition`                                                       |

None of the Athena constructs create the Glue database itself. You own the `glue.CfnDatabase` and hand the same one to
each of them.

## Wiring it together

```python
from aws_cdk import aws_glue as glue
from batch_event_job_monitor_cdk import (
    AthenaOutputsTable,
    AthenaRecordsTable,
    AthenaStateTable,
    JobMonitorFunction,
    JobResubmitFunction,
    PartitionKeySpec,
    ProcessingBucket,
    job_type_config,
)

processing = ProcessingBucket(
    self,
    "Processing",
    bucket_name_prefix="my-processing",   # -> my-processing-<accountId>-<region>-an
    inventory_prefix="inventory/",
    inventories=[("state", "state/"), ("outputs", "outputs/")],
)

job_type_configs = {
    "monthly-composite": job_type_config(job_queue=job_queue, job_definition=job_definition),
}

monitor = JobMonitorFunction(
    self,
    "JobMonitor",
    processing_bucket=processing.bucket,
    job_type_configs=job_type_configs,
)

JobResubmitFunction(
    self,
    "JobResubmit",
    job_type_configs=job_type_configs,
    retry_queue=monitor.queues.retry_queue,
)

database = glue.CfnDatabase(...)
partition_keys = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "injected"),
    PartitionKeySpec(
        "year_month", "string", "date",
        date_range=("2020-01", "NOW"), date_format="yyyy-MM", date_interval_unit="MONTHS",
    ),
]

AthenaRecordsTable(self, "Records", database=database, database_name="processing",
                   records_bucket_name=processing.bucket_name, partition_keys=partition_keys,
                   table_name="records")
AthenaStateTable(self, "State", database=database, database_name="processing",
                 inventory_location_s3path=processing.inventory_location("state"),
                 table_datetime_start=dt_start, table_name="state_inventory", view_name="state",
                 partition_keys=partition_keys)
AthenaOutputsTable(self, "Outputs", database=database, database_name="processing",
                   inventory_location_s3path=processing.inventory_location("outputs"),
                   table_datetime_start=dt_start, table_name="outputs_inventory", view_name="outputs",
                   partition_keys=partition_keys)
```

## `ProcessingBucket`

The bucket the monitor writes canonical records, state pointers, and output-index entries to, plus one daily Parquet S3
Inventory configuration per `(inventory_id, objects_prefix)` pair. `inventory_location(inventory_id)` gives you the
`s3://` path of that inventory's Hive symlink manifests, which is what the state/outputs table constructs read.

### Naming: global vs account regional namespace

Exactly one of `bucket_name` or `bucket_name_prefix` is required.

`bucket_name` puts the bucket in S3's shared global namespace, where the name must be unique across every AWS account in
the partition and becomes claimable by anyone else once you delete it.

`bucket_name_prefix` puts it in your
[account regional namespace](https://docs.aws.amazon.com/AmazonS3/latest/userguide/gpbucketnamespaces.html#account-regional-gp-buckets),
a reserved subdivision only your account can create in. CloudFormation forms the full name as
`{prefix}-{accountId}-{region}-an`:

```python
ProcessingBucket(
    self,
    "Processing",
    bucket_name_prefix="hls-processing",   # -> hls-processing-111122223333-us-west-2-an
    inventory_prefix="inventory/",
    inventories=[("state", "state/"), ("outputs", "outputs/")],
)
```

AWS recommends this as the security default: the name can never be claimed or re-created by another account, and the
same prefix templates cleanly across accounts and regions with no uniqueness fight. The suffix counts against S3's
63-character bucket-name limit, leaving 37 characters for the prefix -- aws-cdk-lib validates the prefix's length and
character set, so a bad prefix fails at synth.

`construct.bucket_name` is the full name either way. For an account regional bucket it carries unresolved account and
region tokens rather than being a plain literal, which is fine everywhere it is used -- the inventory destination ARN,
`inventory_location()`, and the Glue table locations all resolve to `Fn::Join` at synth. Pass it to `AthenaRecordsTable`
as `records_bucket_name` exactly as you would a literal name.

`removal_policy` defaults to `RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE` so a production stack replacement never discards
processing history. For an ephemeral dev stack that should tear down completely, pass both:

```python
ProcessingBucket(
    self,
    "Processing",
    bucket_name_prefix="dev-processing",
    inventory_prefix="inventory/",
    inventories=[("state", "state/"), ("outputs", "outputs/")],
    removal_policy=RemovalPolicy.DESTROY,
    auto_delete_objects=True,
)
```

`auto_delete_objects=True` requires `RemovalPolicy.DESTROY` (the construct raises otherwise). Without it, CloudFormation
cannot delete a non-empty bucket and dev teardown leaves the bucket orphaned.

## `MonitoringQueues`

The five queues every failure path out of the monitor terminates in, so a message that cannot be processed is
inspectable and replayable rather than gone.

| Attribute         | What lands in it                                                                       |
| ----------------- | -------------------------------------------------------------------------------------- |
| `retry_queue`     | retryable failures with attempts remaining -- `JobResubmitFunction`'s event source     |
| `retry_dlq`       | messages the resubmit Lambda could not process, after `max_receive_count` receives     |
| `failure_dlq`     | terminal non-success outcomes (see `ExitCodeOutcome.dlq`)                              |
| `untracked_queue` | raw events for Batch jobs that ran on a monitored queue without the `bejm_*` params    |
| `event_dlq`       | job state-change events the monitor Lambda failed after its EventBridge target retries |

`JobMonitorFunction` creates one for you with defaults, so the batteries-included path needs no queue wiring at all.
Create one yourself to share the queues across constructs, to adopt queues you already own (`retry_queue=` /
`failure_dlq=`), or to change the queue settings:

```python
queues = MonitoringQueues(
    self,
    "Queues",
    max_receive_count=5,                        # receives before redriving to retry_dlq
    retention_period=Duration.days(14),         # SQS maximum; these queues hold evidence
    visibility_timeout=Duration.minutes(5),     # must be >= the resubmit Lambda's timeout
    encryption_master_key=key,                  # implies QueueEncryption.KMS
    removal_policy=RemovalPolicy.RETAIN,        # keep undelivered evidence past teardown
)
JobMonitorFunction(self, "JobMonitor", processing_bucket=..., job_type_configs=..., queues=queues)
```

Encryption at rest defaults to `QueueEncryption.SQS_MANAGED`, set explicitly in the synthesized template rather than
left unset -- SQS applies SSE-SQS either way, but an unset property reads as unencrypted to an auditor or a Config rule.
Every setting applies to all five queues, so you cannot end up with four encrypted and one not.

Adopting a `retry_queue` leaves `retry_dlq` as `None`: only a queue's creator can set its redrive policy, so redrive for
an adopted queue is yours to configure.

With a customer-managed KMS key, remember that EventBridge writes to `untracked_queue` and `event_dlq` directly rather
than through a role this library grants -- that key needs its own key-policy grant for `events.amazonaws.com`.

## `JobMonitorFunction`

Bundles its own Lambda handler and wires two kinds of EventBridge rule to it.

**Tracked rules -- one per job_type.** Scoped to that job*type's own Batch job queue and job definition (matching the
job definition by prefix, since the event's `jobDefinition` is revision-suffixed), for all seven Batch statuses, and
requiring `bejm_job_type` to be present. The bundled handler decodes the `bejm*\*`parameters and calls`monitor_job`.

**Catch-all rules -- one per distinct Batch job queue.** Scoped to the queue only, for `SUBMITTED`/`SUCCEEDED`/`FAILED`,
matching jobs where `bejm_job_type` is _absent_. See [untracked jobs](#untracked-jobs) below.

The two patterns are complements, so exactly one path handles each job and an unmonitored job can no longer crash the
handler on its way to being dropped.

The EventBridge target retries three times (`retry_attempts`) and then delivers to `queues.event_dlq`.

```python
monitor = JobMonitorFunction(
    self,
    "JobMonitor",
    processing_bucket=processing.bucket,
    job_type_configs=job_type_configs,
    queues=queues,                              # optional; created for you if omitted
    metric_namespace="BatchEventJobMonitor",    # CloudWatch namespace for UntrackedJobs
)
monitor.function          # the Lambda
monitor.queues            # the MonitoringQueues it routes to
monitor.rules             # dict[job_type, events.Rule]
monitor.untracked_rules   # dict[job_type, events.Rule], one per distinct job queue
```

### Untracked jobs

A job submitted without `JobGroup.to_batch_parameters()` runs perfectly and produces no canonical record, no state
pointer, and no output-index entry -- nothing to query, and no error to notice. That is the failure mode a manual or
backfill submission is most likely to hit.

The catch-all rule sends those jobs to two independent targets:

- the monitor Lambda, which emits an `UntrackedJobs` CloudWatch metric (Embedded Metric Format on stdout, dimensioned by
  job queue name) and a structured log line naming the job and the missing contract;
- `queues.untracked_queue`, which keeps the raw event so the submission can be recovered and replayed once the caller is
  fixed.

The targets are independent, so the replay copy survives even if the Lambda fails.

Alarm on the metric to turn a silent hole into a page:

```python
cloudwatch.Metric(
    namespace="BatchEventJobMonitor",
    metric_name="UntrackedJobs",
    dimensions_map={"JobQueue": job_queue.job_queue_name},
    statistic="Sum",
).create_alarm(self, "UntrackedJobs", threshold=1, evaluation_periods=1)
```

A job carrying `bejm_job_type` but missing the rest of the contract is tracked-but-malformed, not untracked: it reaches
the handler, `decode_job_group` raises with the list of missing parameters, and the event ends up in `event_dlq`.

## `JobResubmitFunction`

Drains `retry_queue` and resubmits the next attempt. Bundles a generic default handler covering the common case (reuse
each job_type's own queue and job definition every attempt, no per-attempt overrides); pass `entry`/`index` to wrap your
own handler instead. See [resubmitting-jobs.md](resubmitting-jobs.md).

```python
JobResubmitFunction(
    self,
    "JobResubmit",
    job_type_configs=job_type_configs,          # the same mapping given to JobMonitorFunction
    retry_queue=monitor.queues.retry_queue,
)
```

## `AthenaRecordsTable`

A partition-projected Glue table over the Hive-style `records/` prefix. Partition projection means new partitions are
queryable as records land -- no `MSCK REPAIR TABLE`, no Glue crawler.

## `AthenaStateTable` and `AthenaOutputsTable`

A Glue table over the `state/` (respectively `outputs/`) daily S3 Inventory, plus a Presto view that parses each
inventory key into structured columns. Querying the inventory rather than the objects is what keeps state and output
reconciliation cheap at volume.

`table_datetime_start` anchors the inventory's `dt` partition projection, and its time-of-day must match the hour S3
actually delivers the report. You cannot know that hour until the first report lands, and a wrong guess makes early
partitions read empty rather than error -- so check the delivered `dt=` prefix after the first delivery and correct it.

## `PartitionKeySpec`

One column of the ordered partition-key list the three Athena table constructs share. The same list drives the Glue
partition-key columns, the projection parameters, the `storage.location.template` path, and the `regexp_extract` columns
of the state/outputs views -- including the leading `job_type` entry, which is just another partition key at this layer.

| `projection` | Extra fields                                      | Use for                                               |
| ------------ | ------------------------------------------------- | ----------------------------------------------------- |
| `"enum"`     | `enum_values`                                     | small closed sets -- `job_type`, a fixed product list |
| `"date"`     | `date_range`, `date_format`, `date_interval_unit` | time keys -- `year_month`, `dt`                       |
| `"injected"` | none                                              | high-cardinality keys -- MGRS tile ids, granule ids   |

`"injected"` takes its values from the query's `WHERE` clause instead of an enumerated or generated set, which is how a
high-cardinality key stays partitioned without enumerating every value at deploy time:

```python
PartitionKeySpec("tile_id", "string", "injected")
```

The trade-off is that Athena requires an equality predicate on every injected key of a queried table and rejects the
query without one -- `WHERE tile_id = '14TPN'` is mandatory, not an optimization. Athena only supports injected
projection on string columns, and the construct raises for any other `glue_type`.

## `job_type_config()`

Builds one `JobTypeConfig` from typed CDK Batch refs, reducing the job definition to its family ARN (no revision). Batch
resolves a family ARN to whichever revision is currently `ACTIVE`, so rule scoping and resubmit routing follow a new
revision without a redeploy.
