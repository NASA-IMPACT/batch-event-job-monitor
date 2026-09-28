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
- [`IcebergRecordsTable`](#icebergrecordstable)
- [`RecordsRollupFunction`](#recordsrollupfunction)
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
| [`IcebergRecordsTable`](#icebergrecordstable)                    | Iceberg table via Athena DDL, staging table, inventory table, optimizer      | a Glue database, bucket name, records inventory location, partition keys                  |
| [`RecordsRollupFunction`](#recordsrollupfunction)                | SQS queue/DLQ, EventBridge rule on `records/` prefix, two Lambdas            | the processing bucket, database name, Iceberg table, partition keys                       |
| [`PartitionKeySpec`](#partitionkeyspec)                          | nothing -- a value type describing one partition key                         | --                                                                                        |
| [`job_type_config()`](#job_type_config)                          | nothing -- a `JobTypeConfig` from typed CDK Batch refs                       | an `IJobQueue`, an `IJobDefinition`                                                       |

None of the Athena constructs create the Glue database itself. You own the `glue.CfnDatabase` and hand the same one to
each of them.

## Wiring it together

```python
import datetime as dt

from aws_cdk import aws_glue as glue
from batch_event_job_monitor_cdk import (
    AthenaOutputsTable,
    AthenaRecordsTable,
    AthenaStateTable,
    IcebergRecordsTable,
    JobMonitorFunction,
    JobResubmitFunction,
    PartitionKeySpec,
    ProcessingBucket,
    RecordsRollupFunction,
    job_type_config,
)

processing = ProcessingBucket(
    self,
    "Processing",
    bucket_name_prefix="my-processing",   # -> my-processing-<accountId>-<region>-an
    inventory_prefix="inventory/",
    inventories=[("state", "state/"), ("outputs", "outputs/"), ("records", "records/")],
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

iceberg_table = IcebergRecordsTable(
    self, "IcebergRecords",
    database=database, database_name="processing",
    processing_bucket_name=processing.bucket_name,
    records_inventory_location_s3path=processing.inventory_location("records"),
    inventory_datetime_start=dt_start, partition_keys=partition_keys,
)

RecordsRollupFunction(
    self, "RecordsRollup",
    processing_bucket=processing.bucket,
    database_name="processing",
    iceberg_table=iceberg_table,
)
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
    inventories=[("state", "state/"), ("outputs", "outputs/"), ("records", "records/")],
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
    inventories=[("state", "state/"), ("outputs", "outputs/"), ("records", "records/")],
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

## `IcebergRecordsTable`

Creates the Apache Iceberg table that rolls up records/ JSON objects into a queryable columnar format via Athena, plus
the supporting staging table (NDJSON), the inventory table (S3 Inventory Parquet), and the Glue table optimizer for
binpack compaction.

The Iceberg table is created through an Athena DDL custom resource rather than a plain `CfnTable`: an Athena/Iceberg
table requires the metadata file to exist at the S3 location before the Glue catalog entry is valid. This is the one
place this library departs from its pure `CfnTable` pattern.

**Schema updates:** Added columns are applied via `ALTER TABLE ... ADD COLUMN`. A dropped or retyped column fails the
deploy loudly and requires manual intervention -- the handler reads the live table schema via Glue `GetTable` on every
update to detect retypes by comparing the new schema against the catalog.

**Workgroup:** When `workgroup_name` is None (the default), the construct creates its own Athena workgroup with query
results stored under `s3://{processing_bucket_name}/athena-results/` and exposes it as the `workgroup` attribute. Pass
an existing workgroup name to skip creating one; in that case the workgroup's query-results location is yours to manage.
Both `workgroup_name` (the resolved name) and `workgroup` (the created workgroup or None) are exposed as attributes.

**Removal policy scope:** The `removal_policy` parameter governs only the Iceberg table. The staging and inventory
tables are always created with `RemovalPolicy.DESTROY`, matching the other inventory-backed tables in this package.

```python
import datetime as dt

iceberg = IcebergRecordsTable(
    self,
    "IcebergRecords",
    database=database,
    database_name="processing",
    processing_bucket_name=processing.bucket_name,
    records_inventory_location_s3path=processing.inventory_location("records"),
    inventory_datetime_start=dt.datetime(2026, 1, 1, 0, 0),  # must match S3 delivery hour
    partition_keys=partition_keys,
    removal_policy=RemovalPolicy.RETAIN,  # keep table on stack deletion
)

iceberg.iceberg_table_name  # the rolled-up records table
iceberg.staging_table_name  # the NDJSON staging table
iceberg.inventory_table_name  # the S3 Inventory query table
iceberg.table_location  # s3://... URI of table data
iceberg.workgroup_name  # Athena workgroup name
iceberg.workgroup  # CfnWorkGroup created, or None if hand-supplied
```

**Requirements:**

- The caller must add `("records", "records/")` to the `ProcessingBucket` `inventories` list. The
  `inventory_datetime_start` must match the S3 Inventory delivery hour (the time-of-day S3 actually delivers reports to
  the `records/` prefix).

## `RecordsRollupFunction`

Creates the SQS queue, its dead-letter queue, the EventBridge rule triggered by new objects under `records/`, and two
Lambda functions that maintain the Iceberg table: one triggered hourly to roll up queued changes, and one triggered
weekly to reconcile drift from the S3 Inventory (the backfill function -- invoke it repeatedly until the
`ReconcileDrift` metric reaches 0).

Both Lambdas are created from a single handler asset with different payloads (`"mode": "rollup"` or
`"mode": "reconcile"`). They share one Athena workgroup (defaulting to `iceberg_table.workgroup_name`).

**Recursive self-invocation:** AWS Lambda defaults to terminating self-invoking chains at 16 invocations as a safety
limit. This construct opts out via `recursive_loop=RecursiveLoop.ALLOW` to support draining long queues and backfilling
large drift. Four compensating controls make this opt-out defensible: (1) reserved concurrency of 1 ensures the chain
cannot fan out; (2) async retries are set to 0 so failures stop the chain rather than retrying; (3) max-depth counter in
the handler prevents infinite loops; (4) `RollupChainDepth`/`ReconcileChainDepth` metrics report depth at each step to
alarm on runaway chains.

**Freshness for consumers:** Query `SELECT max(rolled_up_at) FROM {database}.{iceberg_table}` to get the timestamp of
the most recent rolled-up record.

**Backfill:** To backfill changes, invoke the reconcile function via the AWS Lambda console or CLI (set payload to
`{"mode": "reconcile", "depth": 0}`) and monitor the `ReconcileDrift` metric. When it reaches 0, all drift is
reconciled. Reconcile against a cold table returns every key, so it is the backfill procedure.

**Sort order:** The table has no declared sort order because Athena does not support declaring one for Iceberg (neither
`WRITE ORDERED BY` nor `sorted_by` table properties are available; setting these requires Spark, which this design
removed on purpose). Glue automatic table optimization binpack-compacts only.

**Workgroup caveat:** When passing an explicit `workgroup_name`, the Athena query-results location grant is still scoped
to `{processing_bucket}/athena-results/*`. Ensure the passed workgroup's results location is within that path, or
queries will fail with access denied.

**Metrics:** Emitted as Embedded Metric Format on stdout under the `BatchEventJobMonitor` namespace (no `PutMetricData`
call, no CloudWatch IAM grant):

- `RolledUpRecords`: records merged in this run
- `MergeDurationMs`: merge operation runtime in milliseconds
- `MissingSourceObjects`: `records/` objects the rollup Lambda's `GetObject` could not find (a 404, not an Athena error)
- `MalformedRecords`: `records/` source objects that could not be parsed or flattened into a staging row
- `ReconcileDrift`: distinct keys still missing from the catalog after this reconcile run (0 means fully reconciled)
- `RollupChainDepth`: rollup self-invocation chain depth (monitor to ensure it does not approach `max_chain_depth`)
- `ReconcileChainDepth`: reconcile self-invocation chain depth (monitor to ensure it does not approach
  `max_chain_depth`)
- `ReconcileTruncated`: 1 when a reconcile chain was refused at `max_chain_depth` with rows still pending, 0 when it
  converged or did not need to chain
- `RollupFailures`: emitted only by the rollup path, 1 when the Athena MERGE raises `AthenaQueryError`

**Alarms worth creating:** See `JobMonitorFunction` on how to alarm on metrics in this namespace.

- `ReconcileDrift` sustained > 0: indicates unfinished backfill
- `ReconcileTruncated` > 0: a reconcile chain gave up at `max_chain_depth` with rows still pending, as distinct from one
  that converged
- `RollupChainDepth` / `ReconcileChainDepth` approaching `max_chain_depth`: queue draining is getting long; may indicate
  a backlog
- DLQ depth: poison messages queued for inspection
- Queue message age: messages waiting for the next scheduled run

```python
RecordsRollupFunction(
    self,
    "RecordsRollup",
    processing_bucket=processing.bucket,  # concrete Bucket (not IBucket)
    database_name="processing",
    iceberg_table=iceberg,
    partition_keys=None,  # defaults to iceberg.partition_key_names
    workgroup_name=None,  # defaults to iceberg.workgroup_name
    max_keys_per_run=25000,
    max_chain_depth=1000,
)
```

**Requirements:**

- `processing_bucket` must be a concrete `s3.Bucket` (not an `s3.IBucket`). The construct calls
  `enable_event_bridge_notification()` and `add_lifecycle_rule()` on the bucket, which are not part of the `IBucket`
  interface. An imported/foreign bucket cannot be used.
- The caller must add `("records", "records/")` to the `ProcessingBucket` `inventories` list.
- The `inventory_datetime_start` passed to `IcebergRecordsTable` must match the S3 Inventory delivery hour.

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
