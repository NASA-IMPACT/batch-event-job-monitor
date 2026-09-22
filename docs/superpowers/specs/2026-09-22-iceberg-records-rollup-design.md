# Iceberg records rollup - design

Date: 2026-09-22 Status: proposed

## Problem

The `records/` prefix holds one small JSON object per entity processing attempt:

```
records/job_type={job_type}/{partition_fields...}/input_entity_id={input_entity_id}/{attempt:03d}.json
```

`AthenaRecordsTable` registers a partition-projected Glue table over that prefix so the records are queryable. Two
problems follow from the storage shape:

1. **Small files.** Every query pays per-object overhead across the whole scanned prefix. The cost is dominated by
   object count, not bytes.
2. **Injected projection.** High-cardinality partition keys use `projection: "injected"`, and Athena rejects any query
   on such a table that omits an equality predicate on every injected key. The table therefore cannot answer
   cross-partition questions at all - only point lookups where the key layout is already known.

The `state/` and `outputs/` prefixes do not have this problem: they are S3-Inventory-backed, already Parquet, and carry
no object bodies worth reading. Only `records/` needs a rollup, because its body holds the only data not derivable from
a key - `events[]`, `current_state`, `output_entity_id`, and `batch_job_id`.

## Goals

- A records table that supports wide, cross-partition scans at Parquet speed.
- Divergence from the JSON source bounded and measurable, converging to zero.
- One records query surface, not two.
- No change to the write path. The JSON objects remain the sole source of truth.

## Non-goals

- Event-grain (exploded `events`) analytics.
- Deriving `state_view` / `outputs_view` from the rolled-up table.
- Cross-region replication.

## Decisions

| decision         | choice                                     | rationale                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| table format     | Apache Iceberg via Glue catalog            | Record objects are overwritten in place as events accumulate, so the rollup needs upsert, not append. Iceberg `MERGE INTO` is the managed answer.                                                                                                                                                                                                                                                                                                                                                  |
| engine           | Lambda + Athena `MERGE INTO`               | At 10k-100k changed objects/day, Glue Spark pays a 2-DPU floor per invocation for work a Lambda finishes in under a minute. ~$3/mo vs ~$25/mo at hourly.                                                                                                                                                                                                                                                                                                                                           |
| change detection | S3 EventBridge notifications -> SQS        | Glue job bookmarks are the off-the-shelf alternative but require enumerating the whole `records/` prefix per run, a cost that scales with total corpus size rather than churn.                                                                                                                                                                                                                                                                                                                     |
| cadence          | hourly (construct-configurable)            | Staleness bound of 1h, versus 24-48h for the existing inventory-backed views.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| query surface    | Iceberg only; `AthenaRecordsTable` retired | Point lookups with a known key belong in `S3RecordStore.get_object`, not Athena. The JSON Glue table existed only to make ad-hoc SQL possible, and Iceberg does that strictly better.                                                                                                                                                                                                                                                                                                              |
| maintenance      | Glue Data Catalog automatic optimization   | Compaction, snapshot expiry, and orphan-file removal are a managed service. Do not hand-roll `OPTIMIZE` / `VACUUM`.                                                                                                                                                                                                                                                                                                                                                                                |
| backfill         | reconcile mode, self-chaining              | Reconcile against an empty table returns every key, so backfill is not a separate code path.                                                                                                                                                                                                                                                                                                                                                                                                       |
| enumeration      | daily `records/` S3 Inventory              | $0.0025 per million objects listed - about $0.76/month at 10M objects. Keeps reconcile a two-line SQL anti-join. `ListObjectsV2` from the Lambda is marginally cheaper but has no SQL join, and would need either 10M rows pulled through `GetQueryResults` or a hand-rolled reimplementation of S3 Inventory. Weekly delivery would cost $0.11/month and match the reconcile schedule, but daily was chosen for the faster first report during migration and the freedom to reconcile more often. |

## Architecture

```
monitor Lambda  --PUT-->  records/.../001.json
                              |
                              | S3 EventBridge notification (prefix records/)
                              v
                    EventBridge rule --> SQS rollup queue --> DLQ
                              ^                  |
             enqueue missing  |                  | EventBridge Scheduler (hourly)
                keys          |                  v
                     ReconcileFunction    RollupFunction
                        (weekly +               1. drain queue -> distinct keys
                         self-chained)          2. GET objects (threadpool)
                              |                 3. write one gzipped NDJSON to staging/
                              |                 4. Athena MERGE INTO iceberg table
                              |                 5. delete SQS messages, delete staging object
                              |                 6. self-invoke if queue still non-empty
                              v
                    records/ S3 Inventory  <----- anti-join against Iceberg table
```

Messages are deleted only after the MERGE succeeds. An invocation that dies mid-run leaves them to reappear; the next
run re-fetches and re-merges. Every operation is idempotent on the natural key, so replay is always safe.

An object overwritten between notification and GET is read at its current content, which is what upsert wants. Duplicate
notifications for one key collapse during dedup.

## Components

```
src/batch_event_job_monitor/
  rollup.py                     core: dedup, fetch, NDJSON encode, SQL generation, time budget
  handlers/rollup_handler.py    thin dispatch on mode: "rollup" | "reconcile"
src/batch_event_job_monitor_cdk/
  iceberg_records_table.py      Iceberg table (custom resource), staging table, records
                                inventory table, table optimizer
  records_rollup_function.py    queue, DLQ, EventBridge rule, both Lambdas, schedules
  athena_common.py              + Iceberg and staging helpers
  partition_key_spec.py         + merge-condition and column-list generators
```

`rollup.py` mirrors the `log_store.py` / `lambda_core.py` split: logic is importable and unit testable, the handler is
thin. Both constructs take the same ordered `partition_keys: list[PartitionKeySpec]` that already drives the existing
Athena constructs, so the Iceberg schema, the MERGE `ON` clause, and the reconcile join all generate from one
declaration.

`records_rollup_function.py` creates two Lambda functions from one asset - `rollup_function` and `reconcile_function` -
so each gets its own timeout, reserved concurrency, and alarms, and a long-running reconcile chain can never starve the
rollup. It creates its own Athena workgroup with results under the processing bucket unless handed one, matching the
batteries-included shape of `JobMonitorFunction`.

`athena_records_table.py` is deleted at the end of the migration sequence.

## Iceberg table

### Schema

| column                 | type                                                                             | notes                                                               |
| ---------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| _partition keys_       | `string`                                                                         | one per `PartitionKeySpec`, ordered, `job_type` first. Natural key. |
| `input_entity_id`      | `string`                                                                         | natural key                                                         |
| `attempt`              | `int`                                                                            | natural key                                                         |
| `output_entity_id`     | `string`                                                                         |                                                                     |
| `batch_job_id`         | `string`                                                                         |                                                                     |
| `current_state`        | `string`                                                                         |                                                                     |
| `events`               | `array<struct<state:string,timestamp:string,batch_job_id:string,exit_code:int>>` | verbatim from the JSON body                                         |
| `last_event_timestamp` | `timestamp`                                                                      | `events[-1].timestamp`, parsed. MERGE ordering key.                 |
| `source_key`           | `string`                                                                         | provenance; reconcile joins on it                                   |
| `rolled_up_at`         | `timestamp`                                                                      | freshness bound is `max(rolled_up_at)`                              |

**Natural key**: the full ordered partition-key list plus `input_entity_id` plus `attempt`. `job_type` is the first
entry of the partition-key list per the existing repo convention, not a special case.

**Ordering key**: `last_event_timestamp` rather than the S3 `LastModified` header. It lives in the body, so it survives
any change of ingestion engine, and it is the semantic time rather than the storage time.
`ProcessingEventRecord.timestamp` is written as `datetime.now(timezone.utc).isoformat()`, so `from_iso8601_timestamp()`
parses it. The events array is append-only within a record, so the final element is always the most recent.

`ProcessingEventRecord.to_dict()` drops `None` fields, so `batch_job_id` and `exit_code` may be absent rather than null
in the JSON. The JSON SerDe reads a missing field as NULL; no special handling needed.

### Physical layout

- Format version 2, Parquet.
- `PARTITIONED BY (job_type)` - identity on `job_type` only.
- Copy-on-write for updates and deletes.
- No declared sort order. See below.

At the expected scale (under 10M rows, roughly 1-2 GB of Parquet) file-level min/max statistics do the pruning that
partitioning would otherwise have to. Identity-partitioning a high-cardinality key such as a tile id would rebuild the
small-file problem in Parquet, which is the problem being solved.

**No sort order is declared, because Athena cannot declare one.** Athena's `CREATE TABLE` for Iceberg supports neither
`WRITE ORDERED BY` nor a `sorted_by` table property; setting a sort order requires Spark, which this design removed on
purpose. Two consequences, both accepted:

- Pruning rests on file-level statistics alone. Rows land clustered by whatever keys changed in a given merge, which
  correlates with recency rather than with the natural key.
- Glue automatic optimization can only binpack-compact, not sort-compact, since sort compaction reads the sort order
  from table metadata that will not be there.

Neither matters at 1-2 GB. If the table grows to where sorted layout would pay for itself, that is the same threshold at
which the partition spec wants revisiting - see Revisit triggers, where the engine question reopens anyway.

### Creation

CDK cannot create an Athena/Iceberg table declaratively through `CfnTable` the way the existing constructs do, because
the Iceberg metadata file must exist before the catalog entry is valid. A CDK custom resource runs the DDL instead:

- **Create**: `CREATE TABLE IF NOT EXISTS ... TBLPROPERTIES ('table_type'='ICEBERG', ...)`
- **Update**: `ALTER TABLE ... ADD COLUMN` for added columns. Any other schema change is a manual migration and the
  custom resource fails loudly rather than guessing.
- **Delete**: `DROP TABLE` only when the removal policy is `DESTROY`, matching the other constructs.

This is the one place the design departs from the repo's pure-`CfnTable` pattern.

### Maintenance

A `glue.CfnTableOptimizer` on the table enables Glue Data Catalog automatic optimization: compaction, snapshot
expiration, and orphan-file removal, triggered past a file-count threshold. Billed at $0.44/DPU-hour with a 1-minute
minimum, proportional to commit frequency. No `OPTIMIZE` or `VACUUM` code ships with this design.

## Staging table

A Glue JSON table over `staging/`, partitioned by `run_id` with `injected` projection. The Lambda supplies the equality
predicate on every read, so injected projection costs nothing here.

Columns match the Iceberg table minus `rolled_up_at`, which the MERGE sets. The Lambda writes the derived columns
(`source_key`, `last_event_timestamp`, and the partition key values parsed out of the object key) into the NDJSON
directly, so no path parsing happens in SQL.

A lifecycle rule expires `staging/` after 7 days, covering the case where a delete fails after a successful merge.

## Rollup mode

1. Drain the queue with long polling, up to a configurable per-run key cap (default 25,000). Deduplicate keys; retain
   receipt handles.
2. `GET` each object on a thread pool. A 404 is skipped and counted (`MissingSourceObjects`) - the object was deleted
   between notification and read, which is not an error. Malformed JSON is skipped, logged, and counted; reconcile
   re-surfaces it.
3. Build one gzipped NDJSON object at `staging/run_id=<uuid>/part.ndjson.gz`. The gzip stream is written to `/tmp`
   rather than buffered in memory so a large backlog cannot exhaust the heap.
4. Run the MERGE (below) via `StartQueryExecution` and poll to completion.
5. On success, delete the SQS messages and the staging object.
6. If `ApproximateNumberOfMessages` is still non-zero and the chain depth is below its maximum, self-invoke
   asynchronously to drain the next batch rather than waiting for the next scheduled tick.

### MERGE

```sql
MERGE INTO <database>.<iceberg_table> t
USING (SELECT * FROM <database>.<staging_table> WHERE run_id = '<run_id>') s
    ON t.job_type = s.job_type
   AND t.<partition_key> = s.<partition_key>   -- one line per PartitionKeySpec
   AND t.input_entity_id = s.input_entity_id
   AND t.attempt = s.attempt
WHEN MATCHED AND s.last_event_timestamp >= t.last_event_timestamp THEN
    UPDATE SET output_entity_id = s.output_entity_id,
               batch_job_id = s.batch_job_id,
               current_state = s.current_state,
               events = s.events,
               last_event_timestamp = s.last_event_timestamp,
               source_key = s.source_key,
               rolled_up_at = current_timestamp
WHEN NOT MATCHED THEN
    INSERT (...) VALUES (..., current_timestamp)
```

The `last_event_timestamp >=` guard makes out-of-order replay safe: a reconcile that enqueues an older version of a key
cannot clobber a newer merged row.

One source row per target row is guaranteed because staging is deduplicated by key in step 1, which is the precondition
Iceberg MERGE requires.

## Reconcile mode

Runs on a weekly schedule, and on demand during backfill.

Requires a `records/` S3 Inventory. `("records", "records/")` is added to the `ProcessingBucket` `inventories` list, and
`IcebergRecordsTable` builds the Glue table over it with the existing `athena_common.create_inventory_table` helper - so
it takes `records_inventory_location_s3path` and `table_datetime_start` exactly as `AthenaStateTable` does today, with
the same constraint that the anchor's time-of-day must match the S3 delivery hour.

```sql
SELECT inv.key
FROM <database>.<records_inventory_table> inv
LEFT JOIN <database>.<iceberg_table> t ON t.source_key = inv.key
WHERE inv.dt = (SELECT max(dt) FROM <database>.<records_inventory_table>)
  AND inv.is_latest
  AND NOT inv.is_delete_marker
  AND (t.source_key IS NULL OR inv.last_modified_date > t.rolled_up_at)
```

Matching keys are sent to the same SQS queue and flow through the normal rollup path. The row count is emitted as
`ReconcileDrift`; alarm on sustained non-zero.

Comparing S3's `last_modified_date` against Athena's `rolled_up_at` can produce false positives under clock skew. These
are harmless - the key is merged again, idempotently.

Inventory lag means reconcile only ever validates data older than roughly 48h. That is correct for its purpose: it is
the correctness backstop for the notification path, not the freshness path.

### Self-chaining and backfill

Reconcile pages the Athena result with `GetQueryResults`. When the time budget runs low (stop with about 2 minutes of
timeout remaining) and rows remain, it self-invokes asynchronously carrying `{query_execution_id, next_token, depth}`.
Continuing an existing result set rather than re-running the query keeps the snapshot consistent across hops.

Reconcile is **convergent**: every key it enqueues gets merged, so the next pass matches strictly fewer. Backfill is
therefore not a distinct code path - it is reconcile against a cold table, and it terminates on its own.

The two chains compose. Reconcile needs roughly 4-5 hops to enqueue a 10M-key corpus; the rollup chain then drains it at
the per-run cap without waiting on the hourly schedule. Full backfill is on the order of hours, unattended, rather than
weeks of scheduled ticks.

Both functions must set `recursive_loop=lambda.RecursiveLoop.ALLOW`. Lambda's recursive loop detection defaults to
`TERMINATE`, which cuts a self-invoking chain off at 16 invocations - enough for a reconcile chain, but it would
silently halt a backfill drain chain roughly 25 times short of finishing. The failure is quiet, so without this setting
backfill would appear to stall for no visible reason. `aws-cdk-lib>=2.262.0` is pinned, well past the version that added
the property.

That is a deliberate opt-out of an AWS safety feature, so the compensating controls carry the whole burden:

- max-depth counter in the invocation payload, default 1,000, checked before every self-invoke
- async retry attempts set to 0, so a Lambda-level retry cannot double-merge
- reserved concurrency of 1 on each function, making each chain serial by construction
- `ChainDepth` metric emitted per hop, alarmed as it approaches the maximum

Worst case if the depth guard itself is wrong: 1,000 invocations of at most 15 minutes each, bounded and visible on the
metric, rather than an unbounded spend.

## Failure handling

| failure                 | behavior                                                                          |
| ----------------------- | --------------------------------------------------------------------------------- |
| invocation dies mid-run | messages reappear, next run re-merges (idempotent)                                |
| Athena query fails      | handler raises, messages retained, staging object expires via lifecycle rule      |
| poison key              | SQS `maxReceiveCount` 5 -> DLQ; queue keeps moving; alarm on DLQ depth            |
| source object 404       | skip, count `MissingSourceObjects`, not an error                                  |
| malformed JSON          | skip, log, count; reconcile re-surfaces it                                        |
| backlog                 | per-run key cap bounds every run; remainder stays queued and visible as depth/age |

The per-run cap is what turns the 15-minute Lambda timeout from a failure mode into a throughput bound. Steady state at
hourly is roughly 15-45 seconds against a 900-second limit.

## Observability

Metrics follow the existing repo pattern: Embedded Metric Format records on stdout under the `BatchEventJobMonitor`
namespace, as `untracked.py` does. No `PutMetricData` call and no CloudWatch IAM grant.

- `RolledUpRecords`
- `MergeDurationMs`
- `MissingSourceObjects`
- `MalformedRecords`
- `ReconcileDrift`
- `ChainDepth`
- `RollupFailures`

Plus native SQS metrics for queue depth, message age, and DLQ depth.

Freshness bound for consumers: `SELECT max(rolled_up_at) FROM <iceberg_table>`.

## IAM

Rollup function:

- `s3:GetObject` on `records/*`
- `s3:PutObject`, `s3:DeleteObject` on `staging/*`
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the Iceberg data location and the Athena results location
- `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`
- `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
- `glue:GetDatabase`, `glue:GetTable`, `glue:UpdateTable` (Iceberg commits update the catalog entry)
- `lambda:InvokeFunction` on itself, for the drain chain

Reconcile function: the Athena and Glue read grants above, `sqs:SendMessage`, and `lambda:InvokeFunction` on itself. No
write access to the Iceberg data location.

The table optimizer takes its own role with read/write on the Iceberg data location and `glue:UpdateTable`.

## Testing

**Unit, no AWS**: key deduplication; partition-field extraction from an object key; NDJSON encoding including absent
`batch_job_id` / `exit_code`; `last_event_timestamp` derivation; MERGE SQL generation from a `PartitionKeySpec` list;
reconcile SQL generation; time-budget and max-depth guards.

**Stubbed boto3**: drain -> fetch -> staging write; 404 skipped and counted; malformed JSON skipped; messages _not_
deleted when the merge fails; self-invoke fired when the queue is non-empty and suppressed at max depth.

**CDK assertions**: table optimizer present; schedule rate; reserved concurrency of 1 on both functions;
`RecursiveLoop.ALLOW` on both functions; queue visibility timeout exceeds the rollup timeout; DLQ `maxReceiveCount`;
staging lifecycle rule; IAM grants; async retry attempts of 0.

The `RecursiveLoop` assertion matters more than it looks: the default would break backfill silently rather than loudly,
so nothing else in the test suite would catch its absence.

**Integration, opt-in, against a deployed dev stack**: insert; update-in-place on re-merge; `last_event_timestamp` guard
rejecting a stale replay; reconcile finding a deliberately withheld key (which is also the backfill test). MERGE
semantics cannot be verified any other way.

## Deployment and migration sequence

Order matters - the JSON table must not be removed until the Iceberg table is verified complete.

1. Add `("records", "records/")` to the `ProcessingBucket` `inventories` list. Deploy. Wait for the first inventory
   report, which can take up to 48h.
2. Deploy `IcebergRecordsTable` and `RecordsRollupFunction`. The hourly rollup begins covering new writes immediately;
   it does not depend on step 1.
3. Invoke reconcile. Let the chains run until `ReconcileDrift` reaches 0.
4. Verify: row count per `job_type` in the Iceberg table against object count per `job_type` in the inventory table.
5. Remove `AthenaRecordsTable` from the stack and delete `athena_records_table.py`.

## Revisit triggers

- **Per-run cap hit routinely, or sustained queue depth.** Port the merge step to Glue Spark driven by the same SQS
  queue. The change-detection half is identical, so only the merge changes.
- **Volume roughly 10x the current estimate.** Spark becomes correct on its own merits, and Glue job bookmarks become
  viable as a replacement for EventBridge/SQS.
- **Table growth past a few tens of millions of rows.** Revisit the partition spec; a bucket transform on the
  highest-cardinality partition key is the next step, not identity partitioning.
- **Complaints about `state_view` / `outputs_view` freshness.** `current_state` is a column on this table, so both views
  are derivable from it an order of magnitude fresher. Out of scope here, but it is where this leads.

## Unverified assumptions

These are estimates the implementation should confirm rather than inherit:

- Athena MERGE duration of 5-30 seconds for a batch at the per-run cap.
- Per-run steady-state total of 15-45 seconds.
- Reconcile enqueue throughput of roughly 2-3M keys per invocation hop.
- Glue automatic optimization cost of roughly $1-5/month at hourly commit frequency.
