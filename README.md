# batch-event-job-monitoring

Reusable AWS Batch job-monitoring components: an S3-backed job log store, a
Lambda-based job monitor core, and CDK constructs for the supporting
processing bucket and Athena/Glue databases used to query job logs.

## Origin

This package generalizes job-monitoring patterns first developed in
`hls-vi-historical-orchestration` and `hls-nextgen-orchestration` into a
standalone, reusable library.
