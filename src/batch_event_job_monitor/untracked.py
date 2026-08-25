"""Recording Batch jobs that ran unmonitored on a monitored job queue.

A job submitted without JobGroup.to_batch_parameters() runs perfectly and
produces no canonical record, no state pointer, and no output-index entry --
nothing to query, and no error to notice. JobMonitorFunction's per-queue
catch-all EventBridge rule routes those jobs here, where they become a
CloudWatch metric and a structured log line; the same rule also delivers the
raw event to the untracked queue so the submission can be recovered and
replayed once the caller is fixed.

The metric is published as an Embedded Metric Format record on stdout rather
than through a PutMetricData call: no extra dependency, no IAM grant, no
synchronous CloudWatch call on the monitor's hot path.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from batch_event_job_monitor.job_details import JobDetails

DEFAULT_METRIC_NAMESPACE = "BatchEventJobMonitor"
UNTRACKED_JOBS_METRIC = "UntrackedJobs"

# EMF dimension values may not be empty, and a job queue is only absent from
# a job state-change event if AWS changes the event shape.
_UNKNOWN_JOB_QUEUE = "unknown"


def _job_queue_name(job_queue_arn: str | None) -> str:
    """Short queue name from a Batch job queue ARN, for the metric dimension."""
    if not job_queue_arn:
        return _UNKNOWN_JOB_QUEUE
    return job_queue_arn.rpartition("/")[2] or job_queue_arn


def untracked_job_metric_record(
    job: JobDetails,
    *,
    namespace: str = DEFAULT_METRIC_NAMESPACE,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Build the Embedded Metric Format record for one untracked job.

    Parameters
    ----------
    job : JobDetails
        The untracked job's state-change event detail.
    namespace : str, optional
        CloudWatch namespace to publish under. Defaults to
        DEFAULT_METRIC_NAMESPACE.
    now : Callable[[], datetime] or None, optional
        Callable returning the current time, used for the EMF timestamp.
        Defaults to `datetime.now(timezone.utc)`.

    Returns
    -------
    dict[str, Any]
        An EMF record carrying the UntrackedJobs count, dimensioned by job
        queue name, alongside the job's identifying fields as properties.
    """
    current_time = now or (lambda: datetime.now(timezone.utc))
    return {
        "_aws": {
            "Timestamp": int(current_time().timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [["JobQueue"]],
                    "Metrics": [{"Name": UNTRACKED_JOBS_METRIC, "Unit": "Count"}],
                }
            ],
        },
        "JobQueue": _job_queue_name(job.job_queue),
        UNTRACKED_JOBS_METRIC: 1,
        "message": (
            f"Batch job {job.job_id} ran on a monitored job queue without the "
            "bejm_* monitoring parameters and is invisible to job monitoring. "
            "Set JobGroup.to_batch_parameters() on SubmitJobRequest.parameters."
        ),
        "jobId": job.job_id,
        "jobName": job.job_name,
        "jobQueueArn": job.job_queue,
        "jobDefinition": job.job_definition,
        "status": job.status,
        "exitCode": job.exit_code,
    }


def record_untracked_job(
    job: JobDetails,
    *,
    namespace: str = DEFAULT_METRIC_NAMESPACE,
    now: Callable[[], datetime] | None = None,
    emit: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Emit the UntrackedJobs metric and log line for one untracked job.

    Parameters
    ----------
    job : JobDetails
        The untracked job's state-change event detail.
    namespace : str, optional
        CloudWatch namespace to publish under. Defaults to
        DEFAULT_METRIC_NAMESPACE.
    now : Callable[[], datetime] or None, optional
        Callable returning the current time, used for the EMF timestamp.
    emit : Callable[[str], None], optional
        Where the serialized record is written. Defaults to `print`, which
        is what puts it on stdout for the CloudWatch Logs EMF parser.

    Returns
    -------
    dict[str, Any]
        The record that was emitted.
    """
    record = untracked_job_metric_record(job, namespace=namespace, now=now)
    emit(json.dumps(record))
    return record
