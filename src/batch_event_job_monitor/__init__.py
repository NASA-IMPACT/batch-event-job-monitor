from importlib.metadata import PackageNotFoundError, version

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.lambda_core import monitor_job
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    Classification,
    ExitCodeOutcome,
    ExitCodeOutcomes,
    ExitCodeOutcomesBuilder,
    JobContext,
    JobTypeConfig,
    ProcessingEventRecord,
    ProcessingState,
    RetryMessage,
    RetryPolicy,
)
from batch_event_job_monitor.submission import resubmit_job, submit_job

try:
    __version__ = version("batch-event-job-monitor")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "Classification",
    "ExitCodeOutcome",
    "ExitCodeOutcomes",
    "ExitCodeOutcomesBuilder",
    "JobContext",
    "JobDetails",
    "JobTypeConfig",
    "ProcessingEventRecord",
    "ProcessingState",
    "RetryMessage",
    "RetryPolicy",
    "S3RecordStore",
    "monitor_job",
    "resubmit_job",
    "submit_job",
]
