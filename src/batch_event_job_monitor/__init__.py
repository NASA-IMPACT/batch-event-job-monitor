from importlib.metadata import PackageNotFoundError, version

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.lambda_core import monitor_job
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    JobContext,
    ProcessingEventRecord,
    ProcessingState,
    RetryPolicy,
    is_terminal,
)

try:
    __version__ = version("batch-event-job-monitor")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "JobContext",
    "JobDetails",
    "ProcessingEventRecord",
    "ProcessingState",
    "RetryPolicy",
    "S3RecordStore",
    "is_terminal",
    "monitor_job",
]
