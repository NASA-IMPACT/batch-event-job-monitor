from importlib.metadata import PackageNotFoundError, version

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.lambda_core import monitor_job
from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    PARAM_PREFIX,
    ExitCodeOutcome,
    ExitCodeOutcomes,
    ExitCodeOutcomesBuilder,
    JobContext,
    JobGroup,
    JobTypeConfig,
    ProcessingEventRecord,
    ProcessingState,
    ProcessingStates,
    RetryMessage,
    RetryPolicy,
)
from batch_event_job_monitor.submission import resubmit_job, submit_job
from batch_event_job_monitor.untracked import (
    DEFAULT_METRIC_NAMESPACE,
    UNTRACKED_JOBS_METRIC,
    record_untracked_job,
    untracked_job_metric_record,
)

try:
    __version__ = version("batch-event-job-monitor")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "DEFAULT_METRIC_NAMESPACE",
    "PARAM_PREFIX",
    "UNTRACKED_JOBS_METRIC",
    "ExitCodeOutcome",
    "ExitCodeOutcomes",
    "ExitCodeOutcomesBuilder",
    "JobContext",
    "JobDetails",
    "JobGroup",
    "JobTypeConfig",
    "ProcessingEventRecord",
    "ProcessingState",
    "ProcessingStates",
    "RetryMessage",
    "RetryPolicy",
    "S3RecordStore",
    "monitor_job",
    "record_untracked_job",
    "resubmit_job",
    "submit_job",
    "untracked_job_metric_record",
]
