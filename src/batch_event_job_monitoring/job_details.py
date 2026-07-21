from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from batch_event_job_monitoring.models import ProcessingState, RetryPolicy

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import JobDetailTypeDef


@dataclass
class JobDetails:
    """Container for parsing an AWS Batch job state change event detail."""

    raw: dict[str, Any]  # the EventBridge "detail" object for an aws.batch job state change event

    @classmethod
    def from_event(cls, detail: dict[str, Any]) -> JobDetails:
        """Build a JobDetails from an EventBridge job state change event detail."""
        return cls(raw=detail)

    @property
    def _typed_raw(self) -> JobDetailTypeDef:
        return self.raw  # type: ignore[return-value]

    @property
    def job_id(self) -> str:
        """AWS Batch job identifier."""
        return self._typed_raw["jobId"]

    @property
    def job_name(self) -> str:
        """AWS Batch job name."""
        return self._typed_raw["jobName"]

    @property
    def status(self) -> str:
        """Terminal job status, e.g. "SUCCEEDED" or "FAILED"."""
        return self._typed_raw["status"]

    @property
    def exit_code(self) -> int | None:
        """Container exit code, checking the top level then the last attempt."""
        container = self._typed_raw.get("container")
        if container is not None:
            exit_code = container.get("exitCode")
            if exit_code is not None:
                return exit_code

        attempts = self._typed_raw.get("attempts")
        if attempts:
            last_container = attempts[-1].get("container")
            if last_container is not None:
                return last_container.get("exitCode")

        return None

    @property
    def status_reason(self) -> str | None:
        """Status reason, checking the top level then the last attempt."""
        status_reason = self._typed_raw.get("statusReason")
        if status_reason is not None:
            return status_reason

        attempts = self._typed_raw.get("attempts")
        if attempts:
            return attempts[-1].get("statusReason")

        return None

    def classify(self, retry_policy: RetryPolicy) -> ProcessingState:
        """Classify this job's outcome into a ProcessingState.

        The Lambda that calls this is only ever invoked for terminal
        SUCCEEDED/FAILED job state change events, so any other status
        indicates a wiring bug.
        """
        if self.status == "SUCCEEDED":
            return ProcessingState.SUCCESS

        if self.status == "FAILED":
            status_reason = self.status_reason or ""
            if status_reason.startswith(
                retry_policy.spot_interruption_status_reason_prefixes
            ):
                return ProcessingState.FAILURE_RETRYABLE
            return ProcessingState.FAILURE_NONRETRYABLE

        raise ValueError(f"Unexpected non-terminal job status: {self.status!r}")
