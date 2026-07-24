from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from batch_event_job_monitor.models import (
    ExitCodeOutcomes,
    JobGroup,
    ProcessingState,
    ProcessingStates,
    RetryPolicy,
)

if TYPE_CHECKING:
    from mypy_boto3_batch.type_defs import JobDetailTypeDef

_PARAM_PREFIX = "bejm_"
_REQUIRED_PARAM_KEYS = (
    "job_type",
    "input_entity_ids",
    "output_entity_id",
    "partition_fields",
    "attempt",
)

# Non-terminal Batch statuses, collapsed to AWAITING -- the distinction
# between them (queueing vs. actually running) is not tracked.
_AWAITING_STATUSES = frozenset({"PENDING", "RUNNABLE", "STARTING", "RUNNING"})


@dataclass
class JobDetails:
    """Container for parsing an AWS Batch job state change event detail."""

    # the EventBridge "detail" object for an aws.batch job state change event
    raw: dict[str, Any]

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

    def classify(
        self,
        retry_policy: RetryPolicy,
        exit_code_outcomes: ExitCodeOutcomes | None = None,
    ) -> ProcessingState:
        """Classify this job's status.

        Called for every aws.batch job state change event, not only
        terminal ones -- SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING map to
        non-terminal ProcessingStates.

        Parameters
        ----------
        retry_policy : RetryPolicy
            Used for the built-in spot-interruption classification fallback.
        exit_code_outcomes : ExitCodeOutcomes or None, optional
            Deploy-time, job_type-specific exit-code taxonomy (see
            JobTypeConfig) -- resolved by the caller, not read from this
            job's own Batch parameters, since it's container/image-tied
            configuration, not per-job data. Checked first for a FAILED
            status, producing that outcome's own named ProcessingState;
            falls back to the spot-interruption-prefix check when no
            outcome matches this job's exit code.
        """
        if self.status == "SUBMITTED":
            return ProcessingStates.SUBMITTED

        if self.status in _AWAITING_STATUSES:
            return ProcessingStates.AWAITING

        if self.status == "SUCCEEDED":
            return ProcessingStates.SUCCESS

        if self.status == "FAILED":
            outcome = (exit_code_outcomes or ExitCodeOutcomes()).get(self.exit_code)
            if outcome is not None:
                return outcome.to_processing_state()

            status_reason = self.status_reason or ""
            if status_reason.startswith(
                retry_policy.spot_interruption_status_reason_prefixes
            ):
                return ProcessingStates.FAILURE_RETRYABLE
            return ProcessingStates.FAILURE_NONRETRYABLE

        raise ValueError(f"Unrecognized job status: {self.status!r}")

    @property
    def parameters(self) -> dict[str, str]:
        """AWS Batch SubmitJobRequest.parameters echoed back on this job."""
        return self._typed_raw.get("parameters") or {}

    def decode_job_group(self) -> JobGroup:
        """Decode the JobGroup from this job's Batch parameters.

        Raises
        ------
        ValueError
            Listing every missing or malformed reserved parameter at once.
            This decodes external input (the raw EventBridge detail), so
            explicit validation with an actionable message is warranted
            here, unlike this codebase's internal code paths.
        """
        parameters = self.parameters
        stripped = {
            key[len(_PARAM_PREFIX) :]: value
            for key, value in parameters.items()
            if key.startswith(_PARAM_PREFIX)
        }

        missing = [
            f"{_PARAM_PREFIX}{key}"
            for key in _REQUIRED_PARAM_KEYS
            if key not in stripped
        ]
        if missing:
            raise ValueError(
                f"Batch job {self.job_id!r} is missing required monitoring "
                f"parameters: {', '.join(missing)}. Set these via "
                "JobGroup.to_batch_parameters() on SubmitJobRequest.parameters "
                "when submitting jobs monitored by JobMonitorFunction."
            )

        try:
            attempt = int(stripped["attempt"])
        except ValueError as exc:
            raise ValueError(
                f"Batch job {self.job_id!r}: {_PARAM_PREFIX}attempt is not an "
                f"integer: {stripped['attempt']!r}"
            ) from exc

        try:
            partition_fields = json.loads(stripped["partition_fields"])
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Batch job {self.job_id!r}: {_PARAM_PREFIX}partition_fields is "
                "not valid JSON"
            ) from exc
        if not isinstance(partition_fields, dict):
            raise ValueError(
                f"Batch job {self.job_id!r}: {_PARAM_PREFIX}partition_fields must "
                f"decode to an object, got {type(partition_fields).__name__}"
            )

        try:
            input_entity_ids = json.loads(stripped["input_entity_ids"])
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Batch job {self.job_id!r}: {_PARAM_PREFIX}input_entity_ids is "
                "not valid JSON"
            ) from exc
        if not (
            isinstance(input_entity_ids, list)
            and input_entity_ids
            and all(isinstance(entity_id, str) for entity_id in input_entity_ids)
        ):
            raise ValueError(
                f"Batch job {self.job_id!r}: {_PARAM_PREFIX}input_entity_ids must "
                "decode to a non-empty array of strings"
            )

        return JobGroup(
            job_type=stripped["job_type"],
            partition_fields=partition_fields,
            input_entity_ids=input_entity_ids,
            output_entity_id=stripped["output_entity_id"],
            attempt=attempt,
        )
