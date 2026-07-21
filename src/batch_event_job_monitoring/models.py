from __future__ import annotations

import enum
from dataclasses import asdict, dataclass
from typing import Any


class ProcessingState(str, enum.Enum):
    """State of a processing job."""

    SUBMITTED = "SUBMITTED"
    AWAITING = "AWAITING"
    SUCCESS = "SUCCESS"
    FAILURE_RETRYABLE = "FAILURE_RETRYABLE"
    FAILURE_NONRETRYABLE = "FAILURE_NONRETRYABLE"


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for job retry behavior."""

    max_attempts: int = 3
    spot_interruption_status_reason_prefixes: tuple[str, ...] = ("Host EC2",)


def is_terminal(
    state: ProcessingState, attempt: int, retry_policy: RetryPolicy
) -> bool:
    """Determine if a processing state is terminal.

    Parameters
    ----------
    state : ProcessingState
        The current processing state.
    attempt : int
        The current attempt number.
    retry_policy : RetryPolicy
        The retry policy configuration.

    Returns
    -------
    bool
        True if the state is terminal, False otherwise.
    """
    if state in (ProcessingState.SUCCESS, ProcessingState.FAILURE_NONRETRYABLE):
        return True
    if state == ProcessingState.FAILURE_RETRYABLE:
        return attempt >= retry_policy.max_attempts
    return False


@dataclass
class ProcessingEventRecord:
    """Record of a processing event."""

    state: str
    timestamp: str
    batch_job_id: str | None = None
    exit_code: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary, dropping None-valued fields.

        Returns
        -------
        dict[str, Any]
            Dictionary representation with None fields excluded.
        """
        result: dict[str, Any] = {}
        for key, value in asdict(self).items():
            if value is not None:
                result[key] = value
        return result
