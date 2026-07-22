from __future__ import annotations

import enum
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any

# Ranks a same-attempt transition against the previously recorded state, so
# an out-of-order/stale event (EventBridge does not guarantee delivery
# order) can be detected: a new state ranking lower than the recorded one is
# stale. Terminal states tie -- nothing supersedes a terminal outcome within
# the same attempt.
_RANK = {
    "SUBMITTED": 0,
    "AWAITING": 1,
    "SUCCESS": 2,
    "FAILURE_RETRYABLE": 2,
    "FAILURE_NONRETRYABLE": 2,
}


class ProcessingState(str, enum.Enum):
    """State of a processing job."""

    SUBMITTED = "SUBMITTED"
    AWAITING = "AWAITING"
    SUCCESS = "SUCCESS"
    FAILURE_RETRYABLE = "FAILURE_RETRYABLE"
    FAILURE_NONRETRYABLE = "FAILURE_NONRETRYABLE"

    @property
    def rank(self) -> int:
        """Lifecycle rank, for detecting out-of-order/stale transitions."""
        return _RANK[self.value]

    def is_terminal(self, attempt: int, retry_policy: RetryPolicy) -> bool:
        """Determine if this state is terminal.

        Parameters
        ----------
        attempt : int
            The current attempt number.
        retry_policy : RetryPolicy
            The retry policy configuration.

        Returns
        -------
        bool
            True if the state is terminal, False otherwise.
        """
        if self in (ProcessingState.SUCCESS, ProcessingState.FAILURE_NONRETRYABLE):
            return True
        if self == ProcessingState.FAILURE_RETRYABLE:
            return attempt >= retry_policy.max_attempts
        return False


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for job retry behavior."""

    max_attempts: int = 3
    spot_interruption_status_reason_prefixes: tuple[str, ...] = ("Host EC2",)

    def to_dict(self) -> dict[str, Any]:
        """Encode for embedding in JobTypeConfig's deploy-time JSON."""
        return {
            "max_attempts": self.max_attempts,
            "spot_interruption_status_reason_prefixes": list(
                self.spot_interruption_status_reason_prefixes
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetryPolicy:
        """Decode from JobTypeConfig's deploy-time JSON. Missing keys default."""
        kwargs: dict[str, Any] = {}
        if "max_attempts" in data:
            kwargs["max_attempts"] = data["max_attempts"]
        if "spot_interruption_status_reason_prefixes" in data:
            kwargs["spot_interruption_status_reason_prefixes"] = tuple(
                data["spot_interruption_status_reason_prefixes"]
            )
        return cls(**kwargs)


@dataclass(frozen=True)
class Classification:
    """The outcome of classifying a job's status.

    `state` drives all routing/storage mechanics (rank, terminality, S3 key
    building) and is always one of the fixed ProcessingState members.

    `label` and `dlq` carry consumer-defined, open-ended routing detail
    (e.g. distinguishing a data-driven "skip" outcome like a cloud-cover
    screen from a genuine bug) without requiring ProcessingState itself to
    grow new members per job type. See ExitCodeOutcome.
    """

    state: ProcessingState
    label: str | None = None
    dlq: bool = True


_PARAM_PREFIX = "bejm_"


@dataclass(frozen=True)
class JobContext:
    """Identifying and partitioning fields for a job processing event."""

    job_type: str
    partition_fields: dict[str, str]
    input_entity_id: str
    output_entity_id: str
    attempt: int

    @classmethod
    def new(
        cls,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        input_entity_id: str,
        output_entity_id: str,
    ) -> JobContext:
        """Build a JobContext for a brand-new entity, at attempt 1."""
        return cls(
            job_type=job_type,
            partition_fields=partition_fields,
            input_entity_id=input_entity_id,
            output_entity_id=output_entity_id,
            attempt=1,
        )

    def next_attempt(self) -> JobContext:
        """This context's entity, advanced to its next attempt."""
        return replace(self, attempt=self.attempt + 1)

    def to_batch_parameters(self) -> dict[str, str]:
        """Encode this context as AWS Batch SubmitJobRequest.parameters.

        Consumers merge these into their own `parameters` dict when calling
        `submit_job` (directly, or via `resubmit_job`) so that
        JobMonitorFunction's bundled handler can reconstruct the
        JobContext from the EventBridge job state-change event with no
        consumer-authored Lambda code.

        AWS Batch echoes `parameters` back into the event `detail` for
        every state change of that job's life.
        """
        return {
            f"{_PARAM_PREFIX}job_type": self.job_type,
            f"{_PARAM_PREFIX}input_entity_id": self.input_entity_id,
            f"{_PARAM_PREFIX}output_entity_id": self.output_entity_id,
            f"{_PARAM_PREFIX}partition_fields": json.dumps(
                self.partition_fields, sort_keys=True
            ),
            f"{_PARAM_PREFIX}attempt": str(self.attempt),
        }


@dataclass(frozen=True)
class ExitCodeOutcome:
    """A named outcome for one Batch container exit code.

    `retryable` determines which ProcessingState this outcome classifies
    into (FAILURE_RETRYABLE vs FAILURE_NONRETRYABLE).

    `dlq` determines whether a terminal instance of this outcome should
    route to the dead-letter queue.

    `label` is descriptive only -- it never affects routing, only the
    output-index key and canonical event.
    """

    label: str
    retryable: bool = False
    dlq: bool = True


@dataclass(frozen=True)
class ExitCodeOutcomes:
    """Exit-code -> ExitCodeOutcome mapping.

    Deploy-time, per-job_type configuration (tied to a container image's
    exit-code taxonomy, not to any individual job) -- carried via
    JobTypeConfig, not per-job Batch parameters, so a mapping change never
    needs to be duplicated across every job of that type.
    """

    by_exit_code: dict[int, ExitCodeOutcome] = field(default_factory=dict)

    def get(self, exit_code: int | None) -> ExitCodeOutcome | None:
        """The outcome for exit_code, or None if unmapped or exit_code is None."""
        return None if exit_code is None else self.by_exit_code.get(exit_code)

    def to_dict(self) -> dict[str, Any]:
        """Encode for embedding in JobTypeConfig's deploy-time JSON."""
        return {
            str(code): {
                "label": outcome.label,
                "retryable": outcome.retryable,
                "dlq": outcome.dlq,
            }
            for code, outcome in self.by_exit_code.items()
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExitCodeOutcomes:
        """Decode from JobTypeConfig's deploy-time JSON."""
        return cls(
            {int(code): ExitCodeOutcome(**fields) for code, fields in data.items()}
        )


@dataclass
class ExitCodeOutcomesBuilder:
    """Fluent builder for ExitCodeOutcomes.

    >>> outcomes = (
    ...     ExitCodeOutcomesBuilder()
    ...     .add(3, "LOW_SUN_ANGLE", dlq=False)
    ...     .add(4, "CLOUDY", dlq=False)
    ...     .build()
    ... )
    """

    outcomes: dict[int, ExitCodeOutcome] = field(default_factory=dict)

    def add(
        self,
        exit_code: int,
        label: str,
        *,
        retryable: bool = False,
        dlq: bool = True,
    ) -> ExitCodeOutcomesBuilder:
        """Map exit_code to a new ExitCodeOutcome. Returns self for chaining."""
        self.outcomes[exit_code] = ExitCodeOutcome(
            label=label, retryable=retryable, dlq=dlq
        )
        return self

    def build(self) -> ExitCodeOutcomes:
        """Build the immutable ExitCodeOutcomes mapping."""
        return ExitCodeOutcomes(dict(self.outcomes))


@dataclass(frozen=True)
class JobTypeConfig:
    """Deploy-time classification config for one job_type.

    Bundles retry_policy and exit_code_outcomes since both are tied to a
    container image/tag -- a redeploy is required to change either.

    Both are genuinely job_type-specific: some job types are flakier than
    others (e.g. those calling external services), and exit-code meaning is
    always container-specific.

    JobMonitorFunction resolves one JobTypeConfig per job_type from a
    single deploy-time env var; there is no per-job or per-invocation
    override.
    """

    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    exit_code_outcomes: ExitCodeOutcomes = field(default_factory=ExitCodeOutcomes)

    def to_dict(self) -> dict[str, Any]:
        """Encode for embedding in JobMonitorFunction's deploy-time JSON."""
        return {
            "retry_policy": self.retry_policy.to_dict(),
            "exit_code_outcomes": self.exit_code_outcomes.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobTypeConfig:
        """Decode from JobMonitorFunction's deploy-time JSON. Missing keys default."""
        return cls(
            retry_policy=RetryPolicy.from_dict(data.get("retry_policy", {})),
            exit_code_outcomes=ExitCodeOutcomes.from_dict(
                data.get("exit_code_outcomes", {})
            ),
        )


@dataclass(frozen=True)
class RetryMessage:
    """SQS message body for the retry queue / DLQ.

    Fields are flat (mirroring JobContext's own fields plus batch_job_id)
    rather than nesting a `context: JobContext` field, so the JSON wire
    format stays a single flat object instead of nesting `context` under
    its own key.
    """

    job_type: str
    partition_fields: dict[str, str]
    input_entity_id: str
    output_entity_id: str
    attempt: int
    batch_job_id: str
    label: str | None = None

    @classmethod
    def from_context(
        cls, context: JobContext, *, batch_job_id: str, label: str | None = None
    ) -> RetryMessage:
        """Build a RetryMessage from a JobContext plus the Batch job id."""
        return cls(**asdict(context), batch_job_id=batch_job_id, label=label)

    @property
    def context(self) -> JobContext:
        """The JobContext this message was built from."""
        return JobContext(
            job_type=self.job_type,
            partition_fields=self.partition_fields,
            input_entity_id=self.input_entity_id,
            output_entity_id=self.output_entity_id,
            attempt=self.attempt,
        )

    def to_json(self) -> str:
        """Serialize to the SQS message body JSON string."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, body: str) -> RetryMessage:
        """Parse an SQS message body JSON string into a RetryMessage."""
        return cls(**json.loads(body))


@dataclass
class ProcessingEventRecord:
    """Record of a processing event."""

    state: str
    timestamp: str
    batch_job_id: str | None = None
    exit_code: int | None = None
    label: str | None = None

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
