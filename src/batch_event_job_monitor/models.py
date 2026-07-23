from __future__ import annotations

import enum
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any


class _Kind(enum.Enum):
    """Internal lifecycle/routing archetype a ProcessingState behaves as.

    Not exposed publicly -- consumers describe custom terminal outcomes via
    ExitCodeOutcome.retryable/.dlq, never by constructing a ProcessingState
    or picking a _Kind directly.
    """

    SUBMITTED = enum.auto()
    AWAITING = enum.auto()
    SUCCESS = enum.auto()
    RETRYABLE_FAILURE = enum.auto()
    NONRETRYABLE_FAILURE = enum.auto()


@dataclass(frozen=True)
class ProcessingState:
    """A job's processing state.

    Not a closed enum. SUBMITTED/AWAITING/SUCCESS/FAILURE_RETRYABLE/
    FAILURE_NONRETRYABLE are provided by ProcessingStates below as the
    built-in lifecycle, but a job_type's ExitCodeOutcomes can produce
    additional named terminal states (e.g. "CLOUDY") that behave like
    FAILURE_RETRYABLE or FAILURE_NONRETRYABLE for routing/terminality
    purposes while using their own name in the S3 key and canonical
    record, the same way SUCCESS/FAILURE_RETRYABLE/FAILURE_NONRETRYABLE do.

    This name-carries-meaning design is why job classification stays
    data-driven (see ExitCodeOutcome) rather than requiring a Python
    callback: a new terminal outcome is just a new entry in a job_type's
    exit-code table, not a new ProcessingState member to ship a code change
    for.
    """

    name: str
    kind: _Kind
    dlq: bool = True

    @property
    def retryable(self) -> bool:
        """True if this state's terminality depends on attempt exhaustion."""
        return self.kind is _Kind.RETRYABLE_FAILURE

    @property
    def rank(self) -> int:
        """Lifecycle rank, for detecting out-of-order/stale transitions."""
        if self.kind is _Kind.SUBMITTED:
            return 0
        if self.kind is _Kind.AWAITING:
            return 1
        return 2

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
        if self.kind in (_Kind.SUBMITTED, _Kind.AWAITING):
            return False
        if self.kind is _Kind.RETRYABLE_FAILURE:
            return attempt >= retry_policy.max_attempts
        return True


class ProcessingStates:
    """Registry of the built-in ProcessingState lifecycle members.

    Kept separate from ProcessingState itself so that class stays a plain
    value type -- the type flowing through classify()/find_state_pointer()/
    etc, including job_type-specific custom states that never appear here.
    """

    SUBMITTED = ProcessingState(name="SUBMITTED", kind=_Kind.SUBMITTED)
    AWAITING = ProcessingState(name="AWAITING", kind=_Kind.AWAITING)
    SUCCESS = ProcessingState(name="SUCCESS", kind=_Kind.SUCCESS)
    FAILURE_RETRYABLE = ProcessingState(
        name="FAILURE_RETRYABLE", kind=_Kind.RETRYABLE_FAILURE
    )
    FAILURE_NONRETRYABLE = ProcessingState(
        name="FAILURE_NONRETRYABLE", kind=_Kind.NONRETRYABLE_FAILURE
    )


# The bounded set of states a job_type with no custom ExitCodeOutcomes can
# produce. S3RecordStore's lookup methods scan over this by default; pass
# ExitCodeOutcomes.states() instead for a job_type that declares custom
# terminal outcomes, so pointers in those states are found too.
BASELINE_PROCESSING_STATES: tuple[ProcessingState, ...] = (
    ProcessingStates.SUBMITTED,
    ProcessingStates.AWAITING,
    ProcessingStates.SUCCESS,
    ProcessingStates.FAILURE_RETRYABLE,
    ProcessingStates.FAILURE_NONRETRYABLE,
)


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

    `name` becomes this outcome's ProcessingState name -- used directly in
    the state-pointer/output-index keys and the canonical record, the same
    way SUCCESS/FAILURE_RETRYABLE/FAILURE_NONRETRYABLE are for the
    built-in states.

    `retryable` determines whether this outcome's terminality is
    exhaustion-gated (like FAILURE_RETRYABLE) or immediate (like
    FAILURE_NONRETRYABLE).

    `dlq` determines whether a terminal instance of this outcome should
    route to the dead-letter queue.
    """

    name: str
    retryable: bool = False
    dlq: bool = True

    def to_processing_state(self) -> ProcessingState:
        """This outcome as a ProcessingState."""
        kind = _Kind.RETRYABLE_FAILURE if self.retryable else _Kind.NONRETRYABLE_FAILURE
        return ProcessingState(name=self.name, kind=kind, dlq=self.dlq)


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

    def states(self) -> tuple[ProcessingState, ...]:
        """The bounded set of ProcessingStates this job_type can produce.

        Baseline lifecycle states plus one ProcessingState per distinct
        declared outcome name (exit codes sharing a name collapse to one
        state). Pass to S3RecordStore's lookup methods so they scan for a
        job_type's full set of possible pointers, not just the five
        built-in states.
        """
        declared: dict[str, ProcessingState] = {}
        for outcome in self.by_exit_code.values():
            declared[outcome.name] = outcome.to_processing_state()
        return BASELINE_PROCESSING_STATES + tuple(declared.values())

    def to_dict(self) -> dict[str, Any]:
        """Encode for embedding in JobTypeConfig's deploy-time JSON."""
        return {
            str(code): {
                "name": outcome.name,
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
        name: str,
        *,
        retryable: bool = False,
        dlq: bool = True,
    ) -> ExitCodeOutcomesBuilder:
        """Map exit_code to a new ExitCodeOutcome. Returns self for chaining."""
        self.outcomes[exit_code] = ExitCodeOutcome(
            name=name, retryable=retryable, dlq=dlq
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

    Fields are flat (mirroring JobContext's own fields plus batch_job_id
    and state) rather than nesting a `context: JobContext` field, so the
    JSON wire format stays a single flat object instead of nesting
    `context` under its own key.
    """

    job_type: str
    partition_fields: dict[str, str]
    input_entity_id: str
    output_entity_id: str
    attempt: int
    batch_job_id: str
    state: str

    @classmethod
    def from_context(
        cls, context: JobContext, *, batch_job_id: str, state: str
    ) -> RetryMessage:
        """Build a RetryMessage from a JobContext plus the Batch job id and state."""
        return cls(**asdict(context), batch_job_id=batch_job_id, state=state)

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
