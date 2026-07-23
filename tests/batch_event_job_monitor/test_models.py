import json

import pytest

from batch_event_job_monitor.models import (
    ExitCodeOutcome,
    ExitCodeOutcomes,
    ExitCodeOutcomesBuilder,
    JobContext,
    JobTypeConfig,
    ProcessingEventRecord,
    ProcessingStates,
    RetryMessage,
    RetryPolicy,
)


class TestProcessingState:
    """Tests for the built-in ProcessingState instances."""

    def test_processing_state_members(self) -> None:
        """Test that all built-in states exist."""
        assert hasattr(ProcessingStates, "SUBMITTED")
        assert hasattr(ProcessingStates, "AWAITING")
        assert hasattr(ProcessingStates, "SUCCESS")
        assert hasattr(ProcessingStates, "FAILURE_RETRYABLE")
        assert hasattr(ProcessingStates, "FAILURE_NONRETRYABLE")

    def test_processing_state_names(self) -> None:
        """Test that built-in ProcessingState names are correct strings."""
        assert ProcessingStates.SUBMITTED.name == "SUBMITTED"
        assert ProcessingStates.AWAITING.name == "AWAITING"
        assert ProcessingStates.SUCCESS.name == "SUCCESS"
        assert ProcessingStates.FAILURE_RETRYABLE.name == "FAILURE_RETRYABLE"
        assert ProcessingStates.FAILURE_NONRETRYABLE.name == "FAILURE_NONRETRYABLE"

    def test_only_failure_retryable_is_retryable(self) -> None:
        assert ProcessingStates.FAILURE_RETRYABLE.retryable is True
        assert ProcessingStates.SUBMITTED.retryable is False
        assert ProcessingStates.AWAITING.retryable is False
        assert ProcessingStates.SUCCESS.retryable is False
        assert ProcessingStates.FAILURE_NONRETRYABLE.retryable is False

    def test_custom_state_from_outcome_is_not_baseline(self) -> None:
        """A custom outcome-derived state has its own name, not a baseline one."""
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        assert cloudy.name == "CLOUDY"
        assert cloudy != ProcessingStates.FAILURE_NONRETRYABLE


class TestRank:
    """Tests for ProcessingState.rank."""

    def test_submitted_ranks_below_awaiting(self) -> None:
        assert ProcessingStates.SUBMITTED.rank < ProcessingStates.AWAITING.rank

    def test_awaiting_ranks_below_terminal_states(self) -> None:
        assert ProcessingStates.AWAITING.rank < ProcessingStates.SUCCESS.rank
        assert ProcessingStates.AWAITING.rank < ProcessingStates.FAILURE_RETRYABLE.rank
        assert (
            ProcessingStates.AWAITING.rank < ProcessingStates.FAILURE_NONRETRYABLE.rank
        )

    def test_terminal_states_tie(self) -> None:
        assert (
            ProcessingStates.SUCCESS.rank
            == ProcessingStates.FAILURE_RETRYABLE.rank
            == ProcessingStates.FAILURE_NONRETRYABLE.rank
        )

    def test_custom_terminal_state_ranks_like_builtin_terminal(self) -> None:
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        assert cloudy.rank == ProcessingStates.FAILURE_NONRETRYABLE.rank


class TestRetryPolicy:
    """Tests for RetryPolicy dataclass."""

    def test_retry_policy_defaults(self) -> None:
        """Test RetryPolicy default values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.spot_interruption_status_reason_prefixes == ("Host EC2",)

    def test_retry_policy_custom_values(self) -> None:
        """Test RetryPolicy with custom values."""
        prefixes = ("Host EC2", "Custom")
        policy = RetryPolicy(
            max_attempts=5, spot_interruption_status_reason_prefixes=prefixes
        )
        assert policy.max_attempts == 5
        assert policy.spot_interruption_status_reason_prefixes == prefixes

    def test_retry_policy_frozen(self) -> None:
        """Test that RetryPolicy is frozen (immutable)."""
        policy = RetryPolicy()
        with pytest.raises(AttributeError):
            policy.max_attempts = 5  # type: ignore[misc]

    def test_to_dict_from_dict_round_trips(self) -> None:
        policy = RetryPolicy(
            max_attempts=5, spot_interruption_status_reason_prefixes=("Custom",)
        )
        assert RetryPolicy.from_dict(policy.to_dict()) == policy

    def test_from_dict_missing_keys_uses_defaults(self) -> None:
        assert RetryPolicy.from_dict({}) == RetryPolicy()

    def test_to_dict_is_json_serializable(self) -> None:
        policy = RetryPolicy()
        assert json.loads(json.dumps(policy.to_dict())) == policy.to_dict()


class TestIsTerminal:
    """Tests for ProcessingState.is_terminal."""

    def test_success_is_always_terminal(self) -> None:
        """SUCCESS is always terminal regardless of attempt count."""
        policy = RetryPolicy(max_attempts=3)
        assert ProcessingStates.SUCCESS.is_terminal(1, policy)
        assert ProcessingStates.SUCCESS.is_terminal(2, policy)
        assert ProcessingStates.SUCCESS.is_terminal(3, policy)

    def test_failure_nonretryable_is_always_terminal(self) -> None:
        """FAILURE_NONRETRYABLE is always terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert ProcessingStates.FAILURE_NONRETRYABLE.is_terminal(1, policy)
        assert ProcessingStates.FAILURE_NONRETRYABLE.is_terminal(2, policy)
        assert ProcessingStates.FAILURE_NONRETRYABLE.is_terminal(3, policy)

    def test_failure_retryable_not_terminal_below_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is not terminal when attempt < max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert not ProcessingStates.FAILURE_RETRYABLE.is_terminal(1, policy)
        assert not ProcessingStates.FAILURE_RETRYABLE.is_terminal(2, policy)

    def test_failure_retryable_terminal_at_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is terminal when attempt >= max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert ProcessingStates.FAILURE_RETRYABLE.is_terminal(3, policy)

    def test_failure_retryable_terminal_above_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is terminal when attempt > max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert ProcessingStates.FAILURE_RETRYABLE.is_terminal(4, policy)

    def test_submitted_not_terminal(self) -> None:
        """SUBMITTED is never terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert not ProcessingStates.SUBMITTED.is_terminal(1, policy)
        assert not ProcessingStates.SUBMITTED.is_terminal(3, policy)

    def test_awaiting_not_terminal(self) -> None:
        """AWAITING is never terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert not ProcessingStates.AWAITING.is_terminal(1, policy)
        assert not ProcessingStates.AWAITING.is_terminal(3, policy)

    def test_is_terminal_with_different_max_attempts(self) -> None:
        """Test is_terminal with different max_attempts values."""
        policy_2 = RetryPolicy(max_attempts=2)
        policy_5 = RetryPolicy(max_attempts=5)

        assert not ProcessingStates.FAILURE_RETRYABLE.is_terminal(1, policy_2)
        assert ProcessingStates.FAILURE_RETRYABLE.is_terminal(2, policy_2)

        assert not ProcessingStates.FAILURE_RETRYABLE.is_terminal(4, policy_5)
        assert ProcessingStates.FAILURE_RETRYABLE.is_terminal(5, policy_5)

    def test_custom_nonretryable_outcome_always_terminal(self) -> None:
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        assert cloudy.is_terminal(1, RetryPolicy(max_attempts=3))

    def test_custom_retryable_outcome_exhaustion_gated(self) -> None:
        transient = ExitCodeOutcome(
            name="TRANSIENT", dlq=True, retryable=True
        ).to_processing_state()
        policy = RetryPolicy(max_attempts=3)
        assert not transient.is_terminal(1, policy)
        assert transient.is_terminal(3, policy)


class TestProcessingEventRecord:
    """Tests for ProcessingEventRecord dataclass."""

    def test_processing_event_record_required_fields(self) -> None:
        """Test ProcessingEventRecord with required fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            timestamp="2024-01-01T00:00:00Z",
        )
        assert record.state == "SUCCESS"
        assert record.timestamp == "2024-01-01T00:00:00Z"
        assert record.batch_job_id is None
        assert record.exit_code is None

    def test_processing_event_record_all_fields(self) -> None:
        """Test ProcessingEventRecord with all fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            timestamp="2024-01-01T00:00:00Z",
            batch_job_id="batch-123",
            exit_code=0,
        )
        assert record.state == "SUCCESS"
        assert record.timestamp == "2024-01-01T00:00:00Z"
        assert record.batch_job_id == "batch-123"
        assert record.exit_code == 0

    def test_to_dict_drops_none_fields(self) -> None:
        """Test that to_dict() drops fields with None values."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            timestamp="2024-01-01T00:00:00Z",
            batch_job_id=None,
            exit_code=None,
        )
        result = record.to_dict()
        assert result == {
            "state": "SUCCESS",
            "timestamp": "2024-01-01T00:00:00Z",
        }
        assert "batch_job_id" not in result
        assert "exit_code" not in result

    def test_to_dict_includes_non_none_fields(self) -> None:
        """Test that to_dict() includes fields with non-None values."""
        record = ProcessingEventRecord(
            state="FAILURE_RETRYABLE",
            timestamp="2024-01-01T00:00:00Z",
            batch_job_id="batch-456",
            exit_code=1,
        )
        result = record.to_dict()
        assert result == {
            "state": "FAILURE_RETRYABLE",
            "timestamp": "2024-01-01T00:00:00Z",
            "batch_job_id": "batch-456",
            "exit_code": 1,
        }

    def test_to_dict_partial_none_fields(self) -> None:
        """Test to_dict() with some None and some non-None fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            timestamp="2024-01-01T00:00:00Z",
            batch_job_id="batch-789",
            exit_code=None,
        )
        result = record.to_dict()
        assert result == {
            "state": "SUCCESS",
            "timestamp": "2024-01-01T00:00:00Z",
            "batch_job_id": "batch-789",
        }
        assert "exit_code" not in result

    def test_to_dict_returns_dict_type(self) -> None:
        """Test that to_dict() returns a dict."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            timestamp="2024-01-01T00:00:00Z",
        )
        result = record.to_dict()
        assert isinstance(result, dict)


class TestJobContext:
    """Tests for JobContext.to_batch_parameters."""

    def test_new_defaults_to_attempt_one(self) -> None:
        context = JobContext.new(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
        )
        assert context.attempt == 1

    def test_next_attempt_increments(self) -> None:
        context = JobContext(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
            attempt=2,
        )
        assert context.next_attempt().attempt == 3

    def test_next_attempt_preserves_other_fields(self) -> None:
        context = JobContext(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
            attempt=1,
        )
        next_context = context.next_attempt()
        assert next_context.job_type == context.job_type
        assert next_context.partition_fields == context.partition_fields
        assert next_context.input_entity_id == context.input_entity_id
        assert next_context.output_entity_id == context.output_entity_id

    def test_encodes_all_identity_fields(self) -> None:
        context = JobContext(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK", "year_month": "2024-06"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
            attempt=2,
        )
        params = context.to_batch_parameters()
        assert params == {
            "bejm_job_type": "monthly-composite",
            "bejm_input_entity_id": "12TVK_2024-06_source",
            "bejm_output_entity_id": "12TVK_2024-06_output",
            "bejm_partition_fields": json.dumps(
                {"tile_id": "12TVK", "year_month": "2024-06"}, sort_keys=True
            ),
            "bejm_attempt": "2",
        }

    def test_partition_fields_round_trips_through_json(self) -> None:
        context = JobContext(
            job_type="job",
            partition_fields={"b": "2", "a": "1"},
            input_entity_id="e",
            output_entity_id="o",
            attempt=1,
        )
        params = context.to_batch_parameters()
        assert json.loads(params["bejm_partition_fields"]) == {"a": "1", "b": "2"}


class TestRetryMessage:
    """Tests for RetryMessage."""

    def _context(self) -> JobContext:
        return JobContext(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
            attempt=1,
        )

    def test_from_context_round_trips_to_context(self) -> None:
        context = self._context()
        message = RetryMessage.from_context(
            context, batch_job_id="batch-123", state="FAILURE_RETRYABLE"
        )
        assert message.context == context
        assert message.batch_job_id == "batch-123"

    def test_to_json_is_flat(self) -> None:
        message = RetryMessage.from_context(
            self._context(), batch_job_id="batch-123", state="FAILURE_RETRYABLE"
        )
        body = json.loads(message.to_json())
        assert body == {
            "job_type": "monthly-composite",
            "partition_fields": {"tile_id": "12TVK"},
            "input_entity_id": "12TVK_2024-06_source",
            "output_entity_id": "12TVK_2024-06_output",
            "attempt": 1,
            "batch_job_id": "batch-123",
            "state": "FAILURE_RETRYABLE",
        }

    def test_from_json_round_trips(self) -> None:
        message = RetryMessage.from_context(
            self._context(), batch_job_id="batch-123", state="FAILURE_RETRYABLE"
        )
        assert RetryMessage.from_json(message.to_json()) == message

    def test_state_round_trips(self) -> None:
        message = RetryMessage.from_context(
            self._context(), batch_job_id="batch-123", state="CLOUDY"
        )
        assert message.state == "CLOUDY"
        assert RetryMessage.from_json(message.to_json()).state == "CLOUDY"


class TestExitCodeOutcome:
    def test_retryable_defaults_false(self) -> None:
        outcome = ExitCodeOutcome(name="CLOUDY", dlq=False)
        assert outcome.retryable is False

    def test_dlq_is_required(self) -> None:
        with pytest.raises(TypeError):
            ExitCodeOutcome(name="CLOUDY")  # type: ignore[call-arg]

    def test_to_processing_state_nonretryable(self) -> None:
        state = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        assert state.name == "CLOUDY"
        assert state.retryable is False
        assert state.dlq is False

    def test_to_processing_state_retryable(self) -> None:
        state = ExitCodeOutcome(
            name="TRANSIENT", dlq=True, retryable=True
        ).to_processing_state()
        assert state.name == "TRANSIENT"
        assert state.retryable is True


class TestExitCodeOutcomes:
    def test_get_returns_none_for_unmapped_exit_code(self) -> None:
        outcomes = ExitCodeOutcomes({4: ExitCodeOutcome(name="CLOUDY", dlq=False)})
        assert outcomes.get(1) is None

    def test_get_returns_none_for_none_exit_code(self) -> None:
        outcomes = ExitCodeOutcomes({4: ExitCodeOutcome(name="CLOUDY", dlq=False)})
        assert outcomes.get(None) is None

    def test_get_returns_mapped_outcome(self) -> None:
        outcome = ExitCodeOutcome(name="CLOUDY", dlq=False)
        outcomes = ExitCodeOutcomes({4: outcome})
        assert outcomes.get(4) == outcome

    def test_empty_mapping_round_trips(self) -> None:
        assert ExitCodeOutcomes.from_dict(ExitCodeOutcomes().to_dict()) == (
            ExitCodeOutcomes()
        )

    def test_to_dict_from_dict_round_trips(self) -> None:
        outcomes = ExitCodeOutcomes(
            {
                3: ExitCodeOutcome(name="LOW_SUN_ANGLE", dlq=False),
                4: ExitCodeOutcome(name="CLOUDY", dlq=False),
                42: ExitCodeOutcome(name="TRANSIENT", dlq=True, retryable=True),
            }
        )
        decoded = ExitCodeOutcomes.from_dict(outcomes.to_dict())
        assert decoded == outcomes

    def test_to_dict_is_json_serializable(self) -> None:
        outcomes = ExitCodeOutcomes({4: ExitCodeOutcome(name="CLOUDY", dlq=False)})
        assert json.loads(json.dumps(outcomes.to_dict())) == outcomes.to_dict()

    def test_states_includes_baseline_and_declared(self) -> None:
        outcomes = ExitCodeOutcomes(
            {
                3: ExitCodeOutcome(name="LOW_SUN_ANGLE", dlq=False),
                4: ExitCodeOutcome(name="CLOUDY", dlq=False),
            }
        )
        names = {state.name for state in outcomes.states()}
        assert names == {
            "SUBMITTED",
            "AWAITING",
            "SUCCESS",
            "FAILURE_RETRYABLE",
            "FAILURE_NONRETRYABLE",
            "LOW_SUN_ANGLE",
            "CLOUDY",
        }

    def test_states_dedupes_same_name_across_exit_codes(self) -> None:
        outcomes = ExitCodeOutcomes(
            {
                3: ExitCodeOutcome(name="CLOUDY", dlq=False),
                4: ExitCodeOutcome(name="CLOUDY", dlq=False),
            }
        )
        names = [state.name for state in outcomes.states()]
        assert names.count("CLOUDY") == 1

    def test_states_empty_mapping_is_just_baseline(self) -> None:
        names = {state.name for state in ExitCodeOutcomes().states()}
        assert names == {
            "SUBMITTED",
            "AWAITING",
            "SUCCESS",
            "FAILURE_RETRYABLE",
            "FAILURE_NONRETRYABLE",
        }


class TestExitCodeOutcomesBuilder:
    def test_add_returns_self_for_chaining(self) -> None:
        builder = ExitCodeOutcomesBuilder()
        assert builder.add(3, "LOW_SUN_ANGLE", dlq=False) is builder

    def test_build_produces_expected_mapping(self) -> None:
        outcomes = (
            ExitCodeOutcomesBuilder()
            .add(3, "LOW_SUN_ANGLE", dlq=False)
            .add(4, "CLOUDY", dlq=False)
            .build()
        )
        assert outcomes.get(3) == ExitCodeOutcome(name="LOW_SUN_ANGLE", dlq=False)
        assert outcomes.get(4) == ExitCodeOutcome(name="CLOUDY", dlq=False)

    def test_add_overwrites_same_exit_code(self) -> None:
        outcomes = (
            ExitCodeOutcomesBuilder()
            .add(3, "FIRST", dlq=False)
            .add(3, "SECOND", dlq=False)
            .build()
        )
        assert outcomes.get(3) == ExitCodeOutcome(name="SECOND", dlq=False)


class TestJobTypeConfig:
    def test_defaults(self) -> None:
        config = JobTypeConfig()
        assert config.retry_policy == RetryPolicy()
        assert config.exit_code_outcomes == ExitCodeOutcomes()

    def test_to_dict_from_dict_round_trips(self) -> None:
        config = JobTypeConfig(
            retry_policy=RetryPolicy(max_attempts=5),
            exit_code_outcomes=ExitCodeOutcomes(
                {4: ExitCodeOutcome(name="CLOUDY", dlq=False)}
            ),
        )
        assert JobTypeConfig.from_dict(config.to_dict()) == config

    def test_from_dict_missing_keys_uses_defaults(self) -> None:
        assert JobTypeConfig.from_dict({}) == JobTypeConfig()

    def test_to_dict_is_json_serializable(self) -> None:
        config = JobTypeConfig(
            exit_code_outcomes=ExitCodeOutcomes(
                {4: ExitCodeOutcome(name="CLOUDY", dlq=False)}
            )
        )
        assert json.loads(json.dumps(config.to_dict())) == config.to_dict()
