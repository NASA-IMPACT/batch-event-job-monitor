import enum

import pytest

from hls_batch_job_monitoring.models import (
    ProcessingEventRecord,
    ProcessingState,
    RetryPolicy,
    is_terminal,
)


class TestProcessingState:
    """Tests for ProcessingState enum."""

    def test_processing_state_members(self) -> None:
        """Test that all required members exist."""
        assert hasattr(ProcessingState, "SUBMITTED")
        assert hasattr(ProcessingState, "AWAITING")
        assert hasattr(ProcessingState, "SUCCESS")
        assert hasattr(ProcessingState, "FAILURE_RETRYABLE")
        assert hasattr(ProcessingState, "FAILURE_NONRETRYABLE")

    def test_processing_state_is_string_enum(self) -> None:
        """Test that ProcessingState is a string enum."""
        assert issubclass(ProcessingState, str)
        assert issubclass(ProcessingState, enum.Enum)

    def test_processing_state_values(self) -> None:
        """Test that ProcessingState values are correct strings."""
        assert ProcessingState.SUBMITTED.value == "SUBMITTED"
        assert ProcessingState.AWAITING.value == "AWAITING"
        assert ProcessingState.SUCCESS.value == "SUCCESS"
        assert ProcessingState.FAILURE_RETRYABLE.value == "FAILURE_RETRYABLE"
        assert ProcessingState.FAILURE_NONRETRYABLE.value == "FAILURE_NONRETRYABLE"


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
        policy = RetryPolicy(max_attempts=5, spot_interruption_status_reason_prefixes=prefixes)
        assert policy.max_attempts == 5
        assert policy.spot_interruption_status_reason_prefixes == prefixes

    def test_retry_policy_frozen(self) -> None:
        """Test that RetryPolicy is frozen (immutable)."""
        policy = RetryPolicy()
        with pytest.raises(AttributeError):
            policy.max_attempts = 5


class TestIsTerminal:
    """Tests for is_terminal function."""

    def test_success_is_always_terminal(self) -> None:
        """SUCCESS is always terminal regardless of attempt count."""
        policy = RetryPolicy(max_attempts=3)
        assert is_terminal(ProcessingState.SUCCESS, 1, policy)
        assert is_terminal(ProcessingState.SUCCESS, 2, policy)
        assert is_terminal(ProcessingState.SUCCESS, 3, policy)

    def test_failure_nonretryable_is_always_terminal(self) -> None:
        """FAILURE_NONRETRYABLE is always terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert is_terminal(ProcessingState.FAILURE_NONRETRYABLE, 1, policy)
        assert is_terminal(ProcessingState.FAILURE_NONRETRYABLE, 2, policy)
        assert is_terminal(ProcessingState.FAILURE_NONRETRYABLE, 3, policy)

    def test_failure_retryable_not_terminal_below_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is not terminal when attempt < max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert not is_terminal(ProcessingState.FAILURE_RETRYABLE, 1, policy)
        assert not is_terminal(ProcessingState.FAILURE_RETRYABLE, 2, policy)

    def test_failure_retryable_terminal_at_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is terminal when attempt >= max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert is_terminal(ProcessingState.FAILURE_RETRYABLE, 3, policy)

    def test_failure_retryable_terminal_above_max_attempts(self) -> None:
        """FAILURE_RETRYABLE is terminal when attempt > max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert is_terminal(ProcessingState.FAILURE_RETRYABLE, 4, policy)

    def test_submitted_not_terminal(self) -> None:
        """SUBMITTED is never terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert not is_terminal(ProcessingState.SUBMITTED, 1, policy)
        assert not is_terminal(ProcessingState.SUBMITTED, 3, policy)

    def test_awaiting_not_terminal(self) -> None:
        """AWAITING is never terminal."""
        policy = RetryPolicy(max_attempts=3)
        assert not is_terminal(ProcessingState.AWAITING, 1, policy)
        assert not is_terminal(ProcessingState.AWAITING, 3, policy)

    def test_is_terminal_with_different_max_attempts(self) -> None:
        """Test is_terminal with different max_attempts values."""
        policy_2 = RetryPolicy(max_attempts=2)
        policy_5 = RetryPolicy(max_attempts=5)

        assert not is_terminal(ProcessingState.FAILURE_RETRYABLE, 1, policy_2)
        assert is_terminal(ProcessingState.FAILURE_RETRYABLE, 2, policy_2)

        assert not is_terminal(ProcessingState.FAILURE_RETRYABLE, 4, policy_5)
        assert is_terminal(ProcessingState.FAILURE_RETRYABLE, 5, policy_5)


class TestProcessingEventRecord:
    """Tests for ProcessingEventRecord dataclass."""

    def test_processing_event_record_required_fields(self) -> None:
        """Test ProcessingEventRecord with required fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            ts="2024-01-01T00:00:00Z",
        )
        assert record.state == "SUCCESS"
        assert record.ts == "2024-01-01T00:00:00Z"
        assert record.batch_job_id is None
        assert record.exit_code is None

    def test_processing_event_record_all_fields(self) -> None:
        """Test ProcessingEventRecord with all fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            ts="2024-01-01T00:00:00Z",
            batch_job_id="batch-123",
            exit_code=0,
        )
        assert record.state == "SUCCESS"
        assert record.ts == "2024-01-01T00:00:00Z"
        assert record.batch_job_id == "batch-123"
        assert record.exit_code == 0

    def test_to_dict_drops_none_fields(self) -> None:
        """Test that to_dict() drops fields with None values."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            ts="2024-01-01T00:00:00Z",
            batch_job_id=None,
            exit_code=None,
        )
        result = record.to_dict()
        assert result == {
            "state": "SUCCESS",
            "ts": "2024-01-01T00:00:00Z",
        }
        assert "batch_job_id" not in result
        assert "exit_code" not in result

    def test_to_dict_includes_non_none_fields(self) -> None:
        """Test that to_dict() includes fields with non-None values."""
        record = ProcessingEventRecord(
            state="FAILURE_RETRYABLE",
            ts="2024-01-01T00:00:00Z",
            batch_job_id="batch-456",
            exit_code=1,
        )
        result = record.to_dict()
        assert result == {
            "state": "FAILURE_RETRYABLE",
            "ts": "2024-01-01T00:00:00Z",
            "batch_job_id": "batch-456",
            "exit_code": 1,
        }

    def test_to_dict_partial_none_fields(self) -> None:
        """Test to_dict() with some None and some non-None fields."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            ts="2024-01-01T00:00:00Z",
            batch_job_id="batch-789",
            exit_code=None,
        )
        result = record.to_dict()
        assert result == {
            "state": "SUCCESS",
            "ts": "2024-01-01T00:00:00Z",
            "batch_job_id": "batch-789",
        }
        assert "exit_code" not in result

    def test_to_dict_returns_dict_type(self) -> None:
        """Test that to_dict() returns a dict."""
        record = ProcessingEventRecord(
            state="SUCCESS",
            ts="2024-01-01T00:00:00Z",
        )
        result = record.to_dict()
        assert isinstance(result, dict)
