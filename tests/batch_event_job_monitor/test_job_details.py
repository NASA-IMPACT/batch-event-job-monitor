import json
from typing import Any

import pytest

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.models import (
    ExitCodeOutcomesBuilder,
    JobContext,
    ProcessingState,
    RetryPolicy,
)


def make_detail(**overrides: Any) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "jobId": "job-123",
        "jobName": "granule-processing-job",
        "jobQueue": "queue-arn",
        "status": "SUCCEEDED",
        "startedAt": 1700000000,
        "jobDefinition": "job-def-arn",
    }
    detail.update(overrides)
    return detail


class TestFromEvent:
    def test_from_event_stores_raw_detail(self) -> None:
        detail = make_detail()
        job_details = JobDetails.from_event(detail)
        assert job_details.raw == detail


class TestProperties:
    def test_job_id(self) -> None:
        job_details = JobDetails.from_event(make_detail(jobId="abc"))
        assert job_details.job_id == "abc"

    def test_job_name(self) -> None:
        job_details = JobDetails.from_event(make_detail(jobName="my-job"))
        assert job_details.job_name == "my-job"

    def test_status(self) -> None:
        job_details = JobDetails.from_event(make_detail(status="FAILED"))
        assert job_details.status == "FAILED"

    def test_exit_code_from_top_level_container(self) -> None:
        job_details = JobDetails.from_event(make_detail(container={"exitCode": 1}))
        assert job_details.exit_code == 1

    def test_exit_code_falls_back_to_last_attempt(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                attempts=[
                    {"container": {"exitCode": 137}},
                    {"container": {"exitCode": 1}},
                ]
            )
        )
        assert job_details.exit_code == 1

    def test_exit_code_missing_returns_none(self) -> None:
        job_details = JobDetails.from_event(make_detail())
        assert job_details.exit_code is None

    def test_exit_code_missing_from_attempt_container_returns_none(self) -> None:
        job_details = JobDetails.from_event(make_detail(attempts=[{"container": {}}]))
        assert job_details.exit_code is None

    def test_status_reason_from_top_level(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(statusReason="Essential container in task exited")
        )
        assert job_details.status_reason == "Essential container in task exited"

    def test_status_reason_falls_back_to_last_attempt(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                attempts=[
                    {"statusReason": "first attempt reason"},
                    {"statusReason": "Host EC2 (instance i-abc) terminated."},
                ]
            )
        )
        assert job_details.status_reason == "Host EC2 (instance i-abc) terminated."

    def test_status_reason_missing_returns_none(self) -> None:
        job_details = JobDetails.from_event(make_detail())
        assert job_details.status_reason is None


class TestClassify:
    def test_succeeded_returns_success(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(status="SUCCEEDED", container={"exitCode": 0})
        )
        assert job_details.classify(RetryPolicy()).state == ProcessingState.SUCCESS

    def test_failed_with_spot_interruption_status_reason_is_retryable(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                status="FAILED",
                statusReason="Host EC2 (instance i-0abcd1234) terminated.",
            )
        )
        assert (
            job_details.classify(RetryPolicy()).state
            == ProcessingState.FAILURE_RETRYABLE
        )

    def test_failed_with_spot_interruption_from_last_attempt_is_retryable(
        self,
    ) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                status="FAILED",
                attempts=[
                    {"statusReason": "some other reason"},
                    {"statusReason": "Host EC2 (instance i-0abcd1234) terminated."},
                ],
            )
        )
        assert (
            job_details.classify(RetryPolicy()).state
            == ProcessingState.FAILURE_RETRYABLE
        )

    def test_failed_with_different_reason_is_nonretryable(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                status="FAILED",
                statusReason="Essential container in task exited",
            )
        )
        assert (
            job_details.classify(RetryPolicy()).state
            == ProcessingState.FAILURE_NONRETRYABLE
        )

    def test_failed_with_missing_status_reason_is_nonretryable(self) -> None:
        job_details = JobDetails.from_event(make_detail(status="FAILED"))
        assert (
            job_details.classify(RetryPolicy()).state
            == ProcessingState.FAILURE_NONRETRYABLE
        )

    def test_failed_uses_custom_retry_policy_prefixes(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(status="FAILED", statusReason="Custom reason: retry me")
        )
        policy = RetryPolicy(spot_interruption_status_reason_prefixes=("Custom",))
        assert job_details.classify(policy).state == ProcessingState.FAILURE_RETRYABLE

    def test_submitted_returns_submitted(self) -> None:
        job_details = JobDetails.from_event(make_detail(status="SUBMITTED"))
        assert job_details.classify(RetryPolicy()).state == ProcessingState.SUBMITTED

    @pytest.mark.parametrize("status", ["PENDING", "RUNNABLE", "STARTING", "RUNNING"])
    def test_in_flight_status_returns_awaiting(self, status: str) -> None:
        job_details = JobDetails.from_event(make_detail(status=status))
        assert job_details.classify(RetryPolicy()).state == ProcessingState.AWAITING

    def test_unrecognized_status_raises_value_error(self) -> None:
        job_details = JobDetails.from_event(make_detail(status="BOGUS"))
        with pytest.raises(ValueError):
            job_details.classify(RetryPolicy())


class TestClassifyExitCodeOutcomes:
    def _detail(self, *, exit_code: int, **overrides: Any) -> dict[str, Any]:
        return make_detail(
            status="FAILED", container={"exitCode": exit_code}, **overrides
        )

    def test_nonretryable_outcome_sets_state_label_and_dlq(self) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY", dlq=False).build()
        job_details = JobDetails.from_event(self._detail(exit_code=4))
        classification = job_details.classify(RetryPolicy(), outcomes)
        assert classification.state == ProcessingState.FAILURE_NONRETRYABLE
        assert classification.label == "CLOUDY"
        assert classification.dlq is False

    def test_retryable_outcome_sets_failure_retryable(self) -> None:
        outcomes = (
            ExitCodeOutcomesBuilder()
            .add(42, "TRANSIENT_TOOL_ERROR", retryable=True)
            .build()
        )
        job_details = JobDetails.from_event(self._detail(exit_code=42))
        classification = job_details.classify(RetryPolicy(), outcomes)
        assert classification.state == ProcessingState.FAILURE_RETRYABLE
        assert classification.label == "TRANSIENT_TOOL_ERROR"

    def test_dlq_defaults_true(self) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY").build()
        job_details = JobDetails.from_event(self._detail(exit_code=4))
        assert job_details.classify(RetryPolicy(), outcomes).dlq is True

    def test_unmapped_exit_code_falls_back_to_default_classification(self) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY").build()
        job_details = JobDetails.from_event(self._detail(exit_code=1))
        classification = job_details.classify(RetryPolicy(), outcomes)
        assert classification.state == ProcessingState.FAILURE_NONRETRYABLE
        assert classification.label is None
        assert classification.dlq is True

    def test_no_exit_code_outcomes_falls_back_to_default_classification(self) -> None:
        job_details = JobDetails.from_event(self._detail(exit_code=4))
        classification = job_details.classify(RetryPolicy())
        assert classification.state == ProcessingState.FAILURE_NONRETRYABLE
        assert classification.label is None

    def test_spot_interruption_fallback_still_applies_when_no_outcome_matches(
        self,
    ) -> None:
        outcomes = ExitCodeOutcomesBuilder().add(4, "CLOUDY").build()
        job_details = JobDetails.from_event(
            self._detail(
                exit_code=137,
                statusReason="Host EC2 (instance i-0abcd1234) terminated.",
            )
        )
        classification = job_details.classify(RetryPolicy(), outcomes)
        assert classification.state == ProcessingState.FAILURE_RETRYABLE
        assert classification.label is None


class TestParameters:
    def test_parameters_present(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(parameters={"bejm_job_type": "monthly-composite"})
        )
        assert job_details.parameters == {"bejm_job_type": "monthly-composite"}

    def test_parameters_missing_returns_empty_dict(self) -> None:
        job_details = JobDetails.from_event(make_detail())
        assert job_details.parameters == {}


def make_bejm_parameters(**overrides: str) -> dict[str, str]:
    params = {
        "bejm_job_type": "monthly-composite",
        "bejm_input_entity_id": "12TVK_2024-06_source",
        "bejm_output_entity_id": "12TVK_2024-06_output",
        "bejm_partition_fields": json.dumps({"tile_id": "12TVK"}),
        "bejm_attempt": "1",
    }
    params.update(overrides)
    return params


class TestDecodeContext:
    def test_decodes_full_context(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(parameters=make_bejm_parameters())
        )
        assert job_details.decode_context() == JobContext(
            job_type="monthly-composite",
            partition_fields={"tile_id": "12TVK"},
            input_entity_id="12TVK_2024-06_source",
            output_entity_id="12TVK_2024-06_output",
            attempt=1,
        )

    @pytest.mark.parametrize(
        "key",
        [
            "bejm_job_type",
            "bejm_input_entity_id",
            "bejm_output_entity_id",
            "bejm_partition_fields",
            "bejm_attempt",
        ],
    )
    def test_missing_single_key_raises_with_key_name(self, key: str) -> None:
        params = make_bejm_parameters()
        del params[key]
        job_details = JobDetails.from_event(make_detail(parameters=params))
        with pytest.raises(ValueError, match=key):
            job_details.decode_context()

    def test_all_keys_missing_lists_all_in_error(self) -> None:
        job_details = JobDetails.from_event(make_detail(parameters={}))
        with pytest.raises(ValueError) as exc_info:
            job_details.decode_context()
        message = str(exc_info.value)
        for key in (
            "bejm_job_type",
            "bejm_input_entity_id",
            "bejm_output_entity_id",
            "bejm_partition_fields",
            "bejm_attempt",
        ):
            assert key in message

    def test_non_integer_attempt_raises(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(parameters=make_bejm_parameters(bejm_attempt="not-a-number"))
        )
        with pytest.raises(ValueError, match="bejm_attempt"):
            job_details.decode_context()

    def test_malformed_partition_fields_json_raises(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                parameters=make_bejm_parameters(bejm_partition_fields="not-json")
            )
        )
        with pytest.raises(ValueError, match="bejm_partition_fields"):
            job_details.decode_context()

    def test_partition_fields_not_object_raises(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(parameters=make_bejm_parameters(bejm_partition_fields="[1, 2]"))
        )
        with pytest.raises(ValueError, match="bejm_partition_fields"):
            job_details.decode_context()
