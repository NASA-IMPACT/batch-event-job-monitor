from typing import Any

import pytest

from batch_event_job_monitor.job_details import JobDetails
from batch_event_job_monitor.models import ProcessingState, RetryPolicy


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
        assert job_details.classify(RetryPolicy()) == ProcessingState.SUCCESS

    def test_failed_with_spot_interruption_status_reason_is_retryable(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                status="FAILED",
                statusReason="Host EC2 (instance i-0abcd1234) terminated.",
            )
        )
        assert job_details.classify(RetryPolicy()) == ProcessingState.FAILURE_RETRYABLE

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
        assert job_details.classify(RetryPolicy()) == ProcessingState.FAILURE_RETRYABLE

    def test_failed_with_different_reason_is_nonretryable(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(
                status="FAILED",
                statusReason="Essential container in task exited",
            )
        )
        assert (
            job_details.classify(RetryPolicy()) == ProcessingState.FAILURE_NONRETRYABLE
        )

    def test_failed_with_missing_status_reason_is_nonretryable(self) -> None:
        job_details = JobDetails.from_event(make_detail(status="FAILED"))
        assert (
            job_details.classify(RetryPolicy()) == ProcessingState.FAILURE_NONRETRYABLE
        )

    def test_failed_uses_custom_retry_policy_prefixes(self) -> None:
        job_details = JobDetails.from_event(
            make_detail(status="FAILED", statusReason="Custom reason: retry me")
        )
        policy = RetryPolicy(spot_interruption_status_reason_prefixes=("Custom",))
        assert job_details.classify(policy) == ProcessingState.FAILURE_RETRYABLE

    @pytest.mark.parametrize("status", ["SUBMITTED", "PENDING", "RUNNABLE", "RUNNING"])
    def test_non_terminal_status_raises_value_error(self, status: str) -> None:
        job_details = JobDetails.from_event(make_detail(status=status))
        with pytest.raises(ValueError):
            job_details.classify(RetryPolicy())
