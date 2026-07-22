"""Tests for resubmit_job.

Uses a local stub batch_client rather than moto's Batch mock: resubmit_job's
own logic under test is orchestration (attempt bump, parameter merging), not
AWS Batch's submit_job request-validation semantics, and moto's Batch support
requires a full compute-environment/job-queue/job-definition fixture stack to
exercise realistically.
"""

from __future__ import annotations

from typing import Any, cast

from mypy_boto3_batch import BatchClient

from batch_event_job_monitor.models import JobContext
from batch_event_job_monitor.submission import resubmit_job, submit_job

CONTEXT = JobContext(
    job_type="monthly-composite",
    partition_fields={"tile_id": "12TVK", "year_month": "2024-06"},
    input_entity_id="12TVK_2024-06_source",
    output_entity_id="12TVK_2024-06_output",
    attempt=1,
)


class FakeBatchClient:
    def __init__(self, job_id: str = "new-batch-job-id") -> None:
        self.job_id = job_id
        self.calls: list[dict[str, Any]] = []

    def submit_job(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(kwargs)
        return {"jobId": self.job_id}


def base_submit_job_params(context: JobContext) -> dict[str, Any]:
    return {
        "jobName": f"{context.job_type}-{context.input_entity_id}",
        "jobQueue": "queue-arn",
        "jobDefinition": "job-def-arn",
    }


class TestResubmitJob:
    def test_bumps_attempt(self) -> None:
        batch_client = FakeBatchClient()
        resubmit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]
        assert params["parameters"]["bejm_attempt"] == "2"

    def test_returns_new_job_id(self) -> None:
        batch_client = FakeBatchClient(job_id="abc-123")
        result = resubmit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        assert result == "abc-123"

    def test_injects_identity_parameters(self) -> None:
        batch_client = FakeBatchClient()
        resubmit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]["parameters"]
        assert params["bejm_job_type"] == "monthly-composite"
        assert params["bejm_input_entity_id"] == "12TVK_2024-06_source"
        assert params["bejm_output_entity_id"] == "12TVK_2024-06_output"
        assert params["bejm_attempt"] == "2"

    def test_passes_through_base_submit_job_params(self) -> None:
        batch_client = FakeBatchClient()
        resubmit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]
        assert params["jobQueue"] == "queue-arn"
        assert params["jobDefinition"] == "job-def-arn"

    def test_callback_receives_bumped_context(self) -> None:
        seen: list[JobContext] = []

        def build_params(context: JobContext) -> dict[str, Any]:
            seen.append(context)
            return base_submit_job_params(context)

        resubmit_job(
            batch_client=cast(BatchClient, FakeBatchClient()),
            build_submit_job_params=build_params,
            context=CONTEXT,
        )
        assert seen[0].attempt == 2
        assert seen[0].input_entity_id == CONTEXT.input_entity_id

    def test_callbacks_own_parameters_survive_merge(self) -> None:
        def build_params(context: JobContext) -> dict[str, Any]:
            return {
                **base_submit_job_params(context),
                "parameters": {"custom_key": "custom_value"},
            }

        batch_client = FakeBatchClient()
        resubmit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=build_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]["parameters"]
        assert params["custom_key"] == "custom_value"
        assert params["bejm_attempt"] == "2"


class TestSubmitJob:
    def test_does_not_bump_attempt(self) -> None:
        batch_client = FakeBatchClient()
        submit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]["parameters"]
        assert params["bejm_attempt"] == "1"

    def test_returns_new_job_id(self) -> None:
        batch_client = FakeBatchClient(job_id="abc-123")
        result = submit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        assert result == "abc-123"

    def test_injects_identity_parameters(self) -> None:
        batch_client = FakeBatchClient()
        submit_job(
            batch_client=cast(BatchClient, batch_client),
            build_submit_job_params=base_submit_job_params,
            context=CONTEXT,
        )
        params = batch_client.calls[0]["parameters"]
        assert params["bejm_job_type"] == "monthly-composite"
        assert params["bejm_input_entity_id"] == "12TVK_2024-06_source"
        assert params["bejm_output_entity_id"] == "12TVK_2024-06_output"
        assert params["bejm_attempt"] == "1"

    def test_callback_receives_unmodified_context(self) -> None:
        seen: list[JobContext] = []

        def build_params(context: JobContext) -> dict[str, Any]:
            seen.append(context)
            return base_submit_job_params(context)

        submit_job(
            batch_client=cast(BatchClient, FakeBatchClient()),
            build_submit_job_params=build_params,
            context=CONTEXT,
        )
        assert seen[0] == CONTEXT
