"""Tests for the job_type_config CDK helper."""

from __future__ import annotations

from aws_cdk import App, Stack, aws_batch as batch

from batch_event_job_monitor.models import ExitCodeOutcomes, RetryPolicy
from batch_event_job_monitor_cdk.job_type_config import (
    job_definition_family_arn,
    job_type_config,
)

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"


def _stack() -> Stack:
    return Stack(App(), "TestStack")


class TestJobDefinitionFamilyArn:
    def test_no_revision_suffix_unchanged(self) -> None:
        stack = _stack()
        job_definition = batch.EcsJobDefinition.from_job_definition_arn(
            stack, "JobDefinition", _JOB_DEFINITION_ARN
        )
        assert job_definition_family_arn(job_definition) == _JOB_DEFINITION_ARN

    def test_revision_suffix_stripped(self) -> None:
        stack = _stack()
        job_definition = batch.EcsJobDefinition.from_job_definition_arn(
            stack, "JobDefinition", f"{_JOB_DEFINITION_ARN}:7"
        )
        assert job_definition_family_arn(job_definition) == _JOB_DEFINITION_ARN


class TestJobTypeConfig:
    def test_extracts_arns_from_typed_refs(self) -> None:
        stack = _stack()
        job_queue = batch.JobQueue.from_job_queue_arn(stack, "JobQueue", _JOB_QUEUE_ARN)
        job_definition = batch.EcsJobDefinition.from_job_definition_arn(
            stack, "JobDefinition", f"{_JOB_DEFINITION_ARN}:3"
        )
        config = job_type_config(job_queue=job_queue, job_definition=job_definition)
        assert config.job_queue_arn == _JOB_QUEUE_ARN
        assert config.job_definition_arn == _JOB_DEFINITION_ARN

    def test_defaults_retry_policy_and_exit_code_outcomes(self) -> None:
        stack = _stack()
        job_queue = batch.JobQueue.from_job_queue_arn(stack, "JobQueue", _JOB_QUEUE_ARN)
        job_definition = batch.EcsJobDefinition.from_job_definition_arn(
            stack, "JobDefinition", _JOB_DEFINITION_ARN
        )
        config = job_type_config(job_queue=job_queue, job_definition=job_definition)
        assert config.retry_policy == RetryPolicy()
        assert config.exit_code_outcomes == ExitCodeOutcomes()

    def test_passes_through_given_retry_policy_and_exit_code_outcomes(self) -> None:
        stack = _stack()
        job_queue = batch.JobQueue.from_job_queue_arn(stack, "JobQueue", _JOB_QUEUE_ARN)
        job_definition = batch.EcsJobDefinition.from_job_definition_arn(
            stack, "JobDefinition", _JOB_DEFINITION_ARN
        )
        retry_policy = RetryPolicy(max_attempts=5)
        config = job_type_config(
            job_queue=job_queue,
            job_definition=job_definition,
            retry_policy=retry_policy,
        )
        assert config.retry_policy == retry_policy
