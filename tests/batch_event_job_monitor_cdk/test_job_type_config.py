"""Tests for the job_type_config CDK helper."""

from __future__ import annotations

from aws_cdk import App, Size, Stack, Token, aws_batch as batch, aws_ecs as ecs

from batch_event_job_monitor.models import ExitCodeOutcomes, RetryPolicy
from batch_event_job_monitor_cdk.job_type_config import (
    job_definition_family_arn,
    job_type_config,
)

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"


def _stack() -> Stack:
    return Stack(App(), "TestStack")


def _in_stack_job_definition(stack: Stack, name: str) -> batch.EcsJobDefinition:
    """A job definition created in this app, whose ARN is a CDK token."""
    return batch.EcsJobDefinition(
        stack,
        "OwnedJobDefinition",
        job_definition_name=name,
        container=batch.EcsEc2ContainerDefinition(
            stack,
            "Container",
            image=ecs.ContainerImage.from_registry("busybox"),
            cpu=1,
            memory=Size.mebibytes(512),
        ),
    )


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

    def test_owned_job_definition_arn_is_a_token(self) -> None:
        # Guards the premise of the test below: a job definition created in
        # this app exposes a token whose ref carries the revision, so the
        # revision cannot be stripped by string surgery.
        stack = _stack()
        job_definition = _in_stack_job_definition(stack, "composites")
        assert Token.is_unresolved(job_definition.job_definition_arn)

    def test_owned_job_definition_family_arn_omits_the_revision(self) -> None:
        stack = Stack(
            App(), "TestStack", env={"account": "123456789012", "region": "us-west-2"}
        )
        job_definition = _in_stack_job_definition(stack, "composites")

        resolved = stack.resolve(job_definition_family_arn(job_definition))
        separator, parts = resolved["Fn::Join"]
        assert separator == ""
        # The ARN ends at the job definition name, so nothing can follow it
        # -- a ":<revision>" here would make the EventBridge rule's
        # "{family_arn}:" prefix match no event at all.
        assert parts[-2].endswith(":job-definition/")
        assert isinstance(parts[-1], dict)
        assert "Fn::Select" in parts[-1]


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
