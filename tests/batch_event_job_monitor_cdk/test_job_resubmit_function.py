"""Tests for the JobResubmitFunction CDK construct.

Tests using a custom entry/index override construct the App with the
aws:cdk:bundling-stacks context key set to an empty list, so
PythonFunction's Docker-based asset bundling is skipped in favor of a
placeholder asset -- these are unit tests of the construct's resource
wiring, not of the consumer's bundled code. The bundled-default-handler
path uses plain lambda.Code.from_asset (no Docker), so it doesn't need
that context key.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from aws_cdk import App, Stack, aws_batch as batch, aws_sqs as sqs
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.job_resubmit_function import JobResubmitFunction

_CUSTOM_ENTRY = tempfile.mkdtemp()
Path(_CUSTOM_ENTRY, "handler.py").write_text("def handler(event, context):\n    pass\n")

_BATCH_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_BATCH_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"


def _make_refs(stack: Stack) -> tuple[batch.IJobQueue, batch.IJobDefinition]:
    job_queue = batch.JobQueue.from_job_queue_arn(
        stack, "JobQueue", _BATCH_JOB_QUEUE_ARN
    )
    job_definition = batch.EcsJobDefinition.from_job_definition_arn(
        stack, "JobDefinition", _BATCH_JOB_DEFINITION_ARN
    )
    return job_queue, job_definition


def _make_stack(
    *,
    include_describe_jobs: bool = True,
    environment: dict[str, str] | None = None,
    entry: str | None = None,
    index: str | None = None,
    app_context: dict[str, object] | None = None,
) -> tuple[Stack, JobResubmitFunction]:
    app = App(context=app_context or {})
    stack = Stack(app, "TestStack")
    retry_queue = sqs.Queue(stack, "RetryQueue")
    job_queue, job_definition = _make_refs(stack)
    construct = JobResubmitFunction(
        stack,
        "TestJobResubmitFunction",
        job_queue=job_queue,
        job_definition=job_definition,
        retry_queue=retry_queue,
        entry=entry,
        index=index,
        include_describe_jobs=include_describe_jobs,
        environment=environment,
    )
    return stack, construct


def _make_override_stack() -> tuple[Stack, JobResubmitFunction]:
    return _make_stack(
        entry=_CUSTOM_ENTRY,
        index="handler.py",
        app_context={"aws:cdk:bundling-stacks": []},
    )


class TestDefaultHandler:
    def test_creates_lambda_function(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)

    def test_uses_bundled_default_handler(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Handler": "batch_event_job_monitor.handlers.job_resubmit_handler.handler",
                "Runtime": "python3.12",
            },
        )

    def test_sets_job_queue_and_definition_env_vars(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "BATCH_JOB_QUEUE_ARN": _BATCH_JOB_QUEUE_ARN,
                            "BATCH_JOB_DEFINITION_ARN": _BATCH_JOB_DEFINITION_ARN,
                        }
                    )
                }
            },
        )

    def test_passes_through_extra_environment(self) -> None:
        stack, _ = _make_stack(environment={"CUSTOM_KEY": "custom-value"})
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like({"CUSTOM_KEY": "custom-value"})
                }
            },
        )


class TestEntryIndexValidation:
    def test_entry_without_index_raises(self) -> None:
        with pytest.raises(ValueError, match="entry and index must be given together"):
            _make_stack(entry=_CUSTOM_ENTRY)

    def test_index_without_entry_raises(self) -> None:
        with pytest.raises(ValueError, match="entry and index must be given together"):
            _make_stack(index="handler.py")


class TestCustomHandlerOverride:
    def test_creates_lambda_function(self) -> None:
        stack, _ = _make_override_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)

    def test_sets_job_queue_and_definition_env_vars(self) -> None:
        stack, _ = _make_override_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "BATCH_JOB_QUEUE_ARN": _BATCH_JOB_QUEUE_ARN,
                            "BATCH_JOB_DEFINITION_ARN": _BATCH_JOB_DEFINITION_ARN,
                        }
                    )
                }
            },
        )


class TestIamGrants:
    def test_submit_job_and_describe_jobs_by_default(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": Match.array_with(
                                        ["batch:SubmitJob", "batch:DescribeJobs"]
                                    ),
                                    "Resource": Match.array_with(
                                        [
                                            _BATCH_JOB_QUEUE_ARN,
                                            _BATCH_JOB_DEFINITION_ARN,
                                        ]
                                    ),
                                }
                            )
                        ]
                    )
                }
            },
        )

    def test_describe_jobs_omitted_when_disabled(self) -> None:
        stack, _ = _make_stack(include_describe_jobs=False)
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        policies = [r for r in resources.values() if r["Type"] == "AWS::IAM::Policy"]
        actions = [
            action
            for policy in policies
            for stmt in policy["Properties"]["PolicyDocument"]["Statement"]
            for action in (
                stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
            )
        ]
        assert "batch:DescribeJobs" not in actions
        assert "batch:SubmitJob" in actions


class TestEventSource:
    def test_creates_event_source_mapping(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::EventSourceMapping", 1)

    def test_reports_batch_item_failures(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {"FunctionResponseTypes": ["ReportBatchItemFailures"]},
        )

    def test_default_batch_size(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping", {"BatchSize": 100}
        )
