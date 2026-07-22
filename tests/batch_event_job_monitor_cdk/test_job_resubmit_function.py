"""Tests for the JobResubmitFunction CDK construct.

Constructs the App with the aws:cdk:bundling-stacks context key set to an
empty list, so PythonFunction's Docker-based asset bundling is skipped in
favor of a placeholder asset -- these are unit tests of the construct's
resource wiring, not of the consumer's bundled code.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from aws_cdk import App, Stack, aws_sqs as sqs
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.job_resubmit_function import JobResubmitFunction

_ENTRY = tempfile.mkdtemp()
Path(_ENTRY, "handler.py").write_text("def handler(event, context):\n    pass\n")

_BATCH_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_BATCH_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"


def _make_stack(
    *,
    include_describe_jobs: bool = True,
    environment: dict[str, str] | None = None,
) -> tuple[Stack, JobResubmitFunction]:
    app = App(context={"aws:cdk:bundling-stacks": []})
    stack = Stack(app, "TestStack")
    retry_queue = sqs.Queue(stack, "RetryQueue")
    construct = JobResubmitFunction(
        stack,
        "TestJobResubmitFunction",
        entry=_ENTRY,
        index="handler.py",
        retry_queue=retry_queue,
        batch_job_queue_arn=_BATCH_JOB_QUEUE_ARN,
        batch_job_definition_arn=_BATCH_JOB_DEFINITION_ARN,
        include_describe_jobs=include_describe_jobs,
        environment=environment,
    )
    return stack, construct


class TestFunction:
    def test_creates_lambda_function(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)

    def test_passes_through_environment(self) -> None:
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
