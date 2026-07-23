"""Tests for the JobMonitorFunction CDK construct."""

from __future__ import annotations

import json

from aws_cdk import App, Stack, aws_s3 as s3, aws_sqs as sqs
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor.models import (
    ExitCodeOutcome,
    ExitCodeOutcomes,
    JobTypeConfig,
    RetryPolicy,
)
from batch_event_job_monitor_cdk.job_monitor_function import JobMonitorFunction

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"

_ONE_JOB_TYPE_CONFIG = {
    "monthly-composite": JobTypeConfig(
        job_queue_arn=_JOB_QUEUE_ARN, job_definition_arn=_JOB_DEFINITION_ARN
    )
}


def _make_stack(
    *,
    with_retry_queue: bool = False,
    with_dlq: bool = False,
    job_type_configs: dict[str, JobTypeConfig] | None = None,
) -> tuple[Stack, JobMonitorFunction]:
    app = App()
    stack = Stack(app, "TestStack")
    bucket = s3.Bucket(stack, "Bucket")
    retry_queue = sqs.Queue(stack, "RetryQueue") if with_retry_queue else None
    dlq = sqs.Queue(stack, "Dlq") if with_dlq else None
    construct = JobMonitorFunction(
        stack,
        "TestJobMonitorFunction",
        processing_bucket=bucket,
        job_type_configs=job_type_configs or _ONE_JOB_TYPE_CONFIG,
        retry_queue=retry_queue,
        dlq=dlq,
    )
    return stack, construct


class TestFunction:
    def test_creates_lambda_function(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)

    def test_handler_and_runtime(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Handler": "batch_event_job_monitor.handlers.job_monitor_handler.handler",
                "Runtime": "python3.12",
            },
        )

    def test_environment_without_retry_or_dlq_queues(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_equals(
                        {
                            "PROCESSING_BUCKET_NAME": Match.any_value(),
                            "PROCESSING_JOB_TYPE_CONFIGS": Match.any_value(),
                        }
                    )
                }
            },
        )

    def test_job_type_config_serialized_into_env_var(self) -> None:
        job_type_configs = {
            "monthly-composite": JobTypeConfig(
                job_queue_arn=_JOB_QUEUE_ARN,
                job_definition_arn=_JOB_DEFINITION_ARN,
                retry_policy=RetryPolicy(max_attempts=5),
                exit_code_outcomes=ExitCodeOutcomes(
                    {4: ExitCodeOutcome(name="CLOUDY", dlq=False)}
                ),
            )
        }
        stack, _ = _make_stack(job_type_configs=job_type_configs)
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        [fn] = [r for r in resources.values() if r["Type"] == "AWS::Lambda::Function"]
        configs = json.loads(
            fn["Properties"]["Environment"]["Variables"]["PROCESSING_JOB_TYPE_CONFIGS"]
        )
        assert set(configs.keys()) == {"monthly-composite"}
        config = configs["monthly-composite"]
        assert config["job_queue_arn"] == _JOB_QUEUE_ARN
        assert config["job_definition_arn"] == _JOB_DEFINITION_ARN
        assert config["retry_policy"]["max_attempts"] == 5
        assert config["exit_code_outcomes"] == {
            "4": {"name": "CLOUDY", "retryable": False, "dlq": False}
        }

    def test_environment_with_queues(self) -> None:
        stack, _ = _make_stack(with_retry_queue=True, with_dlq=True)
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "JOB_RETRY_QUEUE_URL": Match.any_value(),
                            "JOB_FAILURE_DLQ_URL": Match.any_value(),
                        }
                    )
                }
            },
        )


class TestIamGrants:
    def test_s3_read_write_grant_always_present(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {"Action": Match.array_with(["s3:GetObject*"])}
                            )
                        ]
                    )
                }
            },
        )

    def test_sqs_send_grant_present_when_queues_given(self) -> None:
        stack, _ = _make_stack(with_retry_queue=True, with_dlq=True)
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {"Action": Match.array_with(["sqs:SendMessage"])}
                            )
                        ]
                    )
                }
            },
        )

    def test_no_sqs_grant_when_queues_absent(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        policies = [r for r in resources.values() if r["Type"] == "AWS::IAM::Policy"]
        actions = [
            stmt["Action"]
            for policy in policies
            for stmt in policy["Properties"]["PolicyDocument"]["Statement"]
        ]
        assert "sqs:SendMessage" not in actions


class TestEventRule:
    def test_creates_one_rule_per_job_type(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Events::Rule", 1)

    def test_matches_all_batch_statuses(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "EventPattern": {
                    "source": ["aws.batch"],
                    "detail-type": ["Batch Job State Change"],
                    "detail": Match.object_like(
                        {
                            "status": [
                                "SUBMITTED",
                                "PENDING",
                                "RUNNABLE",
                                "STARTING",
                                "RUNNING",
                                "SUCCEEDED",
                                "FAILED",
                            ]
                        }
                    ),
                }
            },
        )

    def test_scoped_to_job_type_queue_and_job_definition(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "EventPattern": {
                    "detail": Match.object_like(
                        {
                            "jobQueue": [_JOB_QUEUE_ARN],
                            "jobDefinition": [{"prefix": f"{_JOB_DEFINITION_ARN}:"}],
                        }
                    )
                }
            },
        )

    def test_targets_the_function(self) -> None:
        stack, construct = _make_stack()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "Targets": Match.array_with(
                    [Match.object_like({"Arn": Match.any_value()})]
                )
            },
        )
        assert construct.rules["monthly-composite"] is not None


class TestMultipleJobTypes:
    _OTHER_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/other-queue"
    _OTHER_JOB_DEFINITION_ARN = (
        "arn:aws:batch:us-west-2:123456789012:job-definition/other-def"
    )

    def _job_type_configs(self) -> dict[str, JobTypeConfig]:
        return {
            "monthly-composite": JobTypeConfig(
                job_queue_arn=_JOB_QUEUE_ARN, job_definition_arn=_JOB_DEFINITION_ARN
            ),
            "granule-processing": JobTypeConfig(
                job_queue_arn=self._OTHER_JOB_QUEUE_ARN,
                job_definition_arn=self._OTHER_JOB_DEFINITION_ARN,
            ),
        }

    def test_creates_one_rule_per_job_type(self) -> None:
        stack, _ = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Events::Rule", 2)

    def test_each_rule_scoped_to_its_own_job_type(self) -> None:
        stack, construct = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        rules = [r for r in resources.values() if r["Type"] == "AWS::Events::Rule"]
        job_queues = {
            tuple(rule["Properties"]["EventPattern"]["detail"]["jobQueue"])
            for rule in rules
        }
        assert job_queues == {(_JOB_QUEUE_ARN,), (self._OTHER_JOB_QUEUE_ARN,)}
        assert set(construct.rules) == {"monthly-composite", "granule-processing"}

    def test_shares_one_lambda_function(self) -> None:
        stack, _ = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)
