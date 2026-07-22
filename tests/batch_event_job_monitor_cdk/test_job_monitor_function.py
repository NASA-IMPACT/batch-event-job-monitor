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


def _make_stack(
    *,
    with_retry_queue: bool = False,
    with_dlq: bool = False,
    job_queue_arns: list[str] | None = None,
    job_type_configs: dict[str, JobTypeConfig] | None = None,
    default_job_type_config: JobTypeConfig | None = None,
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
        job_type_configs=job_type_configs,
        default_job_type_config=default_job_type_config,
        retry_queue=retry_queue,
        dlq=dlq,
        job_queue_arns=job_queue_arns,
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

    def test_environment_without_queues(self) -> None:
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

    def test_default_job_type_config_used_when_no_override_given(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        [fn] = [r for r in resources.values() if r["Type"] == "AWS::Lambda::Function"]
        configs = json.loads(
            fn["Properties"]["Environment"]["Variables"]["PROCESSING_JOB_TYPE_CONFIGS"]
        )
        assert configs == {
            "__default__": {
                "retry_policy": {
                    "max_attempts": 3,
                    "spot_interruption_status_reason_prefixes": ["Host EC2"],
                },
                "exit_code_outcomes": {},
            }
        }

    def test_per_job_type_config_included_alongside_default(self) -> None:
        job_type_configs = {
            "monthly-composite": JobTypeConfig(
                retry_policy=RetryPolicy(max_attempts=5),
                exit_code_outcomes=ExitCodeOutcomes(
                    {4: ExitCodeOutcome(label="CLOUDY", dlq=False)}
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
        assert set(configs.keys()) == {"__default__", "monthly-composite"}
        assert configs["monthly-composite"]["retry_policy"]["max_attempts"] == 5
        assert configs["monthly-composite"]["exit_code_outcomes"] == {
            "4": {"label": "CLOUDY", "retryable": False, "dlq": False}
        }

    def test_custom_default_job_type_config(self) -> None:
        stack, _ = _make_stack(
            default_job_type_config=JobTypeConfig(
                retry_policy=RetryPolicy(max_attempts=7)
            )
        )
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        [fn] = [r for r in resources.values() if r["Type"] == "AWS::Lambda::Function"]
        configs = json.loads(
            fn["Properties"]["Environment"]["Variables"]["PROCESSING_JOB_TYPE_CONFIGS"]
        )
        assert configs["__default__"]["retry_policy"]["max_attempts"] == 7

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
    def test_creates_rule(self) -> None:
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
                    "detail": {
                        "status": [
                            "SUBMITTED",
                            "PENDING",
                            "RUNNABLE",
                            "STARTING",
                            "RUNNING",
                            "SUCCEEDED",
                            "FAILED",
                        ]
                    },
                }
            },
        )

    def test_unscoped_by_default(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        [rule] = [r for r in resources.values() if r["Type"] == "AWS::Events::Rule"]
        assert "jobQueue" not in rule["Properties"]["EventPattern"]["detail"]

    def test_scoped_to_job_queue_arns(self) -> None:
        stack, _ = _make_stack(
            job_queue_arns=["arn:aws:batch:us-west-2:123:job-queue/q"]
        )
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "EventPattern": {
                    "detail": Match.object_like(
                        {"jobQueue": ["arn:aws:batch:us-west-2:123:job-queue/q"]}
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
        assert construct.rule is not None
