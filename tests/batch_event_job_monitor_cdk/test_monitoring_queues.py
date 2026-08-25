"""Tests for the MonitoringQueues CDK construct."""

from __future__ import annotations

from typing import Any

from aws_cdk import App, Duration, Stack, aws_sqs as sqs
from aws_cdk.assertions import Template


def _make_stack(**kwargs: Any) -> tuple[Stack, Any]:
    from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues

    app = App()
    stack = Stack(app, "TestStack")
    construct = MonitoringQueues(stack, "Queues", **kwargs)
    return stack, construct


def _queues(template: Template) -> dict[str, dict[str, Any]]:
    return {
        key: r["Properties"]
        for key, r in template.to_json()["Resources"].items()
        if r["Type"] == "AWS::SQS::Queue"
    }


class TestCreatedQueues:
    def test_creates_all_five_queues(self) -> None:
        stack, _ = _make_stack()
        Template.from_stack(stack).resource_count_is("AWS::SQS::Queue", 5)

    def test_retry_queue_redrives_to_the_retry_dlq(self) -> None:
        stack, construct = _make_stack()
        template = Template.from_stack(stack)
        queues = _queues(template)
        [retry] = [
            props for key, props in queues.items() if key.startswith("QueuesRetryQueue")
        ]
        assert retry["RedrivePolicy"]["maxReceiveCount"] == 5
        assert construct.retry_dlq is not None

    def test_max_receive_count_is_configurable(self) -> None:
        stack, _ = _make_stack(max_receive_count=2)
        queues = _queues(Template.from_stack(stack))
        [retry] = [
            props for key, props in queues.items() if key.startswith("QueuesRetryQueue")
        ]
        assert retry["RedrivePolicy"]["maxReceiveCount"] == 2

    def test_retention_defaults_to_the_sqs_maximum(self) -> None:
        stack, _ = _make_stack()
        queues = _queues(Template.from_stack(stack))
        assert all(
            props["MessageRetentionPeriod"] == 1209600 for props in queues.values()
        )

    def test_retention_is_configurable(self) -> None:
        stack, _ = _make_stack(retention_period=Duration.days(4))
        queues = _queues(Template.from_stack(stack))
        assert all(
            props["MessageRetentionPeriod"] == 345600 for props in queues.values()
        )

    def test_retry_queue_visibility_timeout_is_configurable(self) -> None:
        stack, _ = _make_stack(visibility_timeout=Duration.minutes(11))
        queues = _queues(Template.from_stack(stack))
        [retry] = [
            props for key, props in queues.items() if key.startswith("QueuesRetryQueue")
        ]
        assert retry["VisibilityTimeout"] == 660

    def test_every_queue_enforces_ssl(self) -> None:
        stack, _ = _make_stack()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::SQS::QueuePolicy", 5)


class TestAdoptedQueues:
    def test_adopted_retry_queue_is_not_recreated(self) -> None:
        from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues

        app = App()
        stack = Stack(app, "TestStack")
        existing = sqs.Queue(stack, "Existing")
        construct = MonitoringQueues(stack, "Queues", retry_queue=existing)

        assert construct.retry_queue is existing
        # No retry DLQ is created for an adopted queue: only the queue's
        # creator can set its redrive policy.
        assert construct.retry_dlq is None
        # existing + failure DLQ + untracked + event DLQ
        Template.from_stack(stack).resource_count_is("AWS::SQS::Queue", 4)

    def test_adopted_failure_dlq_is_not_recreated(self) -> None:
        from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues

        app = App()
        stack = Stack(app, "TestStack")
        existing = sqs.Queue(stack, "Existing")
        construct = MonitoringQueues(stack, "Queues", failure_dlq=existing)

        assert construct.failure_dlq is existing
        # existing + retry + retry DLQ + untracked + event DLQ
        Template.from_stack(stack).resource_count_is("AWS::SQS::Queue", 5)
