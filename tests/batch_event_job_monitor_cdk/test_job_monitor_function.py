"""Tests for the JobMonitorFunction CDK construct."""

from __future__ import annotations

import dataclasses
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest
from aws_cdk import (
    App,
    Size,
    Stack,
    aws_batch as batch,
    aws_ecs as ecs,
    aws_s3 as s3,
)
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor.models import (
    BATCH_EVENT_STATUSES,
    ExitCodeOutcome,
    ExitCodeOutcomes,
    JobTypeConfig,
    RetryPolicy,
)
from batch_event_job_monitor_cdk.job_monitor_function import JobMonitorFunction
from batch_event_job_monitor_cdk.job_type_config import job_type_config
from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"

_ONE_JOB_TYPE_CONFIG = {
    "monthly-composite": JobTypeConfig(
        job_queue_arn=_JOB_QUEUE_ARN, job_definition_arn=_JOB_DEFINITION_ARN
    )
}


def _tracking(*statuses: str) -> dict[str, JobTypeConfig]:
    return {
        "monthly-composite": JobTypeConfig(
            job_queue_arn=_JOB_QUEUE_ARN,
            job_definition_arn=_JOB_DEFINITION_ARN,
            tracked_statuses=statuses,
        )
    }


def _make_stack(
    *,
    queues: bool = False,
    job_type_configs: dict[str, JobTypeConfig] | None = None,
    **kwargs: Any,
) -> tuple[Stack, JobMonitorFunction]:
    app = App()
    stack = Stack(app, "TestStack")
    bucket = s3.Bucket(stack, "Bucket")
    construct = JobMonitorFunction(
        stack,
        "TestJobMonitorFunction",
        processing_bucket=bucket,
        job_type_configs=job_type_configs or _ONE_JOB_TYPE_CONFIG,
        queues=MonitoringQueues(stack, "Queues") if queues else None,
        **kwargs,
    )
    return stack, construct


def _rules(template: Template) -> list[dict[str, Any]]:
    resources = template.to_json()["Resources"]
    return [
        r["Properties"] for r in resources.values() if r["Type"] == "AWS::Events::Rule"
    ]


def _tracked_rules(template: Template) -> list[dict[str, Any]]:
    return [
        rule
        for rule in _rules(template)
        if "jobDefinition" in rule["EventPattern"]["detail"]
    ]


def _untracked_rules(template: Template) -> list[dict[str, Any]]:
    return [
        rule
        for rule in _rules(template)
        if "jobDefinition" not in rule["EventPattern"]["detail"]
    ]


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

    def test_environment(self) -> None:
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
                            "JOB_RETRY_QUEUE_URL": Match.any_value(),
                            "JOB_FAILURE_DLQ_URL": Match.any_value(),
                            "MONITOR_METRIC_NAMESPACE": "BatchEventJobMonitor",
                        }
                    )
                }
            },
        )

    def test_metric_namespace_is_overridable(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")
        JobMonitorFunction(
            stack,
            "Monitor",
            processing_bucket=s3.Bucket(stack, "Bucket"),
            job_type_configs=_ONE_JOB_TYPE_CONFIG,
            metric_namespace="MyNamespace",
        )
        Template.from_stack(stack).has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {"MONITOR_METRIC_NAMESPACE": "MyNamespace"}
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


class TestQueues:
    def test_creates_its_own_queues_by_default(self) -> None:
        stack, construct = _make_stack()
        assert isinstance(construct.queues, MonitoringQueues)
        # retry + retry DLQ + failure DLQ + untracked + event DLQ
        Template.from_stack(stack).resource_count_is("AWS::SQS::Queue", 5)

    def test_adopts_the_queues_it_is_given(self) -> None:
        stack, construct = _make_stack(queues=True)
        assert construct.queues.node.scope is stack
        Template.from_stack(stack).resource_count_is("AWS::SQS::Queue", 5)


class TestIamGrants:
    def test_s3_read_write_grant_present(self) -> None:
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

    def test_sqs_send_grant_present(self) -> None:
        stack, _ = _make_stack()
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


class TestTrackedRule:
    def test_one_tracked_rule_per_job_type(self) -> None:
        stack, _ = _make_stack()
        assert len(_tracked_rules(Template.from_stack(stack))) == 1

    def test_matches_every_status_batch_emits_by_default(self) -> None:
        """Batch emits no event for submission, so SUBMITTED is not one."""
        stack, _ = _make_stack()
        [rule] = _tracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["status"] == [
            "PENDING",
            "RUNNABLE",
            "STARTING",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
        ]

    def test_tracked_statuses_narrow_the_tracked_rule(self) -> None:
        stack, _ = _make_stack(
            job_type_configs=_tracking("RUNNABLE", "SUCCEEDED", "FAILED")
        )
        [rule] = _tracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["status"] == [
            "RUNNABLE",
            "SUCCEEDED",
            "FAILED",
        ]

    @pytest.mark.parametrize("missing", ["SUCCEEDED", "FAILED"])
    def test_tracked_statuses_must_include_terminal_statuses(
        self, missing: str
    ) -> None:
        statuses = [s for s in ("RUNNABLE", "SUCCEEDED", "FAILED") if s != missing]
        with pytest.raises(ValueError, match=f"'monthly-composite'.*{missing}"):
            _make_stack(job_type_configs=_tracking(*statuses))

    @pytest.mark.parametrize("status", ["SUBMITTED", "BOGUS"])
    def test_tracked_statuses_reject_statuses_batch_never_emits(
        self, status: str
    ) -> None:
        with pytest.raises(ValueError, match=f"'monthly-composite'.*{status}"):
            _make_stack(job_type_configs=_tracking(status, "SUCCEEDED", "FAILED"))

    def test_scoped_to_job_type_queue_and_job_definition(self) -> None:
        stack, _ = _make_stack()
        [rule] = _tracked_rules(Template.from_stack(stack))
        detail = rule["EventPattern"]["detail"]
        assert detail["jobQueue"] == [_JOB_QUEUE_ARN]
        assert detail["jobDefinition"] == [{"prefix": f"{_JOB_DEFINITION_ARN}:"}]

    def test_requires_the_job_type_parameter(self) -> None:
        stack, _ = _make_stack()
        [rule] = _tracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["parameters"] == {
            "bejm_job_type": [{"exists": True}]
        }

    def test_targets_the_function_with_a_dead_letter_queue(self) -> None:
        stack, construct = _make_stack()
        [rule] = _tracked_rules(Template.from_stack(stack))
        [target] = rule["Targets"]
        assert target["RetryPolicy"] == {"MaximumRetryAttempts": 3}
        assert "DeadLetterConfig" in target
        assert construct.rules["monthly-composite"] is not None


class TestSubmitterStates:
    def _configs(
        self, *names: str, outcomes: ExitCodeOutcomes | None = None
    ) -> dict[str, JobTypeConfig]:
        return {
            "monthly-composite": JobTypeConfig(
                job_queue_arn=_JOB_QUEUE_ARN,
                job_definition_arn=_JOB_DEFINITION_ARN,
                exit_code_outcomes=outcomes or ExitCodeOutcomes(),
                submitter_states=names,
            )
        }

    def test_accepts_distinct_names(self) -> None:
        _make_stack(job_type_configs=self._configs("AWAITING_ANCILLARY"))

    @pytest.mark.parametrize("name", ["", "AWAITING/ANCILLARY", "state=AWAITING"])
    def test_rejects_names_that_are_not_one_key_segment(self, name: str) -> None:
        with pytest.raises(ValueError, match=r"'monthly-composite'.*non-empty"):
            _make_stack(job_type_configs=self._configs(name))

    @pytest.mark.parametrize("name", ["AWAITING", "SUCCESS", "FAILURE_RETRYABLE"])
    def test_rejects_built_in_state_names(self, name: str) -> None:
        with pytest.raises(ValueError, match=f"'monthly-composite'.*{name}.*collide"):
            _make_stack(job_type_configs=self._configs(name))

    def test_rejects_the_job_types_own_outcome_names(self) -> None:
        outcomes = ExitCodeOutcomes({4: ExitCodeOutcome(name="CLOUDY", dlq=False)})
        with pytest.raises(ValueError, match=r"CLOUDY.*collide"):
            _make_stack(job_type_configs=self._configs("CLOUDY", outcomes=outcomes))


class TestUntrackedRule:
    def test_one_untracked_rule_per_job_queue(self) -> None:
        stack, construct = _make_stack()
        assert len(_untracked_rules(Template.from_stack(stack))) == 1
        assert set(construct.untracked_rules) == {"monthly-composite"}

    def test_excludes_jobs_carrying_the_job_type_parameter(self) -> None:
        stack, _ = _make_stack()
        [rule] = _untracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["parameters"] == {
            "bejm_job_type": [{"exists": False}]
        }

    def test_scoped_to_the_queue_not_the_job_definition(self) -> None:
        stack, _ = _make_stack()
        [rule] = _untracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["jobQueue"] == [_JOB_QUEUE_ARN]

    def test_matches_terminal_statuses_only(self) -> None:
        stack, _ = _make_stack()
        [rule] = _untracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["status"] == ["SUCCEEDED", "FAILED"]

    def test_unaffected_by_tracked_statuses(self) -> None:
        stack, _ = _make_stack(
            job_type_configs=_tracking("RUNNABLE", "SUCCEEDED", "FAILED")
        )
        [rule] = _untracked_rules(Template.from_stack(stack))
        assert rule["EventPattern"]["detail"]["status"] == ["SUCCEEDED", "FAILED"]

    def test_targets_both_the_function_and_the_untracked_queue(self) -> None:
        stack, construct = _make_stack()
        template = Template.from_stack(stack)
        [rule] = _untracked_rules(template)
        assert len(rule["Targets"]) == 2

        resources = template.to_json()["Resources"]
        untracked_queue_id = next(
            key
            for key, r in resources.items()
            if r["Type"] == "AWS::SQS::Queue" and "UntrackedQueue" in key
        )
        target_arns = [json.dumps(target["Arn"]) for target in rule["Targets"]]
        assert any(untracked_queue_id in arn for arn in target_arns)
        assert construct.queues.untracked_queue is not None


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

    def _shared_queue_configs(self) -> dict[str, JobTypeConfig]:
        return {
            "monthly-composite": JobTypeConfig(
                job_queue_arn=_JOB_QUEUE_ARN, job_definition_arn=_JOB_DEFINITION_ARN
            ),
            "granule-processing": JobTypeConfig(
                job_queue_arn=_JOB_QUEUE_ARN,
                job_definition_arn=self._OTHER_JOB_DEFINITION_ARN,
            ),
        }

    def test_one_tracked_rule_per_job_type(self) -> None:
        stack, construct = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        assert len(_tracked_rules(template)) == 2
        assert set(construct.rules) == {"monthly-composite", "granule-processing"}

    def test_each_tracked_rule_matches_its_own_tracked_statuses(self) -> None:
        configs = self._job_type_configs()
        configs["granule-processing"] = dataclasses.replace(
            configs["granule-processing"], tracked_statuses=("SUCCEEDED", "FAILED")
        )
        stack, _ = _make_stack(job_type_configs=configs)
        statuses = {
            rule["EventPattern"]["detail"]["jobQueue"][0]: rule["EventPattern"][
                "detail"
            ]["status"]
            for rule in _tracked_rules(Template.from_stack(stack))
        }
        assert statuses == {
            _JOB_QUEUE_ARN: list(BATCH_EVENT_STATUSES),
            self._OTHER_JOB_QUEUE_ARN: ["SUCCEEDED", "FAILED"],
        }

    def test_each_tracked_rule_scoped_to_its_own_job_type(self) -> None:
        stack, _ = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        job_queues = {
            tuple(rule["EventPattern"]["detail"]["jobQueue"])
            for rule in _tracked_rules(template)
        }
        assert job_queues == {(_JOB_QUEUE_ARN,), (self._OTHER_JOB_QUEUE_ARN,)}

    def test_one_untracked_rule_per_distinct_queue(self) -> None:
        stack, construct = _make_stack(job_type_configs=self._job_type_configs())
        assert len(_untracked_rules(Template.from_stack(stack))) == 2
        assert set(construct.untracked_rules) == {
            "monthly-composite",
            "granule-processing",
        }

    def test_job_types_sharing_a_queue_share_one_untracked_rule(self) -> None:
        stack, construct = _make_stack(job_type_configs=self._shared_queue_configs())
        template = Template.from_stack(stack)
        assert len(_tracked_rules(template)) == 2
        assert len(_untracked_rules(template)) == 1
        assert set(construct.untracked_rules) == {"monthly-composite"}

    def test_shares_one_lambda_function(self) -> None:
        stack, _ = _make_stack(job_type_configs=self._job_type_configs())
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::Lambda::Function", 1)


class TestJobDefinitionOwnedByThisApp:
    """A job definition created in this app, not imported from an ARN.

    Its ARN is a token whose ref carries the revision, so the tracked
    rule's "{family_arn}:" prefix must be rebuilt from the revision-free
    name -- otherwise the prefix reads "...job-definition/name:2:" and
    matches no event at all.
    """

    def _stack(self) -> Stack:
        app = App()
        stack = Stack(
            app, "TestStack", env={"account": "123456789012", "region": "us-west-2"}
        )
        job_definition = batch.EcsJobDefinition(
            stack,
            "JobDefinition",
            job_definition_name="composites",
            container=batch.EcsEc2ContainerDefinition(
                stack,
                "Container",
                image=ecs.ContainerImage.from_registry("busybox"),
                cpu=1,
                memory=Size.mebibytes(512),
            ),
        )
        job_queue = batch.JobQueue.from_job_queue_arn(stack, "JobQueue", _JOB_QUEUE_ARN)
        JobMonitorFunction(
            stack,
            "Monitor",
            processing_bucket=s3.Bucket(stack, "Bucket"),
            job_type_configs={
                "composite": job_type_config(
                    job_queue=job_queue, job_definition=job_definition
                )
            },
        )
        return stack

    def test_rule_prefix_has_no_revision_before_its_trailing_colon(self) -> None:
        template = Template.from_stack(self._stack())
        [rule] = _tracked_rules(template)
        [matcher] = rule["EventPattern"]["detail"]["jobDefinition"]
        separator, parts = matcher["prefix"]["Fn::Join"]

        assert separator == ""
        assert parts[-1] == ":"
        # Directly between the name and that colon there must be nothing.
        assert isinstance(parts[-2], dict)
        assert "Fn::Select" in parts[-2]
        assert parts[-3].endswith(":job-definition/")


_CUSTOM_ENTRY = tempfile.mkdtemp()
Path(_CUSTOM_ENTRY, "handler.py").write_text("def handler(event, context):\n    pass\n")

_LEGACY_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/legacy"
_LEGACY_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/old"

_WITH_LEGACY_CONFIGS = {
    "monthly-composite": JobTypeConfig(
        job_queue_arn=_JOB_QUEUE_ARN, job_definition_arn=_JOB_DEFINITION_ARN
    ),
    "legacy": JobTypeConfig(
        job_queue_arn=_LEGACY_JOB_QUEUE_ARN,
        job_definition_arn=_LEGACY_JOB_DEFINITION_ARN,
        requires_bejm_parameters=False,
        route_failures=False,
    ),
}


def _make_custom_handler_stack(
    job_type_configs: dict[str, JobTypeConfig] | None = None, **kwargs: Any
) -> tuple[Stack, JobMonitorFunction]:
    """PythonFunction's Docker bundling is skipped via aws:cdk:bundling-stacks,
    as these test the construct's wiring, not a consumer's bundled code."""
    app = App(context={"aws:cdk:bundling-stacks": []})
    stack = Stack(app, "TestStack")
    construct = JobMonitorFunction(
        stack,
        "TestJobMonitorFunction",
        processing_bucket=s3.Bucket(stack, "Bucket"),
        job_type_configs=job_type_configs or _WITH_LEGACY_CONFIGS,
        entry=_CUSTOM_ENTRY,
        index="handler.py",
        **kwargs,
    )
    return stack, construct


class TestCustomHandler:
    def test_entry_without_index_raises(self) -> None:
        with pytest.raises(ValueError, match="entry and index must be given together"):
            _make_stack(entry=_CUSTOM_ENTRY)

    def test_index_without_entry_raises(self) -> None:
        with pytest.raises(ValueError, match="entry and index must be given together"):
            _make_stack(index="handler.py")

    def test_uses_the_consumer_handler(self) -> None:
        stack, _ = _make_custom_handler_stack()
        Template.from_stack(stack).has_resource_properties(
            "AWS::Lambda::Function", {"Handler": "handler.handler"}
        )

    def test_passes_through_extra_environment(self) -> None:
        stack, _ = _make_custom_handler_stack(environment={"EXTRA": "value"})
        Template.from_stack(stack).has_resource_properties(
            "AWS::Lambda::Function",
            {
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "EXTRA": "value",
                            "PROCESSING_JOB_TYPE_CONFIGS": Match.any_value(),
                        }
                    )
                }
            },
        )


class TestJobTypesWithoutBejmParameters:
    def test_require_a_custom_handler(self) -> None:
        with pytest.raises(ValueError, match="needs a custom handler"):
            _make_stack(job_type_configs=_WITH_LEGACY_CONFIGS)

    def test_tracked_rule_does_not_require_the_parameters(self) -> None:
        stack, _ = _make_custom_handler_stack()
        [legacy] = [
            rule
            for rule in _tracked_rules(Template.from_stack(stack))
            if rule["EventPattern"]["detail"]["jobQueue"] == [_LEGACY_JOB_QUEUE_ARN]
        ]
        assert "parameters" not in legacy["EventPattern"]["detail"]

    def test_job_types_requiring_parameters_still_require_them(self) -> None:
        stack, _ = _make_custom_handler_stack()
        [composite] = [
            rule
            for rule in _tracked_rules(Template.from_stack(stack))
            if rule["EventPattern"]["detail"]["jobQueue"] == [_JOB_QUEUE_ARN]
        ]
        assert composite["EventPattern"]["detail"]["parameters"] == {
            "bejm_job_type": [{"exists": True}]
        }

    def test_no_untracked_rule_for_their_queue(self) -> None:
        stack, _ = _make_custom_handler_stack()
        [untracked] = _untracked_rules(Template.from_stack(stack))
        assert untracked["EventPattern"]["detail"]["jobQueue"] == [_JOB_QUEUE_ARN]

    def test_cannot_share_a_queue_with_job_types_requiring_them(self) -> None:
        configs = {
            **_WITH_LEGACY_CONFIGS,
            "legacy": dataclasses.replace(
                _WITH_LEGACY_CONFIGS["legacy"], job_queue_arn=_JOB_QUEUE_ARN
            ),
        }
        with pytest.raises(ValueError, match="shared by job types"):
            _make_custom_handler_stack(job_type_configs=configs)
