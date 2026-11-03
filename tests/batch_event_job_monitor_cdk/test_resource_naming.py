"""Tests for name_prefix and function descriptions across the CDK constructs."""

from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path
from typing import Any

import pytest
from aws_cdk import App, Aws, Stack, aws_glue as glue, aws_s3 as s3, aws_sqs as sqs
from aws_cdk.assertions import Template

from batch_event_job_monitor.models import JobTypeConfig
from batch_event_job_monitor_cdk.job_monitor_function import JobMonitorFunction
from batch_event_job_monitor_cdk.job_resubmit_function import JobResubmitFunction
from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues
from batch_event_job_monitor_cdk.naming import function_name, queue_name
from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec
from batch_event_job_monitor_cdk.records_rollup_function import RecordsRollupFunction
from batch_event_job_monitor_cdk.records_rollup_table import RecordsRollupTable

_CUSTOM_ENTRY = tempfile.mkdtemp()
Path(_CUSTOM_ENTRY, "handler.py").write_text("def handler(event, context):\n    pass\n")

_JOB_TYPE_CONFIGS = {
    "monthly-composite": JobTypeConfig(
        job_queue_arn="arn:aws:batch:us-west-2:123456789012:job-queue/queue",
        job_definition_arn="arn:aws:batch:us-west-2:123456789012:job-definition/def",
    )
}
_PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
]


def _stack(context: dict[str, object] | None = None) -> Stack:
    return Stack(App(context=context or {}), "TestStack")


def _properties(stack: Stack, resource_type: str) -> list[dict[str, Any]]:
    resources = Template.from_stack(stack).to_json()["Resources"]
    return [r["Properties"] for r in resources.values() if r["Type"] == resource_type]


def _queue_names(stack: Stack) -> set[str | None]:
    return {p.get("QueueName") for p in _properties(stack, "AWS::SQS::Queue")}


def _function_names(stack: Stack) -> set[str]:
    return {
        p["FunctionName"]
        for p in _properties(stack, "AWS::Lambda::Function")
        if "FunctionName" in p
    }


def _description_of(stack: Stack, name: str) -> str:
    [props] = [
        p
        for p in _properties(stack, "AWS::Lambda::Function")
        if p.get("FunctionName") == name
    ]
    return str(props["Description"])


def _job_monitor(stack: Stack, **kwargs: Any) -> JobMonitorFunction:
    return JobMonitorFunction(
        stack,
        "Monitor",
        processing_bucket=s3.Bucket(stack, "Bucket"),
        job_type_configs=_JOB_TYPE_CONFIGS,
        **kwargs,
    )


def _job_resubmit(stack: Stack, **kwargs: Any) -> JobResubmitFunction:
    return JobResubmitFunction(
        stack,
        "Resubmit",
        job_type_configs=_JOB_TYPE_CONFIGS,
        retry_queue=sqs.Queue(stack, "Retry"),
        **kwargs,
    )


def _rollup_table(stack: Stack, **kwargs: Any) -> RecordsRollupTable:
    database = glue.CfnDatabase(
        stack,
        "Database",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    return RecordsRollupTable(
        stack,
        "RollupTable",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path="s3://test-bucket/inv/records/hive/",
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=_PARTITION_KEYS,
        **kwargs,
    )


def _rollup_function(stack: Stack, **kwargs: Any) -> RecordsRollupFunction:
    return RecordsRollupFunction(
        stack,
        "Rollup",
        processing_bucket=s3.Bucket(stack, "Bucket", bucket_name="test-bucket"),
        database_name="test_db",
        rollup_table=_rollup_table(stack),
        **kwargs,
    )


class TestNameHelpers:
    @pytest.mark.parametrize("helper", [function_name, queue_name])
    def test_no_prefix_leaves_the_name_to_cloudformation(self, helper: Any) -> None:
        assert helper(None, "thing") is None

    @pytest.mark.parametrize("helper", [function_name, queue_name])
    def test_joins_prefix_and_suffix(self, helper: Any) -> None:
        assert helper("hls-dev", "thing") == "hls-dev-thing"

    def test_function_name_allows_exactly_the_lambda_limit(self) -> None:
        name = function_name("p" * 58, "thing")
        assert name is not None and len(name) == 64

    def test_function_name_rejects_over_the_lambda_limit(self) -> None:
        with pytest.raises(ValueError, match="64"):
            function_name("p" * 59, "thing")

    def test_queue_name_allows_exactly_the_sqs_limit(self) -> None:
        name = queue_name("p" * 74, "thing")
        assert name is not None and len(name) == 80

    def test_queue_name_rejects_over_the_sqs_limit(self) -> None:
        with pytest.raises(ValueError, match="80"):
            queue_name("p" * 75, "thing")

    @pytest.mark.parametrize("prefix", ["", "has space", "has.dot", "has/slash"])
    @pytest.mark.parametrize("helper", [function_name, queue_name])
    def test_rejects_invalid_prefixes(self, helper: Any, prefix: str) -> None:
        with pytest.raises(ValueError, match="name_prefix"):
            helper(prefix, "thing")

    @pytest.mark.parametrize("helper", [function_name, queue_name])
    def test_unresolved_token_prefix_skips_validation(self, helper: Any) -> None:
        assert helper(Aws.STACK_NAME, "thing") == f"{Aws.STACK_NAME}-thing"


class TestMonitoringQueues:
    def test_prefix_names_every_queue(self) -> None:
        stack = _stack()
        MonitoringQueues(stack, "Queues", name_prefix="hls-dev")
        assert _queue_names(stack) == {
            "hls-dev-retry-queue",
            "hls-dev-retry-dlq",
            "hls-dev-failure-dlq",
            "hls-dev-untracked-queue",
            "hls-dev-event-dlq",
        }

    def test_default_leaves_names_to_cloudformation(self) -> None:
        stack = _stack()
        MonitoringQueues(stack, "Queues")
        assert _queue_names(stack) == {None}


class TestJobMonitorFunction:
    def test_prefix_names_the_function(self) -> None:
        stack = _stack()
        _job_monitor(stack, name_prefix="hls-dev")
        assert "hls-dev-job-monitor" in _function_names(stack)

    def test_prefix_reaches_queues_created_here(self) -> None:
        stack = _stack()
        _job_monitor(stack, name_prefix="hls-dev")
        assert "hls-dev-retry-dlq" in _queue_names(stack)

    def test_prefix_does_not_rename_queues_passed_in(self) -> None:
        stack = _stack()
        _job_monitor(
            stack, name_prefix="hls-dev", queues=MonitoringQueues(stack, "Queues")
        )
        assert _queue_names(stack) == {None}

    def test_explicit_function_name_wins_over_prefix(self) -> None:
        stack = _stack()
        _job_monitor(stack, name_prefix="hls-dev", function_name="my-monitor")
        assert _function_names(stack) == {"my-monitor"}

    def test_default_leaves_names_to_cloudformation(self) -> None:
        stack = _stack()
        _job_monitor(stack)
        assert _function_names(stack) == set()
        assert _queue_names(stack) == {None}

    def test_bundled_handler_is_described(self) -> None:
        stack = _stack()
        _job_monitor(stack, function_name="my-monitor")
        assert "state change" in _description_of(stack, "my-monitor")

    def test_custom_handler_gets_prefix_and_description(self) -> None:
        stack = _stack({"aws:cdk:bundling-stacks": []})
        _job_monitor(
            stack, name_prefix="hls-dev", entry=_CUSTOM_ENTRY, index="handler.py"
        )
        assert "state change" in _description_of(stack, "hls-dev-job-monitor")


class TestJobResubmitFunction:
    def test_prefix_names_the_function(self) -> None:
        stack = _stack()
        _job_resubmit(stack, name_prefix="hls-dev")
        assert _function_names(stack) == {"hls-dev-job-resubmit"}

    def test_explicit_function_name_wins_over_prefix(self) -> None:
        stack = _stack()
        _job_resubmit(stack, name_prefix="hls-dev", function_name="my-resubmit")
        assert _function_names(stack) == {"my-resubmit"}

    def test_default_leaves_name_to_cloudformation(self) -> None:
        stack = _stack()
        _job_resubmit(stack)
        assert _function_names(stack) == set()

    def test_bundled_handler_is_described(self) -> None:
        stack = _stack()
        _job_resubmit(stack, function_name="my-resubmit")
        assert "retry queue" in _description_of(stack, "my-resubmit")

    def test_custom_handler_gets_prefix_and_description(self) -> None:
        stack = _stack({"aws:cdk:bundling-stacks": []})
        _job_resubmit(
            stack, name_prefix="hls-dev", entry=_CUSTOM_ENTRY, index="handler.py"
        )
        assert "retry queue" in _description_of(stack, "hls-dev-job-resubmit")


class TestRecordsRollupTable:
    def test_prefix_names_the_handler_and_its_provider(self) -> None:
        stack = _stack()
        _rollup_table(stack, name_prefix="hls-dev")
        assert _function_names(stack) == {
            "hls-dev-records-table-ddl",
            "hls-dev-records-table-ddl-provider",
        }

    def test_default_leaves_names_to_cloudformation(self) -> None:
        stack = _stack()
        _rollup_table(stack)
        assert _function_names(stack) == set()

    def test_ddl_handler_is_described(self) -> None:
        stack = _stack()
        _rollup_table(stack, name_prefix="hls-dev")
        assert "DDL" in _description_of(stack, "hls-dev-records-table-ddl")


class TestRecordsRollupFunction:
    def test_prefix_names_both_functions(self) -> None:
        stack = _stack()
        _rollup_function(stack, name_prefix="hls-dev")
        assert _function_names(stack) == {
            "hls-dev-records-rollup",
            "hls-dev-records-reconcile",
        }

    def test_prefix_names_both_queues(self) -> None:
        stack = _stack()
        _rollup_function(stack, name_prefix="hls-dev")
        assert _queue_names(stack) == {"hls-dev-rollup-queue", "hls-dev-rollup-dlq"}

    def test_default_leaves_names_to_cloudformation(self) -> None:
        stack = _stack()
        _rollup_function(stack)
        assert _function_names(stack) == set()
        assert _queue_names(stack) == {None}

    def test_descriptions_tell_the_two_functions_apart(self) -> None:
        stack = _stack()
        _rollup_function(stack, name_prefix="hls-dev")
        assert "MERGE" in _description_of(stack, "hls-dev-records-rollup")
        assert "inventory" in _description_of(stack, "hls-dev-records-reconcile")

    def test_functions_are_described_without_a_prefix(self) -> None:
        stack = _stack()
        _rollup_function(stack)
        descriptions = [
            p.get("Description", "")
            for p in _properties(stack, "AWS::Lambda::Function")
        ]
        assert any("MERGE" in d for d in descriptions)
        assert any("inventory" in d for d in descriptions)
