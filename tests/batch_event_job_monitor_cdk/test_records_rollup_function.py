"""Tests for the RecordsRollupFunction CDK construct."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from aws_cdk import App, Stack, aws_glue as glue, aws_s3 as s3
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.iceberg_records_table import IcebergRecordsTable
from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec
from batch_event_job_monitor_cdk.records_rollup_function import RecordsRollupFunction

PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "injected"),
]


def _contains_fragment(value: Any, fragment: str) -> bool:
    """Return whether a token-bearing template value contains a substring.

    A bucket-derived ARN synthesizes as an Fn::Join rather than a plain
    string, so exact/regex string matching on the rendered JSON value does
    not work. Searching the JSON-serialized value for the literal fragment
    does, since Fn::Join's literal segments still appear verbatim in it.
    """
    return fragment in json.dumps(value)


def _statement_touching(
    statements: list[dict[str, Any]], action: str, resource_fragment: str
) -> dict[str, Any] | None:
    """Find the first IAM statement granting ``action`` on a resource.

    Parameters
    ----------
    statements : list[dict[str, Any]]
        Statement list from a synthesized AWS::IAM::Policy's PolicyDocument.
    action : str
        Action that must appear in the statement's Action field.
    resource_fragment : str
        Literal substring the statement's Resource field must contain.

    Returns
    -------
    dict[str, Any] or None
        The matching statement, or None if none matched.
    """
    for statement in statements:
        actions = statement["Action"]
        actions = actions if isinstance(actions, list) else [actions]
        if action in actions and _contains_fragment(
            statement["Resource"], resource_fragment
        ):
            return statement
    return None


def _template() -> Template:
    _, template = _construct()
    return template


def _construct() -> tuple[RecordsRollupFunction, Template]:
    app = App()
    stack = Stack(app, "TestStack")
    bucket = s3.Bucket(stack, "ProcessingBucket", bucket_name="test-bucket")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    table = IcebergRecordsTable(
        stack,
        "IcebergRecords",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path="s3://test-bucket/inv/test-bucket/records/hive/",
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
    )
    construct = RecordsRollupFunction(
        stack,
        "Rollup",
        processing_bucket=bucket,
        database_name="test_db",
        iceberg_table=table,
        partition_keys=PARTITION_KEYS,
    )
    return construct, Template.from_stack(stack)


def test_both_functions_allow_recursive_invocation() -> None:
    template = _template()
    # rollup + reconcile + the DDL handler + the custom-resource provider's
    # own framework function.
    template.resource_count_is("AWS::Lambda::Function", 4)
    functions = template.find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"RecursiveLoop": "Allow"}},
    )
    assert len(functions) == 2


def test_both_functions_are_serialized_by_reserved_concurrency() -> None:
    functions = _template().find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"ReservedConcurrentExecutions": 1}},
    )
    assert len(functions) == 2


def test_async_invocations_are_not_retried() -> None:
    functions = _template().find_resources(
        "AWS::Lambda::EventInvokeConfig",
        {"Properties": {"MaximumRetryAttempts": 0}},
    )
    assert len(functions) == 2


def test_queue_visibility_exceeds_the_rollup_timeout() -> None:
    _template().has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like({"VisibilityTimeout": 960}),
    )


def test_dead_letter_queue_is_wired_with_a_receive_count() -> None:
    _template().has_resource_properties(
        "AWS::SQS::Queue",
        Match.object_like({"RedrivePolicy": Match.object_like({"maxReceiveCount": 5})}),
    )


def test_eventbridge_rule_matches_only_the_records_prefix() -> None:
    _template().has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like(
            {
                "EventPattern": Match.object_like(
                    {
                        "source": ["aws.s3"],
                        "detail": Match.object_like(
                            {"object": {"key": [{"prefix": "records/"}]}}
                        ),
                    }
                )
            }
        ),
    )


def test_rollup_is_scheduled_hourly_and_reconcile_weekly() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(1 hour)"}),
    )
    template.has_resource_properties(
        "AWS::Events::Rule",
        Match.object_like({"ScheduleExpression": "rate(7 days)"}),
    )


def test_construct_exposes_the_required_attributes() -> None:
    construct, _ = _construct()
    assert construct.queue is not None
    assert construct.dlq is not None
    assert construct.rollup_function is not None
    assert construct.reconcile_function is not None


def test_both_functions_carry_every_rollup_handler_env_var() -> None:
    template = _template()
    expected_keys = {
        "PROCESSING_BUCKET_NAME",
        "ROLLUP_QUEUE_URL",
        "ROLLUP_STAGING_PREFIX",
        "ROLLUP_DATABASE",
        "ROLLUP_ICEBERG_TABLE",
        "ROLLUP_STAGING_TABLE",
        "ROLLUP_INVENTORY_TABLE",
        "ROLLUP_WORKGROUP",
        "ROLLUP_PARTITION_KEY_NAMES",
        "ROLLUP_MAX_KEYS",
        "ROLLUP_MAX_DEPTH",
    }
    functions = template.find_resources(
        "AWS::Lambda::Function",
        {
            "Properties": {
                "Handler": ("batch_event_job_monitor.handlers.rollup_handler.handler")
            }
        },
    )
    checked = 0
    for resource in functions.values():
        variables = resource["Properties"]["Environment"]["Variables"]
        assert expected_keys <= set(variables.keys())
        assert "AWS_LAMBDA_FUNCTION_NAME" not in variables
        checked += 1
    assert checked == 2


def test_rollup_function_can_read_records_and_write_staging() -> None:
    template = _template()
    policies = template.find_resources("AWS::IAM::Policy")
    rollup_policy = next(
        resource
        for logical_id, resource in policies.items()
        if "RollupFunction" in logical_id
    )
    statements = rollup_policy["Properties"]["PolicyDocument"]["Statement"]
    assert _statement_touching(statements, "s3:GetObject", "records/*") is not None
    put_staging = _statement_touching(statements, "s3:PutObject", "staging/*")
    assert put_staging is not None
    actions = put_staging["Action"]
    assert "s3:DeleteObject" in (actions if isinstance(actions, list) else [actions])


def test_reconcile_function_has_no_write_access_to_iceberg_data() -> None:
    template = _template()
    policies = template.find_resources("AWS::IAM::Policy")
    reconcile_policy = None
    for logical_id, resource in policies.items():
        if "ReconcileFunction" in logical_id:
            reconcile_policy = resource
            break
    assert reconcile_policy is not None
    statements = reconcile_policy["Properties"]["PolicyDocument"]["Statement"]
    for statement in statements:
        actions = statement["Action"]
        actions = actions if isinstance(actions, list) else [actions]
        resources = statement["Resource"]
        resources = resources if isinstance(resources, list) else [resources]
        resource_strs = [str(r) for r in resources]
        touches_iceberg_prefix = any("iceberg/records" in r for r in resource_strs)
        if touches_iceberg_prefix:
            assert "s3:PutObject" not in actions
            assert "s3:DeleteObject" not in actions


def test_glue_and_athena_grants_are_arn_scoped_not_wildcard() -> None:
    template = _template()
    policies = template.find_resources("AWS::IAM::Policy")
    for resource in policies.values():
        for statement in resource["Properties"]["PolicyDocument"]["Statement"]:
            actions = statement["Action"]
            actions = actions if isinstance(actions, list) else [actions]
            if any(a.startswith("glue:") or a.startswith("athena:") for a in actions):
                resources = statement["Resource"]
                resources = resources if isinstance(resources, list) else [resources]
                assert "*" not in resources


def test_a_new_athena_workgroup_is_created_with_results_under_the_bucket() -> None:
    template = _template()
    workgroups = template.find_resources("AWS::Athena::WorkGroup")
    assert len(workgroups) == 1
    (workgroup,) = workgroups.values()
    output_location = workgroup["Properties"]["WorkGroupConfiguration"][
        "ResultConfiguration"
    ]["OutputLocation"]
    assert _contains_fragment(output_location, "s3://")
    assert _contains_fragment(output_location, "/athena-results/")
