"""Tests for the RecordsRollupFunction CDK construct."""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
from typing import Any

from aws_cdk import App, Stack, aws_glue as glue, aws_s3 as s3
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.partition_key_spec import PartitionKeySpec
from batch_event_job_monitor_cdk.records_rollup_function import RecordsRollupFunction
from batch_event_job_monitor_cdk.records_rollup_table import RecordsRollupTable

PARTITION_KEYS = [
    PartitionKeySpec("job_type", "string", "enum", enum_values=("monthly-composite",)),
    PartitionKeySpec("tile_id", "string", "injected"),
]
INVENTORY_LOCATION = "s3://test-bucket/inv/test-bucket/records/hive/"


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
    table = RecordsRollupTable(
        stack,
        "RecordsRollup",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path=INVENTORY_LOCATION,
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
    )
    construct = RecordsRollupFunction(
        stack,
        "Rollup",
        processing_bucket=bucket,
        database_name="test_db",
        rollup_table=table,
        partition_keys=PARTITION_KEYS,
    )
    return construct, Template.from_stack(stack)


def test_both_functions_allow_recursive_invocation() -> None:
    template = _template()
    # rollup + reconcile + the DDL handler + the custom-resource provider's
    # own framework function + the BucketNotificationsHandler custom
    # resource enable_event_bridge_notification() adds.
    template.resource_count_is("AWS::Lambda::Function", 5)
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


def test_queue_retention_matches_the_dead_letter_queues() -> None:
    # SQS's 4-day default is shorter than a multi-million-key backfill
    # drain can run; without matching the DLQ's 14 days a broken chain
    # would silently expire queued keys.
    template = _template()
    queues = template.find_resources("AWS::SQS::Queue")
    retentions = {
        resource["Properties"].get("MessageRetentionPeriod")
        for resource in queues.values()
    }
    assert retentions == {14 * 24 * 60 * 60}


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


def test_rollup_partition_keys_default_to_the_rollup_tables_own() -> None:
    # A caller who omits partition_keys entirely still gets a MERGE
    # referencing exactly the table's own columns, rather than a rollup
    # function silently pointed at a table it disagrees with.
    app = App()
    stack = Stack(app, "TestStack")
    bucket = s3.Bucket(stack, "ProcessingBucket", bucket_name="test-bucket")
    database = glue.CfnDatabase(
        stack,
        "TestDatabase",
        catalog_id="123456789012",
        database_input=glue.CfnDatabase.DatabaseInputProperty(name="test_db"),
    )
    table = RecordsRollupTable(
        stack,
        "RecordsRollup",
        database=database,
        database_name="test_db",
        processing_bucket_name="test-bucket",
        records_inventory_location_s3path=INVENTORY_LOCATION,
        inventory_datetime_start=dt.datetime(2026, 1, 1, 1, 0),
        partition_keys=PARTITION_KEYS,
    )
    RecordsRollupFunction(
        stack,
        "Rollup",
        processing_bucket=bucket,
        database_name="test_db",
        rollup_table=table,
    )
    template = Template.from_stack(stack)
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
        assert variables["ROLLUP_PARTITION_KEY_NAMES"] == "job_type,tile_id"
        checked += 1
    assert checked == 2


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
        "ROLLUP_RECORDS_TABLE",
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


def _rollup_policy_statements() -> list[dict[str, Any]]:
    template = _template()
    policies = template.find_resources("AWS::IAM::Policy")
    rollup_policy = next(
        resource
        for logical_id, resource in policies.items()
        if "RollupFunction" in logical_id and "SelfInvoke" not in logical_id
    )
    statements: list[dict[str, Any]] = rollup_policy["Properties"]["PolicyDocument"][
        "Statement"
    ]
    return statements


def _reconcile_policy_statements() -> list[dict[str, Any]]:
    template = _template()
    policies = template.find_resources("AWS::IAM::Policy")
    reconcile_policy = next(
        resource
        for logical_id, resource in policies.items()
        if "ReconcileFunction" in logical_id and "SelfInvoke" not in logical_id
    )
    statements: list[dict[str, Any]] = reconcile_policy["Properties"]["PolicyDocument"][
        "Statement"
    ]
    return statements


def test_rollup_function_can_read_records_and_write_staging() -> None:
    statements = _rollup_policy_statements()
    assert _statement_touching(statements, "s3:GetObject", "records/*") is not None
    put_staging = _statement_touching(statements, "s3:PutObject", "staging/*")
    assert put_staging is not None
    actions = put_staging["Action"]
    actions = actions if isinstance(actions, list) else [actions]
    assert "s3:DeleteObject" in actions


def test_rollup_function_can_read_staging_for_the_athena_merge() -> None:
    # The MERGE's USING source is the staging Glue table, and Athena reads
    # a table's underlying data as the calling identity -- without
    # GetObject here every MERGE fails with Access Denied.
    statements = _rollup_policy_statements()
    assert _statement_touching(statements, "s3:GetObject", "staging/*") is not None


_ATHENA_RESULTS_ACTIONS = {
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
    "s3:GetBucketLocation",
    "s3:AbortMultipartUpload",
    "s3:ListBucketMultipartUploads",
    "s3:ListMultipartUploadParts",
}


def test_rollup_function_athena_results_grant_matches_the_ddl_functions() -> None:
    statements = _rollup_policy_statements()
    statement = _statement_touching(
        statements, "s3:GetBucketLocation", "athena-results/"
    )
    assert statement is not None
    assert set(statement["Action"]) == _ATHENA_RESULTS_ACTIONS


def test_reconcile_function_athena_results_grant_matches_the_ddl_functions() -> None:
    statements = _reconcile_policy_statements()
    statement = _statement_touching(
        statements, "s3:GetBucketLocation", "athena-results/"
    )
    assert statement is not None
    assert set(statement["Action"]) == _ATHENA_RESULTS_ACTIONS


def test_both_functions_can_get_the_athena_workgroup() -> None:
    for statements in (_rollup_policy_statements(), _reconcile_policy_statements()):
        statement = _statement_touching(statements, "athena:GetWorkGroup", "workgroup/")
        assert statement is not None


def _inventory_grant_resource() -> str:
    """Return the reconcile Lambda's granted S3 Inventory resource ARN.

    Returns
    -------
    str
        The single resource ARN (an IAM wildcard pattern) of the
        statement granting s3:GetObject under the S3 Inventory prefix.
    """
    statements = _reconcile_policy_statements()
    statement = _statement_touching(
        statements, "s3:GetObject", "inv/test-bucket/records"
    )
    assert statement is not None
    resource = statement["Resource"]
    assert isinstance(resource, str)
    return resource


def test_reconcile_function_can_read_the_s3_inventory_manifest() -> None:
    # reconcile_sql anti-joins the inventory table, whose underlying data
    # is a separate S3 Inventory prefix, never covered by the Iceberg
    # data/results grants.
    resource = _inventory_grant_resource()
    manifest_key = (
        "arn:aws:s3:::test-bucket/inv/test-bucket/records/"
        "hive/dt=2026-01-01-01-00/symlink.txt"
    )
    assert fnmatch.fnmatch(manifest_key, resource)


def test_reconcile_function_can_read_the_s3_inventory_data_files() -> None:
    # The symlink.txt manifests under hive/ point at Parquet files one
    # level up, under a sibling data/ prefix -- a grant scoped to hive/*
    # alone reads the manifest and then gets Access Denied on every file
    # it names. This is the assertion round 1 of review was missing.
    resource = _inventory_grant_resource()
    data_key = "arn:aws:s3:::test-bucket/inv/test-bucket/records/data/0001.parquet"
    assert fnmatch.fnmatch(data_key, resource)


def test_reconcile_function_has_no_write_access_to_table_data() -> None:
    statements = _reconcile_policy_statements()
    for statement in statements:
        actions = statement["Action"]
        actions = actions if isinstance(actions, list) else [actions]
        resources = statement["Resource"]
        resources = resources if isinstance(resources, list) else [resources]
        resource_strs = [str(r) for r in resources]
        touches_table_prefix = any("iceberg/records" in r for r in resource_strs)
        if touches_table_prefix:
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


def test_rollup_workgroup_env_var_matches_the_tables_own_workgroup() -> None:
    # RecordsRollupFunction does not create a workgroup of its own -- it
    # defaults to RecordsRollupTable's, which created exactly one, so the
    # DDL and the rollup/reconcile queries share one workgroup by
    # construction rather than by the caller coincidentally passing a
    # matching string to both constructs.
    template = _template()
    workgroups = template.find_resources("AWS::Athena::WorkGroup")
    assert len(workgroups) == 1
    (workgroup,) = workgroups.values()
    workgroup_name = workgroup["Properties"]["Name"]
    assert workgroup_name == "RecordsRollup-workgroup"

    functions = template.find_resources(
        "AWS::Lambda::Function",
        {
            "Properties": {
                "Handler": ("batch_event_job_monitor.handlers.rollup_handler.handler")
            }
        },
    )
    for resource in functions.values():
        variables = resource["Properties"]["Environment"]["Variables"]
        assert variables["ROLLUP_WORKGROUP"] == workgroup_name


def test_processing_bucket_has_eventbridge_notifications_enabled() -> None:
    # Without this the "records/" EventBridge rule never fires -- S3 only
    # emits to EventBridge when the bucket opts in. CDK applies this
    # through a Custom::S3BucketNotifications resource, not an inline
    # property on the bucket itself.
    template = _template()
    notifications = template.find_resources("Custom::S3BucketNotifications")
    assert len(notifications) == 1
    (notification,) = notifications.values()
    config = notification["Properties"]["NotificationConfiguration"]
    assert "EventBridgeConfiguration" in config


def test_staging_prefix_has_a_seven_day_expiration_lifecycle_rule() -> None:
    template = _template()
    buckets = template.find_resources("AWS::S3::Bucket")
    (bucket,) = buckets.values()
    rules = bucket["Properties"]["LifecycleConfiguration"]["Rules"]
    staging_rule = next(rule for rule in rules if rule.get("Prefix") == "staging/")
    assert staging_rule["ExpirationInDays"] == 7
    assert staging_rule["Status"] == "Enabled"


def test_both_functions_can_invoke_themselves() -> None:
    # The one permission the whole RecursiveLoop.ALLOW design depends on.
    # Scoped to logical ids containing "SelfInvoke" -- the DDL custom
    # resource's own framework role also carries an unrelated
    # lambda:InvokeFunction statement (Task 11), which a bare Action-based
    # search would otherwise also match.
    template = _template()
    policies = template.find_resources(
        "AWS::IAM::Policy",
        {
            "Properties": {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [Match.object_like({"Action": "lambda:InvokeFunction"})]
                    )
                }
            }
        },
    )
    self_invoke_policies = {
        logical_id: resource
        for logical_id, resource in policies.items()
        if "SelfInvoke" in logical_id
    }
    assert len(self_invoke_policies) == 2
    for resource in self_invoke_policies.values():
        statement = resource["Properties"]["PolicyDocument"]["Statement"][0]
        resources = statement["Resource"]
        assert isinstance(resources, list)
        assert len(resources) == 2
