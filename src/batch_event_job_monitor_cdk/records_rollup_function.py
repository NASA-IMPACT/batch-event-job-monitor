"""CDK construct for the records rollup and reconcile Lambdas.

Two functions share one handler asset. Splitting them keeps a long reconcile
chain from starving the hourly rollup, since each carries its own reserved
concurrency of 1.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import (
    Aws,
    Duration,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct

from .iceberg_records_table import (
    ATHENA_RESULTS_ACTIONS,
    ATHENA_RESULTS_PREFIX,
    ICEBERG_PREFIX,
    STAGING_PREFIX,
    IcebergRecordsTable,
)
from .lambda_asset import HANDLER_ENTRY, HANDLER_EXCLUDE
from .partition_key_spec import PartitionKeySpec

RECORDS_PREFIX = "records/"
_ROLLUP_TIMEOUT = Duration.minutes(15)
_DLQ_MAX_RECEIVE_COUNT = 5
_STAGING_EXPIRATION = Duration.days(7)
_INVENTORY_MANIFEST_SUFFIX = "hive/"


def _object_arn_from_s3_uri(s3_uri: str) -> str:
    """Build an S3 object ARN wildcard covering everything under a URI.

    Parameters
    ----------
    s3_uri : str
        An ``s3://bucket/prefix/`` URI. Must be a literal string (not a
        CDK token) -- the bucket and prefix are parsed out of it directly.

    Returns
    -------
    str
        ``arn:aws:s3:::bucket/prefix/*``.
    """
    without_scheme = s3_uri.removeprefix("s3://")
    bucket, _, key_prefix = without_scheme.partition("/")
    return f"arn:aws:s3:::{bucket}/{key_prefix}*"


def _inventory_data_resource(inventory_location_s3path: str) -> str:
    """Build the object ARN covering an S3 Inventory's manifest and data.

    ``inventory_location_s3path`` (e.g. from
    ``ProcessingBucket.inventory_location``) names the ``hive/`` symlink-
    manifest prefix specifically -- ``s3://bucket/prefix/{inventory-id}/
    hive/``. The symlink.txt files under that prefix point at Parquet
    files one level up, under a sibling ``data/`` prefix
    (``.../{inventory-id}/data/*.parquet``), so a grant scoped to
    ``hive/*`` alone reads the manifest fine and then gets Access Denied
    reading the data it names. Stripping the trailing ``hive/`` segment
    off the given URI (rather than reconstructing the parent path from
    its parts) keeps this derived from the one URI that actually crosses
    the interface, instead of a second, independently-assembled path that
    could drift from it.

    Parameters
    ----------
    inventory_location_s3path : str
        s3:// URI of an S3 Inventory's Hive symlink-manifest prefix,
        ending in ``hive/``.

    Returns
    -------
    str
        Object ARN wildcard covering both the ``hive/`` manifests and the
        sibling ``data/`` Parquet files.

    Raises
    ------
    ValueError
        If ``inventory_location_s3path`` does not end with the expected
        ``hive/`` manifest suffix.
    """
    if not inventory_location_s3path.endswith(_INVENTORY_MANIFEST_SUFFIX):
        raise ValueError(
            "expected an S3 Inventory location ending in "
            f"{_INVENTORY_MANIFEST_SUFFIX!r}, got {inventory_location_s3path!r}"
        )
    parent = inventory_location_s3path[: -len(_INVENTORY_MANIFEST_SUFFIX)]
    return _object_arn_from_s3_uri(parent)


class RecordsRollupFunction(Construct):
    """Change-capture queue and the two Lambdas that maintain the table.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    processing_bucket : s3.Bucket
        Bucket holding records/, staging/, and the Iceberg data. A
        concrete Bucket (not s3.IBucket) is required so this construct can
        call enable_event_bridge_notification() and add a lifecycle rule
        on it -- both are Bucket-only methods, not part of the IBucket
        interface an imported/foreign bucket would satisfy.
    database_name : str
        Glue database holding the rollup tables.
    iceberg_table : IcebergRecordsTable
        The table this rollup maintains. Its ``workgroup_name`` is this
        construct's default Athena workgroup (see ``workgroup_name``), and
        its ``inventory_location_s3path`` scopes the reconcile Lambda's
        read grant on the S3 Inventory data reconcile anti-joins against.
    partition_keys : list[PartitionKeySpec] or None, optional
        Ordered partition keys, including the leading job_type entry.
        Defaults to ``iceberg_table.partition_key_names`` -- the same
        partition keys the table's own MERGE and DDL were generated from,
        so the two constructs cannot generate a MERGE referencing columns
        the table does not have by drifting to two different lists. Pass
        an explicit list only to deliberately diverge, which is never
        correct in production.
    workgroup_name : str or None, optional
        Athena workgroup the rollup/reconcile Lambdas query through.
        Defaults to ``iceberg_table.workgroup_name`` -- the same workgroup
        the table's own DDL runs in, so both constructs share one
        workgroup (and its results location) without CDK having to infer
        that coupling from two bare strings. Pass an explicit name only to
        deliberately use a different workgroup for these two Lambdas than
        the one the table's DDL used.
    rollup_schedule : events.Schedule, optional
        Rollup cadence. Defaults to hourly.
    reconcile_schedule : events.Schedule, optional
        Reconcile cadence. Defaults to weekly.
    max_keys_per_run : int, optional
        Per-run cap on distinct keys. Defaults to 25000.
    max_chain_depth : int, optional
        Maximum self-invocation chain length. Defaults to 1000.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    queue : sqs.Queue
        Rollup queue fed by S3 EventBridge notifications.
    dlq : sqs.Queue
        Dead-letter queue for poison notifications.
    rollup_function : lambda_.Function
        The scheduled rollup Lambda.
    reconcile_function : lambda_.Function
        The scheduled reconcile Lambda.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.Bucket,
        database_name: str,
        iceberg_table: IcebergRecordsTable,
        partition_keys: list[PartitionKeySpec] | None = None,
        workgroup_name: str | None = None,
        rollup_schedule: events.Schedule | None = None,
        reconcile_schedule: events.Schedule | None = None,
        max_keys_per_run: int = 25000,
        max_chain_depth: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        resolved_workgroup_name = (
            workgroup_name
            if workgroup_name is not None
            else iceberg_table.workgroup_name
        )
        partition_key_names = (
            [key.name for key in partition_keys]
            if partition_keys is not None
            else iceberg_table.partition_key_names
        )

        processing_bucket.enable_event_bridge_notification()
        processing_bucket.add_lifecycle_rule(
            prefix=STAGING_PREFIX, expiration=_STAGING_EXPIRATION
        )

        self.dlq = sqs.Queue(self, "RollupDlq", retention_period=Duration.days(14))
        self.queue = sqs.Queue(
            self,
            "RollupQueue",
            # Matches the DLQ's retention: a multi-million-key backfill
            # drain can run longer than SQS's 4-day default, and a broken
            # chain would otherwise let queued keys silently expire.
            retention_period=Duration.days(14),
            visibility_timeout=_ROLLUP_TIMEOUT.plus(Duration.minutes(1)),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=_DLQ_MAX_RECEIVE_COUNT, queue=self.dlq
            ),
        )

        events.Rule(
            self,
            "RecordWrittenRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [processing_bucket.bucket_name]},
                    "object": {"key": [{"prefix": RECORDS_PREFIX}]},
                },
            ),
            targets=[targets.SqsQueue(self.queue)],
        )

        environment = {
            "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            "ROLLUP_QUEUE_URL": self.queue.queue_url,
            "ROLLUP_STAGING_PREFIX": STAGING_PREFIX,
            "ROLLUP_DATABASE": database_name,
            "ROLLUP_ICEBERG_TABLE": iceberg_table.iceberg_table_name,
            "ROLLUP_STAGING_TABLE": iceberg_table.staging_table_name,
            "ROLLUP_INVENTORY_TABLE": iceberg_table.inventory_table_name,
            "ROLLUP_WORKGROUP": resolved_workgroup_name,
            "ROLLUP_PARTITION_KEY_NAMES": ",".join(partition_key_names),
            "ROLLUP_MAX_KEYS": str(max_keys_per_run),
            "ROLLUP_MAX_DEPTH": str(max_chain_depth),
        }

        self.rollup_function = self._create_function("RollupFunction", environment)
        self.reconcile_function = self._create_function(
            "ReconcileFunction", environment
        )

        bucket_arn = f"arn:aws:s3:::{processing_bucket.bucket_name}"
        workgroup_arn = (
            f"arn:aws:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:"
            f"workgroup/{resolved_workgroup_name}"
        )
        glue_arns = [
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{database_name}",
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{database_name}/*",
        ]
        iceberg_data_resources = [bucket_arn, f"{bucket_arn}/{ICEBERG_PREFIX}*"]
        athena_results_resources = [
            bucket_arn,
            f"{bucket_arn}/{ATHENA_RESULTS_PREFIX}*",
        ]
        inventory_resource = _inventory_data_resource(
            iceberg_table.inventory_location_s3path
        )

        self._grant_rollup(
            self.rollup_function,
            bucket_arn=bucket_arn,
            workgroup_arn=workgroup_arn,
            glue_arns=glue_arns,
            iceberg_data_resources=iceberg_data_resources,
            athena_results_resources=athena_results_resources,
        )
        self._grant_reconcile(
            self.reconcile_function,
            workgroup_arn=workgroup_arn,
            glue_arns=glue_arns,
            iceberg_data_resources=iceberg_data_resources,
            athena_results_resources=athena_results_resources,
            inventory_resource=inventory_resource,
        )

        self.queue.grant_consume_messages(self.rollup_function)
        self.queue.grant_send_messages(self.reconcile_function)

        events.Rule(
            self,
            "RollupSchedule",
            schedule=rollup_schedule or events.Schedule.rate(Duration.hours(1)),
            targets=[
                targets.LambdaFunction(
                    self.rollup_function,
                    event=events.RuleTargetInput.from_object(
                        {"mode": "rollup", "depth": 0}
                    ),
                )
            ],
        )
        events.Rule(
            self,
            "ReconcileSchedule",
            schedule=reconcile_schedule or events.Schedule.rate(Duration.days(7)),
            targets=[
                targets.LambdaFunction(
                    self.reconcile_function,
                    event=events.RuleTargetInput.from_object(
                        {"mode": "reconcile", "depth": 0}
                    ),
                )
            ],
        )

    def _create_function(
        self, construct_id: str, environment: dict[str, str]
    ) -> lambda_.Function:
        """Build one rollup/reconcile Lambda from the shared handler asset.

        Parameters
        ----------
        construct_id : str
            Construct id for this function, unique within the parent.
        environment : dict[str, str]
            Environment variables read by
            batch_event_job_monitor.handlers.rollup_handler.

        Returns
        -------
        lambda_.Function
            The created function, already granted permission to invoke
            itself for the self-invocation drain/reconcile chain.
        """
        function = lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="batch_event_job_monitor.handlers.rollup_handler.handler",
            code=lambda_.Code.from_asset(HANDLER_ENTRY, exclude=HANDLER_EXCLUDE),
            timeout=_ROLLUP_TIMEOUT,
            memory_size=1024,
            environment=environment,
            reserved_concurrent_executions=1,
            # The default, TERMINATE, cuts a self-invoking chain off at 16
            # invocations, which would silently stall a backfill drain far
            # short of completion. ALLOW is only safe together with the
            # other three compensating controls set here and in the
            # handler: retry_attempts=0, reserved concurrency of 1, the
            # max-depth counter, and the ChainDepth metric.
            recursive_loop=lambda_.RecursiveLoop.ALLOW,
            retry_attempts=0,
        )
        # A self-invoke grant added to the function's own *default* role
        # policy creates a CloudFormation dependency cycle: the function
        # depends on its role's default policy, and a statement naming the
        # function's own ARN makes the policy depend back on the function.
        # A separate, non-default iam.Policy resource sits outside that
        # cycle while still attaching to the same role.
        assert function.role is not None
        iam.Policy(
            self,
            f"{construct_id}SelfInvoke",
            statements=[
                iam.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        function.function_arn,
                        f"{function.function_arn}:*",
                    ],
                )
            ],
            roles=[function.role],
        )
        return function

    def _grant_rollup(
        self,
        function: lambda_.Function,
        *,
        bucket_arn: str,
        workgroup_arn: str,
        glue_arns: list[str],
        iceberg_data_resources: list[str],
        athena_results_resources: list[str],
    ) -> None:
        """Grant the rollup Lambda's full read/write/commit permissions.

        Parameters
        ----------
        function : lambda_.Function
            The rollup Lambda.
        bucket_arn : str
            ARN of the processing bucket.
        workgroup_arn : str
            ARN of the Athena workgroup rollup queries run through.
        glue_arns : list[str]
            Catalog/database/table ARNs for the rollup database.
        iceberg_data_resources : list[str]
            Bucket and object ARNs covering the Iceberg table's data
            location.
        athena_results_resources : list[str]
            Bucket and object ARNs covering the Athena results location.
        """
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{bucket_arn}/{RECORDS_PREFIX}*"],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                # GetObject is required alongside the writes: the Athena
                # MERGE's USING source is the staging Glue table
                # (rollup_schema.py), and Athena reads a table's
                # underlying data as the calling identity, not a separate
                # service role.
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                resources=[f"{bucket_arn}/{STAGING_PREFIX}*"],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=iceberg_data_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=ATHENA_RESULTS_ACTIONS,
                resources=athena_results_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                ],
                resources=[workgroup_arn],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"],
                resources=glue_arns,
            )
        )

    def _grant_reconcile(
        self,
        function: lambda_.Function,
        *,
        workgroup_arn: str,
        glue_arns: list[str],
        iceberg_data_resources: list[str],
        athena_results_resources: list[str],
        inventory_resource: str,
    ) -> None:
        """Grant the reconcile Lambda's read-only permissions.

        No write access to the Iceberg data location: reconcile only
        anti-joins the inventory against the catalog and re-enqueues drift
        for the rollup Lambda to merge.

        Parameters
        ----------
        function : lambda_.Function
            The reconcile Lambda.
        workgroup_arn : str
            ARN of the Athena workgroup reconcile queries run through.
        glue_arns : list[str]
            Catalog/database/table ARNs for the rollup database.
        iceberg_data_resources : list[str]
            Bucket and object ARNs covering the Iceberg table's data
            location.
        athena_results_resources : list[str]
            Bucket and object ARNs covering the Athena results location.
        inventory_resource : str
            Object ARN covering both the S3 Inventory's hive/ manifest
            prefix and its sibling data/ prefix -- see
            _inventory_data_resource.
        """
        function.add_to_role_policy(
            iam.PolicyStatement(
                # s3:ListBucket here is bucket-wide, not scoped to the
                # Iceberg data prefix, because reconcile_sql's anti-join
                # reads the records/ S3 Inventory through
                # SymlinkTextInputFormat, which needs Athena to LIST the
                # inventory prefix -- a prefix outside iceberg_data_resources
                # and not otherwise covered by inventory_resource, which
                # only grants GetObject. Scoping this down would break
                # reconcile with an Access Denied that does not point at
                # this cause.
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=iceberg_data_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[inventory_resource],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=ATHENA_RESULTS_ACTIONS,
                resources=athena_results_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                ],
                resources=[workgroup_arn],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable"],
                resources=glue_arns,
            )
        )
