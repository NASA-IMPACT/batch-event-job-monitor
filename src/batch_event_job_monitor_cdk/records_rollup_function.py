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
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct

from .iceberg_records_table import ICEBERG_PREFIX, STAGING_PREFIX, IcebergRecordsTable
from .lambda_asset import HANDLER_ENTRY, HANDLER_EXCLUDE
from .partition_key_spec import PartitionKeySpec

RECORDS_PREFIX = "records/"
ATHENA_RESULTS_PREFIX = "athena-results/"
_ROLLUP_TIMEOUT = Duration.minutes(15)
_DLQ_MAX_RECEIVE_COUNT = 5


class RecordsRollupFunction(Construct):
    """Change-capture queue and the two Lambdas that maintain the table.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    processing_bucket : s3.IBucket
        Bucket holding records/, staging/, and the Iceberg data.
    database_name : str
        Glue database holding the rollup tables.
    iceberg_table : IcebergRecordsTable
        The table this rollup maintains. Its ``ddl_resource`` is used to
        order the table's DDL after a workgroup created here (see
        ``workgroup_name``).
    partition_keys : list[PartitionKeySpec]
        Ordered partition keys, including the leading job_type entry.
    workgroup_name : str or None, optional
        Athena workgroup the rollup/reconcile Lambdas query through. When
        None (the default), this construct creates its own workgroup with
        query results under ``processing_bucket`` and adds an explicit
        CloudFormation dependency so ``iceberg_table``'s DDL custom
        resource cannot run before the workgroup exists. Pass an existing
        workgroup's name to skip creating one -- in that case ``iceberg_
        table`` was presumably already built against the same name (a bare
        string, so CDK cannot express that coupling as a resource
        reference) and this construct assumes, but cannot verify, that the
        handed-in workgroup's query results also live under
        ``processing_bucket``; if they do not, the S3 grant for the Athena
        results location here is wrong and must be widened by the caller.
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
    workgroup : athena.CfnWorkGroup or None
        The Athena workgroup created here, or None when an existing
        workgroup name was handed in instead.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.IBucket,
        database_name: str,
        iceberg_table: IcebergRecordsTable,
        partition_keys: list[PartitionKeySpec],
        workgroup_name: str | None = None,
        rollup_schedule: events.Schedule | None = None,
        reconcile_schedule: events.Schedule | None = None,
        max_keys_per_run: int = 25000,
        max_chain_depth: int = 1000,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        resolved_workgroup_name, self.workgroup = self._resolve_workgroup(
            workgroup_name,
            processing_bucket=processing_bucket,
            iceberg_table=iceberg_table,
        )

        self.dlq = sqs.Queue(self, "RollupDlq", retention_period=Duration.days(14))
        self.queue = sqs.Queue(
            self,
            "RollupQueue",
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
            "ROLLUP_PARTITION_KEY_NAMES": ",".join(key.name for key in partition_keys),
            "ROLLUP_MAX_KEYS": str(max_keys_per_run),
            "ROLLUP_MAX_DEPTH": str(max_chain_depth),
        }

        self.rollup_function = self._create_function(
            "RollupFunction", environment, function_name=f"{self.node.id}-rollup"
        )
        self.reconcile_function = self._create_function(
            "ReconcileFunction",
            environment,
            function_name=f"{self.node.id}-reconcile",
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

    def _resolve_workgroup(
        self,
        workgroup_name: str | None,
        *,
        processing_bucket: s3.IBucket,
        iceberg_table: IcebergRecordsTable,
    ) -> tuple[str, athena.CfnWorkGroup | None]:
        """Resolve the Athena workgroup name, creating one if not handed in.

        Parameters
        ----------
        workgroup_name : str or None
            Caller-supplied workgroup name, or None to create one here.
        processing_bucket : s3.IBucket
            Bucket the created workgroup's query results are written under.
        iceberg_table : IcebergRecordsTable
            Table whose DDL custom resource must not run before a
            workgroup created here exists.

        Returns
        -------
        tuple[str, athena.CfnWorkGroup or None]
            The resolved workgroup name, and the CfnWorkGroup created here
            (None when an existing name was handed in).
        """
        if workgroup_name is not None:
            return workgroup_name, None

        name = f"{self.node.id}-records-rollup"
        workgroup = athena.CfnWorkGroup(
            self,
            "Workgroup",
            name=name,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=(
                        f"s3://{processing_bucket.bucket_name}/{ATHENA_RESULTS_PREFIX}"
                    ),
                ),
            ),
        )
        # IcebergRecordsTable takes workgroup_name as a bare string, so its
        # DDL custom resource has no CDK-level reference to this workgroup.
        # This dependency only orders the two correctly when the caller
        # also passed this same name into IcebergRecordsTable's own
        # workgroup_name.
        iceberg_table.ddl_resource.node.add_dependency(workgroup)
        return name, workgroup

    def _create_function(
        self, construct_id: str, environment: dict[str, str], *, function_name: str
    ) -> lambda_.Function:
        """Build one rollup/reconcile Lambda from the shared handler asset.

        Parameters
        ----------
        construct_id : str
            Construct id for this function, unique within the parent.
        environment : dict[str, str]
            Environment variables read by
            batch_event_job_monitor.handlers.rollup_handler.
        function_name : str
            Explicit, deterministic function name. Required (rather than
            left to CloudFormation to auto-generate) so the self-invoke
            grant below can be built from a literal ARN string instead of
            a reference to the function's own logical id -- referencing a
            just-created function's Fn::GetAtt from its own role policy
            produces a CloudFormation dependency cycle (the function
            depends on its role's policy, and the policy would depend on
            the function).

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
            function_name=function_name,
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
        self_arn = (
            f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{function_name}"
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[self_arn, f"{self_arn}:*"],
            )
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
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[f"{bucket_arn}/{STAGING_PREFIX}*"],
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=list(
                    dict.fromkeys(iceberg_data_resources + athena_results_resources)
                ),
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
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
        """
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=iceberg_data_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=athena_results_resources,
            )
        )
        function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
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
