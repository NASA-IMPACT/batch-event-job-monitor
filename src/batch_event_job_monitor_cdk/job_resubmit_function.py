"""CDK construct wrapping a consumer-authored job-resubmit Lambda.

Unlike JobMonitorFunction, this cannot bundle a fully generic handler:
resubmit_job's build_submit_job_params callback is inherently job-type
specific (job definition ARN, container overrides, etc) and stays the
consuming repo's responsibility. This construct wires the supporting
infrastructure -- the SQS event source, IAM permissions, environment --
around a Lambda entry point the consumer provides.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_lambda_python_alpha as lambda_python,
    aws_sqs as sqs,
)
from constructs import Construct


class JobResubmitFunction(Construct):
    """Wraps a consumer-authored Lambda that resubmits jobs off the retry queue.

    The consumer's own entry point imports and calls
    `batch_event_job_monitor.resubmit_job` with their own
    `build_submit_job_params` callback -- see `examples/job_resubmit_handler.py`
    for a starting point.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    entry : str
        Path to the consumer's Lambda source directory.
    index : str
        Filename (relative to entry) of the consumer's handler module.
    retry_queue : sqs.IQueue
        The retry queue monitor_job publishes to; this Lambda's event
        source.
    batch_job_queue_arn : str
        AWS Batch job queue ARN to scope the batch:SubmitJob (and
        optionally batch:DescribeJobs) IAM grant to.
    batch_job_definition_arn : str
        AWS Batch job definition ARN (without revision) to scope the IAM
        grant to.
    handler : str, optional
        Handler function name within the index module. Defaults to
        "handler".
    include_describe_jobs : bool, optional
        Also grant batch:DescribeJobs, for consumers whose
        build_submit_job_params introspects the original job. Defaults to
        True.
    environment : dict[str, str] or None, optional
        Additional environment variables for the consumer's handler (e.g.
        their own job queue/job definition ARNs).
    function_name : str or None, optional
        Explicit Lambda function name.
    memory_size : int, optional
        Lambda memory size in MB. Defaults to 256.
    timeout : Duration, optional
        Lambda timeout. Defaults to 1 minute.
    batch_size : int, optional
        SQS event source batch size. Defaults to 100.
    max_batching_window : Duration, optional
        SQS event source max batching window. Defaults to 1 minute.
    bundling : lambda_python.BundlingOptions or None, optional
        Bundling options forwarded to PythonFunction.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    function : lambda_python.PythonFunction
        The consumer's resubmit Lambda.
    event_source_mapping : event_sources.SqsEventSource
        The retry-queue event source.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        entry: str,
        index: str,
        retry_queue: sqs.IQueue,
        batch_job_queue_arn: str,
        batch_job_definition_arn: str,
        handler: str = "handler",
        include_describe_jobs: bool = True,
        environment: dict[str, str] | None = None,
        function_name: str | None = None,
        memory_size: int = 256,
        timeout: Duration = Duration.minutes(1),
        batch_size: int = 100,
        max_batching_window: Duration = Duration.minutes(1),
        bundling: lambda_python.BundlingOptions | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.function = lambda_python.PythonFunction(
            self,
            "Function",
            entry=entry,
            index=index,
            handler=handler,
            runtime=_lambda.Runtime.PYTHON_3_12,
            function_name=function_name,
            memory_size=memory_size,
            timeout=timeout,
            environment=environment,
            bundling=bundling,
        )

        actions = ["batch:SubmitJob"]
        if include_describe_jobs:
            actions.append("batch:DescribeJobs")
        self.function.add_to_role_policy(
            iam.PolicyStatement(
                actions=actions,
                resources=[batch_job_queue_arn, batch_job_definition_arn],
            )
        )

        self.event_source_mapping = event_sources.SqsEventSource(
            retry_queue,
            batch_size=batch_size,
            max_batching_window=max_batching_window,
            report_batch_item_failures=True,
        )
        self.function.add_event_source(self.event_source_mapping)
