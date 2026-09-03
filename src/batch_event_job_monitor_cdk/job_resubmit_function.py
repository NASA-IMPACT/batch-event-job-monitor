"""CDK construct for the job-resubmit Lambda.

Bundles a generic default handler (see
batch_event_job_monitor.handlers.job_resubmit_handler) covering the common
case -- reuse each job_type's own Batch job queue/job definition every
attempt, no custom containerOverrides -- with zero consumer-authored
Python, the same way JobMonitorFunction does.

For anything the default can't express (per-attempt container overrides, a
computed command, batch:DescribeJobs-driven introspection of the original
job), supply entry/index to wrap a Lambda you author instead -- see
docs/resubmitting-jobs.md.
"""

from __future__ import annotations

import json
from pathlib import Path
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

import batch_event_job_monitor
from batch_event_job_monitor.models import JobTypeConfig

_HANDLER_ENTRY = str(Path(batch_event_job_monitor.__file__).parent.parent)
_HANDLER_EXCLUDE = [
    "*",
    "!batch_event_job_monitor",
    "!batch_event_job_monitor/**",
    "**/__pycache__",
    "**/*.pyc",
]
_DEFAULT_HANDLER_PATH = "batch_event_job_monitor.handlers.job_resubmit_handler.handler"


class JobResubmitFunction(Construct):
    """AWS Batch job-resubmit Lambda, triggered by the retry queue.

    Bundles a generic default handler for the common case -- no
    consumer-authored Lambda code needed. Set entry and index to wrap your
    own handler instead when the default can't express what you need (see
    docs/resubmitting-jobs.md).

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    job_type_configs : dict[str, JobTypeConfig]
        Config for every job_type this Lambda may resubmit, keyed by
        job_type -- the same mapping passed to JobMonitorFunction. The
        default handler routes each resubmission to its job_type's own
        job_queue_arn/job_definition_arn; the batch:SubmitJob IAM grant is
        scoped to the union of every listed job_type's queue/job
        definition. See batch_event_job_monitor_cdk.job_type_config for a
        helper that builds a JobTypeConfig from typed CDK Batch refs.
    retry_queue : sqs.IQueue
        The retry queue monitor_job publishes to; this Lambda's event
        source.
    entry : str or None, optional
        Path to a consumer-authored Lambda source directory, overriding
        the bundled default handler. Must be given together with index.
    index : str or None, optional
        Filename (relative to entry) of the consumer's handler module.
        Must be given together with entry.
    handler : str, optional
        Handler function name within the index module. Only used when
        entry/index override the default. Defaults to "handler".
    include_describe_jobs : bool, optional
        Also grant batch:DescribeJobs, for a custom handler whose
        build_submit_job_params introspects the original job. Defaults to
        True.
    environment : dict[str, str] or None, optional
        Additional environment variables for a custom handler (e.g. extra
        config it needs beyond PROCESSING_JOB_TYPE_CONFIGS, which is
        always set).
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
        Bundling options forwarded to PythonFunction when entry/index
        override the default. Ignored otherwise.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    function : _lambda.Function
        The resubmit Lambda (the bundled default, or the consumer's own).
    event_source_mapping : event_sources.SqsEventSource
        The retry-queue event source.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        job_type_configs: dict[str, JobTypeConfig],
        retry_queue: sqs.IQueue,
        entry: str | None = None,
        index: str | None = None,
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

        if (entry is None) != (index is None):
            raise ValueError(
                "entry and index must be given together (or not at all, to use "
                "the bundled default handler)"
            )

        full_environment = {
            "PROCESSING_JOB_TYPE_CONFIGS": json.dumps(
                {
                    job_type: config.to_dict()
                    for job_type, config in job_type_configs.items()
                }
            ),
            **(environment or {}),
        }

        if entry is None:
            self.function = _lambda.Function(
                self,
                "Function",
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler=_DEFAULT_HANDLER_PATH,
                code=_lambda.Code.from_asset(_HANDLER_ENTRY, exclude=_HANDLER_EXCLUDE),
                function_name=function_name,
                memory_size=memory_size,
                timeout=timeout,
                environment=full_environment,
            )
        else:
            assert index is not None
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
                environment=full_environment,
                bundling=bundling,
            )

        actions = ["batch:SubmitJob"]
        if include_describe_jobs:
            actions.append("batch:DescribeJobs")
        resources = dict.fromkeys(
            arn
            for config in job_type_configs.values()
            for arn in (config.job_queue_arn, f"{config.job_definition_arn}:*")
        )
        self.function.add_to_role_policy(
            iam.PolicyStatement(actions=actions, resources=list(resources))
        )

        self.event_source_mapping = event_sources.SqsEventSource(
            retry_queue,
            batch_size=batch_size,
            max_batching_window=max_batching_window,
            report_batch_item_failures=True,
        )
        self.function.add_event_source(self.event_source_mapping)
