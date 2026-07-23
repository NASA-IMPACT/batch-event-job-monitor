"""CDK construct for the batteries-included job-monitor Lambda.

Bundles its own handler (batch_event_job_monitor.handlers.job_monitor_handler)
via plain lambda.Code.from_asset -- not lambda_python_alpha.PythonFunction,
since the handler has no third-party runtime dependency beyond boto3
(already present in the Lambda managed runtime), so none of PythonFunction's
Docker-based dependency-manifest bundling is needed. entry is the parent
directory of the installed batch_event_job_monitor package, computed at
synth time, with exclude patterns restricting the zipped asset to just that
subpackage.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aws_cdk import (
    Duration,
    aws_batch as batch,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct

import batch_event_job_monitor
from batch_event_job_monitor.models import JobTypeConfig

# Reserved job_type key for the fallback JobTypeConfig applied to any
# job_type not explicitly listed in job_type_configs.
_DEFAULT_JOB_TYPE_CONFIG_KEY = "__default__"

_HANDLER_ENTRY = str(Path(batch_event_job_monitor.__file__).parent.parent)
_HANDLER_EXCLUDE = [
    "*",
    "!batch_event_job_monitor",
    "!batch_event_job_monitor/**",
    "**/__pycache__",
    "**/*.pyc",
]

# Every Batch job lifecycle status. JobMonitorFunction tracks the full
# lifecycle (not just terminal SUCCEEDED/FAILED), so ad hoc/backfill job
# submissions are tracked correctly with no other library coordination.
_ALL_BATCH_STATUSES = [
    "SUBMITTED",
    "PENDING",
    "RUNNABLE",
    "STARTING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
]


class JobMonitorFunction(Construct):
    """Batteries-included AWS Batch job-monitor Lambda.

    Bundles its own handler -- no consumer-authored Lambda code is needed
    for the common case. Consumers set JobContext.to_batch_parameters() on
    their Batch SubmitJobRequest.parameters (directly, via resubmit_job, or
    for ad hoc/backfill submissions) so this Lambda can reconstruct the
    JobContext from each EventBridge job state-change event.

    No job_type prop: one instance can monitor multiple job types sharing a
    bucket/queue, since job_type is self-describing per event.

    Classification config (retry policy, exit-code outcomes) is
    deploy-time and job_type-specific -- see JobTypeConfig -- since it's
    tied to a container image/tag, not to any individual job. There is no
    per-job or per-invocation override.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    processing_bucket : s3.IBucket
        Bucket the job monitor reads/writes canonical records, state
        pointers, and output index entries to.
    job_type_configs : dict[str, JobTypeConfig] or None, optional
        Per-job_type classification config, keyed by job_type. A job_type
        not listed here uses default_job_type_config.
    default_job_type_config : JobTypeConfig or None, optional
        Fallback classification config for any job_type not listed in
        job_type_configs. Defaults to JobTypeConfig() (max_attempts=3,
        no custom exit-code outcomes).
    retry_queue : sqs.IQueue or None, optional
        SQS queue to notify for retryable failures with attempts
        remaining. If not given, no retry routing occurs.
    dlq : sqs.IQueue or None, optional
        SQS queue to notify for terminal non-success outcomes. If not
        given, no DLQ routing occurs.
    job_queues : list[batch.IJobQueue] or None, optional
        AWS Batch job queues to scope the EventBridge rule to. If not
        given, the rule matches job state changes from any queue.
    function_name : str or None, optional
        Explicit Lambda function name.
    memory_size : int, optional
        Lambda memory size in MB. Defaults to 256.
    timeout : Duration, optional
        Lambda timeout. Defaults to 1 minute.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    function : _lambda.Function
        The bundled job-monitor Lambda.
    rule : events.Rule
        The EventBridge rule invoking the Lambda for Batch job state
        changes.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.IBucket,
        job_type_configs: dict[str, JobTypeConfig] | None = None,
        default_job_type_config: JobTypeConfig | None = None,
        retry_queue: sqs.IQueue | None = None,
        dlq: sqs.IQueue | None = None,
        job_queues: list[batch.IJobQueue] | None = None,
        function_name: str | None = None,
        memory_size: int = 256,
        timeout: Duration = Duration.minutes(1),
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        all_job_type_configs = {
            _DEFAULT_JOB_TYPE_CONFIG_KEY: default_job_type_config or JobTypeConfig(),
            **(job_type_configs or {}),
        }
        environment = {
            "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            "PROCESSING_JOB_TYPE_CONFIGS": json.dumps(
                {
                    job_type: config.to_dict()
                    for job_type, config in all_job_type_configs.items()
                }
            ),
        }
        if retry_queue is not None:
            environment["JOB_RETRY_QUEUE_URL"] = retry_queue.queue_url
        if dlq is not None:
            environment["JOB_FAILURE_DLQ_URL"] = dlq.queue_url

        self.function = _lambda.Function(
            self,
            "Function",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="batch_event_job_monitor.handlers.job_monitor_handler.handler",
            code=_lambda.Code.from_asset(_HANDLER_ENTRY, exclude=_HANDLER_EXCLUDE),
            function_name=function_name,
            memory_size=memory_size,
            timeout=timeout,
            environment=environment,
        )

        processing_bucket.grant_read_write(self.function)
        if retry_queue is not None:
            retry_queue.grant_send_messages(self.function)
        if dlq is not None:
            dlq.grant_send_messages(self.function)

        detail: dict[str, Any] = {"status": _ALL_BATCH_STATUSES}
        if job_queues is not None:
            detail["jobQueue"] = [q.job_queue_arn for q in job_queues]

        self.rule = events.Rule(
            self,
            "JobStateChangeRule",
            event_pattern=events.EventPattern(
                source=["aws.batch"],
                detail_type=["Batch Job State Change"],
                detail=detail,
            ),
            targets=[targets.LambdaFunction(self.function, retry_attempts=3)],
        )
