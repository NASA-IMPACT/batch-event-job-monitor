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
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
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
    bucket, since job_type is self-describing per event. Each job_type's
    own JobTypeConfig carries its queue/job-definition identity, used both
    to scope that job_type's EventBridge rule and (see JobResubmitFunction)
    to route resubmissions.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    processing_bucket : s3.IBucket
        Bucket the job monitor reads/writes canonical records, state
        pointers, and output index entries to.
    job_type_configs : dict[str, JobTypeConfig]
        Config for every job_type this construct monitors, keyed by
        job_type. A job_type not listed here is never matched by any
        EventBridge rule this construct creates, so its events never reach
        the Lambda. See batch_event_job_monitor_cdk.job_type_config for a
        helper that builds a JobTypeConfig from typed CDK Batch refs.
    retry_queue : sqs.IQueue or None, optional
        SQS queue to notify for retryable failures with attempts
        remaining. If not given, no retry routing occurs.
    dlq : sqs.IQueue or None, optional
        SQS queue to notify for terminal non-success outcomes. If not
        given, no DLQ routing occurs.
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
    rules : dict[str, events.Rule]
        The EventBridge rule invoking the Lambda for each job_type's own
        Batch job queue/job definition, keyed by job_type.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.IBucket,
        job_type_configs: dict[str, JobTypeConfig],
        retry_queue: sqs.IQueue | None = None,
        dlq: sqs.IQueue | None = None,
        function_name: str | None = None,
        memory_size: int = 256,
        timeout: Duration = Duration.minutes(1),
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        environment = {
            "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            "PROCESSING_JOB_TYPE_CONFIGS": json.dumps(
                {
                    job_type: config.to_dict()
                    for job_type, config in job_type_configs.items()
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

        # One rule per job_type, scoped to that job_type's own queue/job
        # definition -- jobDefinition matches by prefix since the event's
        # own jobDefinition field is revision-suffixed even though
        # JobTypeConfig.job_definition_arn is the family ARN.
        self.rules = {
            job_type: events.Rule(
                self,
                f"JobStateChangeRule{job_type}",
                event_pattern=events.EventPattern(
                    source=["aws.batch"],
                    detail_type=["Batch Job State Change"],
                    detail={
                        "status": _ALL_BATCH_STATUSES,
                        "jobQueue": [config.job_queue_arn],
                        "jobDefinition": [{"prefix": f"{config.job_definition_arn}:"}],
                    },
                ),
                targets=[targets.LambdaFunction(self.function, retry_attempts=3)],
            )
            for job_type, config in job_type_configs.items()
        }
