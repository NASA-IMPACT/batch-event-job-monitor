"""CDK construct for the batteries-included job-monitor Lambda.

Bundles its own handler (batch_event_job_monitor.handlers.job_monitor_handler)
via plain lambda.Code.from_asset -- not lambda_python_alpha.PythonFunction,
since the handler has no third-party runtime dependency beyond boto3
(already present in the Lambda managed runtime), so none of PythonFunction's
Docker-based dependency-manifest bundling is needed. entry is the parent
directory of the installed batch_event_job_monitor package, computed at
synth time, with exclude patterns restricting the zipped asset to just that
subpackage.

A consumer-authored handler (entry/index, e.g. wrapping
job_monitor_handler.make_handler with an untracked-job resolver) is built
with PythonFunction instead, the same way JobResubmitFunction's override is.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from aws_cdk import (
    Duration,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as lambda_python,
    aws_s3 as s3,
)
from constructs import Construct

from batch_event_job_monitor.models import (
    BATCH_EVENT_STATUSES,
    PARAM_PREFIX,
    JobTypeConfig,
)
from batch_event_job_monitor_cdk.lambda_asset import HANDLER_ENTRY, HANDLER_EXCLUDE
from batch_event_job_monitor_cdk.monitoring_queues import MonitoringQueues

# Terminal statuses every tracked rule must match: without them no job
# reaches a terminal state, so nothing is retried or dead-lettered.
_REQUIRED_TRACKED_STATUSES = ("SUCCEEDED", "FAILED")

# Statuses the untracked-job catch-all rule matches. Enough to record how an
# unmonitored job ended -- an untracked job has no state to track through.
_UNTRACKED_BATCH_STATUSES = ["SUCCEEDED", "FAILED"]

# EventBridge existence test on the one bejm_* parameter every monitored
# job must carry. Splits each monitored queue's events into the tracked
# rules and the untracked catch-all, so exactly one path handles each job.
_JOB_TYPE_PARAM_PRESENT = {f"{PARAM_PREFIX}job_type": [{"exists": True}]}
_JOB_TYPE_PARAM_ABSENT = {f"{PARAM_PREFIX}job_type": [{"exists": False}]}

_DEFAULT_HANDLER_PATH = "batch_event_job_monitor.handlers.job_monitor_handler.handler"


class JobMonitorFunction(Construct):
    """Batteries-included AWS Batch job-monitor Lambda.

    Bundles its own handler -- no consumer-authored Lambda code is needed
    for the common case. Consumers set JobGroup.to_batch_parameters() on
    their Batch SubmitJobRequest.parameters (directly, via resubmit_job, or
    for ad hoc/backfill submissions) so this Lambda can reconstruct the
    JobGroup from each EventBridge job state-change event.

    No job_type prop: one instance can monitor multiple job types sharing a
    bucket, since job_type is self-describing per event. Each job_type's
    own JobTypeConfig carries its queue/job-definition identity, used both
    to scope that job_type's EventBridge rule and (see JobResubmitFunction)
    to route resubmissions.

    Every failure path terminates in one of the MonitoringQueues rather
    than dropping an event: target failures go to the EventBridge DLQ, and
    a job submitted to a monitored queue without the bejm_* parameters is
    recorded to the untracked queue by a per-queue catch-all rule instead
    of passing unnoticed.

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
        tracked EventBridge rule this construct creates. Its events still
        reach the untracked queue if it shares a Batch job queue with a
        monitored job_type. See
        batch_event_job_monitor_cdk.job_type_config for a helper that
        builds a JobTypeConfig from typed CDK Batch refs.
    queues : MonitoringQueues or None, optional
        The retry/dead-letter/untracked queues to route to. One is created
        as a child of this construct if not given. Pass an explicit
        MonitoringQueues to share queues with another construct or to
        adopt queues you already own.
    metric_namespace : str, optional
        CloudWatch namespace the handler publishes its embedded-metric
        counters to. Defaults to "BatchEventJobMonitor".
    function_name : str or None, optional
        Explicit Lambda function name.
    memory_size : int, optional
        Lambda memory size in MB. Defaults to 256.
    timeout : Duration, optional
        Lambda timeout. Defaults to 1 minute.
    retry_attempts : int, optional
        EventBridge target retries before an event is sent to the
        EventBridge DLQ. Defaults to 3.
    entry : str or None, optional
        Path to a consumer-authored Lambda source directory, overriding
        the bundled default handler -- typically a module wrapping
        job_monitor_handler.make_handler(resolve_untracked=...). Required
        when any job_type sets requires_bejm_parameters=False. Must be
        given together with index.
    index : str or None, optional
        Filename (relative to entry) of the consumer's handler module.
        Must be given together with entry.
    handler : str, optional
        Handler function name within the index module. Only used when
        entry/index override the default. Defaults to "handler".
    bundling : lambda_python.BundlingOptions or None, optional
        Bundling options forwarded to PythonFunction when entry/index
        override the default. Ignored otherwise.
    environment : dict[str, str] or None, optional
        Additional environment variables for a custom handler.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    function : _lambda.Function
        The bundled job-monitor Lambda.
    queues : MonitoringQueues
        The queues this monitor routes to, whether passed in or created
        here.
    rules : dict[str, events.Rule]
        The EventBridge rule invoking the Lambda for each job_type's own
        Batch job queue/job definition, keyed by job_type.
    untracked_rules : dict[str, events.Rule]
        The catch-all rule for each distinct Batch job queue whose job
        types require the bejm_* parameters, keyed by the first job_type
        using that queue.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        processing_bucket: s3.IBucket,
        job_type_configs: dict[str, JobTypeConfig],
        key_prefix: str = "",
        queues: MonitoringQueues | None = None,
        metric_namespace: str = "BatchEventJobMonitor",
        function_name: str | None = None,
        memory_size: int = 256,
        timeout: Duration = Duration.minutes(1),
        retry_attempts: int = 3,
        entry: str | None = None,
        index: str | None = None,
        handler: str = "handler",
        bundling: lambda_python.BundlingOptions | None = None,
        environment: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        for job_type, config in job_type_configs.items():
            _validate_tracked_statuses(job_type, config.tracked_statuses)
        _validate_handler(job_type_configs, entry=entry, index=index)

        self.queues = queues or MonitoringQueues(self, "Queues")

        full_environment = {
            "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            **({"PROCESSING_KEY_PREFIX": key_prefix} if key_prefix else {}),
            "PROCESSING_JOB_TYPE_CONFIGS": json.dumps(
                {
                    job_type: config.to_dict()
                    for job_type, config in job_type_configs.items()
                }
            ),
            "JOB_RETRY_QUEUE_URL": self.queues.retry_queue.queue_url,
            "JOB_FAILURE_DLQ_URL": self.queues.failure_dlq.queue_url,
            "MONITOR_METRIC_NAMESPACE": metric_namespace,
            **(environment or {}),
        }

        self.function: _lambda.Function
        if entry is None:
            self.function = _lambda.Function(
                self,
                "Function",
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler=_DEFAULT_HANDLER_PATH,
                code=_lambda.Code.from_asset(HANDLER_ENTRY, exclude=HANDLER_EXCLUDE),
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

        processing_bucket.grant_read_write(self.function)
        self.queues.retry_queue.grant_send_messages(self.function)
        self.queues.failure_dlq.grant_send_messages(self.function)

        lambda_target = targets.LambdaFunction(
            self.function,
            retry_attempts=retry_attempts,
            dead_letter_queue=self.queues.event_dlq,
        )

        # One rule per job_type, scoped to that job_type's own queue/job
        # definition -- jobDefinition matches by prefix since the event's
        # own jobDefinition field is revision-suffixed even though
        # JobTypeConfig.job_definition_arn is the family ARN. A job_type
        # whose jobs carry no bejm_* parameters matches every event for its
        # queue/job definition instead, for the handler's resolver to infer.
        self.rules = {
            job_type: events.Rule(
                self,
                f"JobStateChangeRule{job_type}",
                event_pattern=events.EventPattern(
                    source=["aws.batch"],
                    detail_type=["Batch Job State Change"],
                    detail={
                        "status": list(config.tracked_statuses),
                        "jobQueue": [config.job_queue_arn],
                        "jobDefinition": [{"prefix": f"{config.job_definition_arn}:"}],
                        **(
                            {"parameters": _JOB_TYPE_PARAM_PRESENT}
                            if config.requires_bejm_parameters
                            else {}
                        ),
                    },
                ),
                targets=[lambda_target],
            )
            for job_type, config in job_type_configs.items()
        }

        self.untracked_rules = {
            job_type: events.Rule(
                self,
                f"UntrackedJobsRule{job_type}",
                event_pattern=events.EventPattern(
                    source=["aws.batch"],
                    detail_type=["Batch Job State Change"],
                    detail={
                        "status": _UNTRACKED_BATCH_STATUSES,
                        "jobQueue": [job_queue_arn],
                        "parameters": _JOB_TYPE_PARAM_ABSENT,
                    },
                ),
                # Two independent targets: the Lambda logs the job and
                # emits the UntrackedJobs metric, and SQS keeps the raw
                # event for recovery/replay whether or not the Lambda
                # succeeded.
                targets=[
                    lambda_target,
                    targets.SqsQueue(self.queues.untracked_queue),
                ],
            )
            for job_type, job_queue_arn in _first_job_type_per_queue(
                {
                    job_type: config
                    for job_type, config in job_type_configs.items()
                    if config.requires_bejm_parameters
                }
            ).items()
        }


def _validate_tracked_statuses(job_type: str, tracked_statuses: Sequence[str]) -> None:
    """Reject statuses Batch never emits, and any list missing a terminal one.

    Raises
    ------
    ValueError
        If a status is not in BATCH_EVENT_STATUSES, or SUCCEEDED or FAILED
        is absent.
    """
    unknown = [s for s in tracked_statuses if s not in BATCH_EVENT_STATUSES]
    if unknown:
        raise ValueError(
            f"job_type {job_type!r}: tracked_statuses {unknown} are not statuses "
            f"AWS Batch sends events for; choose from {list(BATCH_EVENT_STATUSES)}"
        )
    missing = [s for s in _REQUIRED_TRACKED_STATUSES if s not in tracked_statuses]
    if missing:
        raise ValueError(
            f"job_type {job_type!r}: tracked_statuses must include {missing}: "
            "without terminal statuses no job is ever recorded as finished"
        )


def _validate_handler(
    job_type_configs: dict[str, JobTypeConfig],
    *,
    entry: str | None,
    index: str | None,
) -> None:
    """Reject handler and job_type combinations that cannot track every job.

    Raises
    ------
    ValueError
        If only one of entry/index is given; if a job_type sets
        requires_bejm_parameters=False without a custom handler to resolve
        its jobs (the bundled one records them all as untracked); or if a
        Batch job queue is shared by job types that do and do not require
        the bejm_* parameters, where the queue's untracked catch-all rule
        would also match every job of the latter.
    """
    if (entry is None) != (index is None):
        raise ValueError(
            "entry and index must be given together (or not at all, to use "
            "the bundled default handler)"
        )

    without_parameters = sorted(
        job_type
        for job_type, config in job_type_configs.items()
        if not config.requires_bejm_parameters
    )
    if without_parameters and entry is None:
        raise ValueError(
            f"job types {without_parameters} set requires_bejm_parameters=False, "
            "which needs a custom handler built with "
            "job_monitor_handler.make_handler(resolve_untracked=...) -- pass "
            "its entry/index"
        )

    requirement_by_queue: dict[str, set[bool]] = {}
    for config in job_type_configs.values():
        requirement_by_queue.setdefault(config.job_queue_arn, set()).add(
            config.requires_bejm_parameters
        )
    mixed = sorted(
        queue
        for queue, requirement in requirement_by_queue.items()
        if len(requirement) > 1
    )
    if mixed:
        raise ValueError(
            f"job queues {mixed} are shared by job types that do and do not "
            "require the bejm_* parameters; give each kind its own queue"
        )


def _first_job_type_per_queue(
    job_type_configs: dict[str, JobTypeConfig],
) -> dict[str, str]:
    """Map each distinct Batch job queue ARN to the first job_type using it.

    Keying the catch-all rules by job_type rather than by position keeps a
    rule's logical id stable when unrelated job types are added or
    reordered.

    Parameters
    ----------
    job_type_configs : dict[str, JobTypeConfig]
        Config for every monitored job_type, keyed by job_type.

    Returns
    -------
    dict[str, str]
        job_type -> job_queue_arn, one entry per distinct queue ARN.
    """
    by_queue: dict[str, str] = {}
    for job_type, config in job_type_configs.items():
        by_queue.setdefault(config.job_queue_arn, job_type)
    return {job_type: queue_arn for queue_arn, job_type in by_queue.items()}
