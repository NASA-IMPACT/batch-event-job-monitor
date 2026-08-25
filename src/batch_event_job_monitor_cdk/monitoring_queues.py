"""The SQS queues a job monitor needs to never silently drop an event.

Every failure path out of JobMonitorFunction terminates in one of these
queues, so a message that cannot be processed is inspectable and
replayable rather than gone:

- ``retry_queue`` -- retryable failures with attempts remaining, drained by
  JobResubmitFunction. Redrives to ``retry_dlq`` after ``max_receive_count``
  failed resubmission attempts.
- ``retry_dlq`` -- messages the resubmit Lambda could not process.
- ``failure_dlq`` -- terminal non-success outcomes (see ExitCodeOutcome.dlq).
- ``untracked_queue`` -- Batch jobs submitted to a monitored queue without
  the ``bejm_*`` parameters, so the job is recoverable and its submission
  replayable once the caller is fixed.
- ``event_dlq`` -- EventBridge job state-change events the monitor Lambda
  failed to process after its target retries.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import Duration, aws_sqs as sqs
from constructs import Construct


class MonitoringQueues(Construct):
    """The retry, dead-letter, and untracked-job queues for a job monitor.

    Created for you by JobMonitorFunction when you do not pass one. Create
    it yourself when the queues must outlive or be shared beyond a single
    JobMonitorFunction, or when you need to adopt queues you already own.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    retry_queue : sqs.IQueue or None, optional
        An existing retry queue to adopt instead of creating one. Adopting
        leaves redrive to you -- retry_dlq is None in that case, since a
        queue's redrive policy can only be set by whoever creates it.
    failure_dlq : sqs.IQueue or None, optional
        An existing terminal-failure queue to adopt instead of creating
        one.
    max_receive_count : int, optional
        Receives of a retry-queue message before it redrives to retry_dlq.
        Defaults to 5. Ignored when retry_queue is adopted.
    retention_period : Duration, optional
        Message retention for every queue created here. Defaults to 14
        days, the SQS maximum -- these queues exist to preserve evidence,
        so the default is the longest window SQS allows.
    visibility_timeout : Duration, optional
        Visibility timeout for the retry queue. Must be at least the
        timeout of the JobResubmitFunction draining it. Defaults to 5
        minutes. Ignored when retry_queue is adopted.

    Attributes
    ----------
    retry_queue : sqs.IQueue
        Queue the monitor publishes retryable failures to.
    retry_dlq : sqs.Queue or None
        Redrive target of retry_queue, or None when retry_queue was
        adopted.
    failure_dlq : sqs.IQueue
        Queue the monitor publishes terminal non-success outcomes to.
    untracked_queue : sqs.Queue
        Queue the untracked-job catch-all EventBridge rule delivers to.
    event_dlq : sqs.Queue
        EventBridge dead-letter queue for the monitor Lambda target.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        retry_queue: sqs.IQueue | None = None,
        failure_dlq: sqs.IQueue | None = None,
        max_receive_count: int = 5,
        retention_period: Duration = Duration.days(14),
        visibility_timeout: Duration = Duration.minutes(5),
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.retry_dlq: sqs.Queue | None = None
        if retry_queue is None:
            self.retry_dlq = sqs.Queue(
                self,
                "RetryDlq",
                retention_period=retention_period,
                enforce_ssl=True,
            )
            self.retry_queue: sqs.IQueue = sqs.Queue(
                self,
                "RetryQueue",
                retention_period=retention_period,
                visibility_timeout=visibility_timeout,
                enforce_ssl=True,
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=max_receive_count,
                    queue=self.retry_dlq,
                ),
            )
        else:
            self.retry_queue = retry_queue

        self.failure_dlq: sqs.IQueue = failure_dlq or sqs.Queue(
            self,
            "FailureDlq",
            retention_period=retention_period,
            enforce_ssl=True,
        )

        self.untracked_queue = sqs.Queue(
            self,
            "UntrackedQueue",
            retention_period=retention_period,
            enforce_ssl=True,
        )

        self.event_dlq = sqs.Queue(
            self,
            "EventDlq",
            retention_period=retention_period,
            enforce_ssl=True,
        )
