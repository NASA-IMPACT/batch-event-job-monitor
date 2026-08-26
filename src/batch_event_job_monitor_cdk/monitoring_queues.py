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

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_kms as kms,
    aws_sqs as sqs,
)
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
    encryption : sqs.QueueEncryption or None, optional
        Encryption at rest for every queue created here. Defaults to
        QueueEncryption.SQS_MANAGED, or QueueEncryption.KMS when
        encryption_master_key is given.
    encryption_master_key : kms.IKey or None, optional
        Customer-managed KMS key for every queue created here. Setting it
        implies encryption=QueueEncryption.KMS. Note that a customer-
        managed key needs its own key policy grants for the principals
        writing to these queues -- in particular events.amazonaws.com for
        the untracked queue and the EventBridge dead-letter queue, which
        EventBridge writes to directly rather than through a role this
        library grants.
    data_key_reuse : Duration or None, optional
        How long SQS reuses a data key before calling KMS again. Only
        meaningful under KMS encryption. Defaults to the SQS default of 5
        minutes.
    enforce_ssl : bool, optional
        Reject non-TLS requests to every queue created here, via a queue
        policy. Defaults to True.
    removal_policy : RemovalPolicy or None, optional
        Removal policy for every queue created here. Defaults to
        aws-cdk-lib's own queue default (DESTROY). Pass
        RemovalPolicy.RETAIN to keep undelivered evidence when the stack
        is destroyed.

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
        encryption: sqs.QueueEncryption | None = None,
        encryption_master_key: kms.IKey | None = None,
        data_key_reuse: Duration | None = None,
        enforce_ssl: bool = True,
        removal_policy: RemovalPolicy | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if encryption is None:
            # Set SQS_MANAGED explicitly rather than leaving it unset. SQS
            # applies SSE-SQS to a new queue either way, but an unset
            # property means the synthesized template says nothing about
            # encryption, which an auditor or a Config rule reads as
            # unencrypted.
            encryption = (
                sqs.QueueEncryption.KMS
                if encryption_master_key is not None
                else sqs.QueueEncryption.SQS_MANAGED
            )

        # Shared by every queue created here, so a caller cannot end up
        # with one of the five encrypted differently from the rest.
        queue_props: dict[str, Any] = {
            "retention_period": retention_period,
            "enforce_ssl": enforce_ssl,
            "encryption": encryption,
            "encryption_master_key": encryption_master_key,
            "data_key_reuse": data_key_reuse,
            "removal_policy": removal_policy,
        }

        self.retry_dlq: sqs.Queue | None = None
        if retry_queue is None:
            self.retry_dlq = sqs.Queue(self, "RetryDlq", **queue_props)
            self.retry_queue: sqs.IQueue = sqs.Queue(
                self,
                "RetryQueue",
                visibility_timeout=visibility_timeout,
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=max_receive_count,
                    queue=self.retry_dlq,
                ),
                **queue_props,
            )
        else:
            self.retry_queue = retry_queue

        self.failure_dlq: sqs.IQueue = failure_dlq or sqs.Queue(
            self, "FailureDlq", **queue_props
        )
        self.untracked_queue = sqs.Queue(self, "UntrackedQueue", **queue_props)
        self.event_dlq = sqs.Queue(self, "EventDlq", **queue_props)
