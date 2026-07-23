"""Helper for building JobTypeConfig from typed CDK Batch refs.

JobTypeConfig itself (in batch_event_job_monitor.models) stays free of any
aws_cdk import -- it's JSON-encoded into a Lambda env var and decoded again
at runtime, so it only ever holds plain ARN strings. This module is where
typed CDK Batch refs get reduced to those strings.
"""

from __future__ import annotations

from aws_cdk import aws_batch as batch

from batch_event_job_monitor.models import ExitCodeOutcomes, JobTypeConfig, RetryPolicy


def job_definition_family_arn(job_definition: batch.IJobDefinition) -> str:
    """job_definition's ARN with any revision suffix stripped.

    Batch resolves a family ARN (no revision) to whichever revision is
    currently ACTIVE, so resubmissions/rule scoping automatically follow a
    new revision without redeploying.
    """
    arn = job_definition.job_definition_arn
    prefix, _, suffix = arn.rpartition(":")
    return prefix if suffix.isdigit() else arn


def job_type_config(
    *,
    job_queue: batch.IJobQueue,
    job_definition: batch.IJobDefinition,
    retry_policy: RetryPolicy | None = None,
    exit_code_outcomes: ExitCodeOutcomes | None = None,
) -> JobTypeConfig:
    """Build a JobTypeConfig from typed CDK Batch refs.

    The common-case way to build one entry of the job_type_configs mapping
    JobMonitorFunction/JobResubmitFunction take -- extracts job_queue_arn
    and job_definition_arn (family ARN, revision stripped) from the given
    refs.
    """
    return JobTypeConfig(
        job_queue_arn=job_queue.job_queue_arn,
        job_definition_arn=job_definition_family_arn(job_definition),
        retry_policy=retry_policy or RetryPolicy(),
        exit_code_outcomes=exit_code_outcomes or ExitCodeOutcomes(),
    )
