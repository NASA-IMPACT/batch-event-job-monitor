"""S3-backed three-object log store for job processing state."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import Any

import boto3
from botocore.exceptions import ClientError

from batch_event_job_monitor.models import (
    BASELINE_PROCESSING_STATES,
    JobContext,
    ProcessingEventRecord,
    ProcessingState,
)

logger = logging.getLogger(__name__)


def _render_partition(partition_fields: dict[str, str]) -> str:
    return "".join(f"{k}={v}/" for k, v in partition_fields.items())


def _pointer_body(context: JobContext) -> bytes:
    return json.dumps(
        {
            "input_entity_id": context.input_entity_id,
            "output_entity_id": context.output_entity_id,
            "attempt": context.attempt,
        }
    ).encode()


@dataclass
class S3RecordStore:
    """Three-object S3 log schema.

    Objects written per entity processing attempt:

    1. Canonical record (append-only events array):
       records/job_type={job_type}/{partition_fields...}/input_entity_id={input_entity_id}/{attempt:03d}.json

    2. State pointer (write-new / delete-old, minimal JSON body):
       state/state={STATE}/job_type={job_type}/{partition_fields...}/input_entity_id={input_entity_id}/{attempt:03d}

    3. Output index (empty body, terminal states only):
       outputs/state={STATE}/job_type={job_type}/{partition_fields...}/{output_entity_id}
    """

    bucket: str
    client: Any = field(default_factory=lambda: boto3.client("s3"))

    # -------------------- keys
    @staticmethod
    def canonical_key(context: JobContext) -> str:
        """Build the canonical record key.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.

        Returns
        -------
        str
            The S3 key for the canonical record object.
        """
        partition = _render_partition(context.partition_fields)
        return (
            f"records/job_type={context.job_type}/{partition}"
            f"input_entity_id={context.input_entity_id}/{context.attempt:03d}.json"
        )

    @staticmethod
    def _state_prefix(
        state: ProcessingState, job_type: str, partition_fields: dict[str, str]
    ) -> str:
        partition = _render_partition(partition_fields)
        return f"state/state={state.name}/job_type={job_type}/{partition}"

    @staticmethod
    def state_pointer_key(state: ProcessingState, context: JobContext) -> str:
        """Build the state pointer key.

        Parameters
        ----------
        state : ProcessingState
            The state whose pointer key is being built.
        context : JobContext
            Identifying and partitioning fields for the job processing event.

        Returns
        -------
        str
            The S3 key for the state pointer object.
        """
        return (
            S3RecordStore._state_prefix(
                state, context.job_type, context.partition_fields
            )
            + f"input_entity_id={context.input_entity_id}/{context.attempt:03d}"
        )

    @staticmethod
    def output_index_key(state: ProcessingState, context: JobContext) -> str:
        """Build the output index key.

        Parameters
        ----------
        state : ProcessingState
            The terminal state whose output index key is being built. Its
            name is used directly -- a job_type's custom terminal outcomes
            (e.g. "CLOUDY", see ExitCodeOutcome) key exactly like any
            built-in state.
        context : JobContext
            Identifying and partitioning fields for the job processing event.

        Returns
        -------
        str
            The S3 key for the output index object.
        """
        partition = _render_partition(context.partition_fields)
        return (
            f"outputs/state={state.name}/job_type={context.job_type}/"
            f"{partition}{context.output_entity_id}"
        )

    # -------------------- canonical record
    def append_canonical_event(
        self,
        *,
        context: JobContext,
        event: ProcessingEventRecord,
        batch_job_id: str | None = None,
    ) -> None:
        """Append a state-transition event to the canonical record.

        Creates the record on first call; overwrites with the appended events
        array on subsequent calls. Not atomic -- duplicate events on Lambda
        retries are acceptable for observability records.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        event : ProcessingEventRecord
            The event to append.
        batch_job_id : str or None, optional
            The AWS Batch job id associated with this attempt.
        """
        key = self.canonical_key(context)
        record: dict[str, Any]
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            record = json.loads(resp["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchKey":
                raise
            record = {
                "input_entity_id": context.input_entity_id,
                "output_entity_id": context.output_entity_id,
                "job_type": context.job_type,
                "partition_fields": context.partition_fields,
                "attempt": context.attempt,
                "batch_job_id": batch_job_id,
                "events": [],
                "current_state": event.state,
            }

        record["events"].append(event.to_dict())
        record["current_state"] = event.state

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(record).encode(),
            ContentType="application/json",
        )

    # -------------------- state pointer
    def write_state_pointer(
        self,
        *,
        context: JobContext,
        new_state: ProcessingState,
        old_state: ProcessingState | None,
        old_attempt: int | None = None,
    ) -> None:
        """Write new state pointer and delete the old one.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        new_state : ProcessingState
            The state to write a pointer for.
        old_state : ProcessingState or None
            The previous state whose pointer should be deleted, if any.
        old_attempt : int or None, optional
            Attempt number of the pointer being replaced, if different from
            context.attempt -- a cross-attempt transition, e.g. retiring a
            prior attempt's terminal pointer when the next attempt's first
            event arrives. Defaults to context.attempt.
        """
        body = _pointer_body(context)
        new_key = self.state_pointer_key(new_state, context)
        self.client.put_object(
            Bucket=self.bucket,
            Key=new_key,
            Body=body,
            ContentType="application/json",
        )
        if old_state is not None:
            old_context = (
                context
                if old_attempt is None or old_attempt == context.attempt
                else replace(context, attempt=old_attempt)
            )
            old_key = self.state_pointer_key(old_state, old_context)
            try:
                self.client.delete_object(Bucket=self.bucket, Key=old_key)
            except ClientError:
                logger.warning("Failed to delete old state pointer %s", old_key)

    def write_state_pointer_conditional(
        self,
        *,
        context: JobContext,
        state: ProcessingState,
    ) -> bool:
        """Write state pointer only if it does not already exist.

        Uses S3 conditional write (IfNoneMatch='*') to prevent duplicate
        submissions when a trigger fires multiple times for the same
        partition.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        state : ProcessingState
            The state to write a pointer for.

        Returns
        -------
        bool
            True if the pointer was written, False if it already existed.
        """
        body = _pointer_body(context)
        key = self.state_pointer_key(state, context)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                IfNoneMatch="*",
            )
            return True
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ConditionalRequestConflict", "PreconditionFailed"):
                return False
            raise

    def delete_state_pointer(
        self,
        *,
        context: JobContext,
        state: ProcessingState,
    ) -> None:
        """Delete a state pointer object.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        state : ProcessingState
            The state whose pointer should be deleted.
        """
        key = self.state_pointer_key(state, context)
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError:
            logger.warning("Failed to delete state pointer %s", key)

    # -------------------- output index
    def write_output_index(
        self,
        *,
        context: JobContext,
        state: ProcessingState,
    ) -> None:
        """Write an empty output index entry for a terminal state.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        state : ProcessingState
            The terminal state to index under.
        """
        key = self.output_index_key(state, context)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=b"")

    # -------------------- scanning
    def list_by_state(
        self,
        *,
        job_type: str,
        state: ProcessingState,
        partition_fields: dict[str, str],
    ) -> list[dict[str, Any]]:
        """List all state pointers for a given state, job type, and partition.

        Parameters
        ----------
        job_type : str
            The job type.
        state : ProcessingState
            The state to scan for.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.

        Returns
        -------
        list[dict[str, Any]]
            Dicts parsed from the pointer JSON bodies, each containing
            input_entity_id, output_entity_id, and attempt.
        """
        prefix = self._state_prefix(state, job_type, partition_fields)
        results: list[dict[str, Any]] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                try:
                    resp = self.client.get_object(Bucket=self.bucket, Key=obj["Key"])
                    data = json.loads(resp["Body"].read())
                    results.append(data)
                except (ClientError, json.JSONDecodeError):
                    logger.warning("Skipping unreadable pointer %s", obj["Key"])
        return results

    # -------------------- current-state lookups
    def find_state_pointer(
        self,
        *,
        context: JobContext,
        states: Iterable[ProcessingState] = BASELINE_PROCESSING_STATES,
    ) -> ProcessingState | None:
        """Look up the state pointer for an exact (entity, attempt).

        Checks existence of each candidate state pointer for this context
        via head_object. Used for same-attempt transitions.

        Parameters
        ----------
        context : JobContext
            Identifying and partitioning fields for the job processing event.
        states : Iterable[ProcessingState], optional
            The bounded set of states to check. Defaults to the five
            built-in lifecycle states; pass a job_type's
            ExitCodeOutcomes.states() when it declares custom terminal
            outcomes, so pointers in those states are found too.

        Returns
        -------
        ProcessingState or None
            The recorded state, or None if this (entity, attempt) has no
            pointer yet. If more than one state pointer exists (a previous
            delete_object failure left a stale one behind -- see
            write_state_pointer), logs a warning and returns the
            highest-ranked state as authoritative.
        """
        hits: list[ProcessingState] = []
        for state in states:
            key = self.state_pointer_key(state, context)
            try:
                self.client.head_object(Bucket=self.bucket, Key=key)
                hits.append(state)
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code not in ("404", "NoSuchKey", "NotFound"):
                    raise
        if not hits:
            return None
        if len(hits) > 1:
            logger.warning(
                "Multiple state pointers found for job_type=%s input_entity_id=%s "
                "attempt=%d: %s",
                context.job_type,
                context.input_entity_id,
                context.attempt,
                [state.name for state in hits],
            )
        return max(hits, key=lambda state: state.rank)

    def find_active_pointer(
        self,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        input_entity_id: str,
        states: Iterable[ProcessingState] = BASELINE_PROCESSING_STATES,
    ) -> tuple[ProcessingState, int] | None:
        """Look up an entity's active pointer, regardless of attempt.

        Bounded scan: one list_objects_v2 call per candidate state,
        prefixed to this input_entity_id (ignoring the attempt suffix).
        Used only when a SUBMITTED event's exact (entity, attempt) has no
        pointer yet, to find whichever prior attempt's pointer needs
        retiring.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        input_entity_id : str
            The processed entity identifier.
        states : Iterable[ProcessingState], optional
            The bounded set of states to check -- see find_state_pointer.

        Returns
        -------
        tuple[ProcessingState, int] or None
            The (state, attempt) parsed from the active pointer's body, or
            None if the entity has no active pointer (a brand-new entity).
            If more than one hit is found, logs a warning and returns the
            highest-ranked (then highest-attempt) hit as authoritative.
        """
        hits: list[tuple[ProcessingState, int]] = []
        for state in states:
            prefix = (
                self._state_prefix(state, job_type, partition_fields)
                + f"input_entity_id={input_entity_id}/"
            )
            resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            for obj in resp.get("Contents", []):
                try:
                    body = self.client.get_object(Bucket=self.bucket, Key=obj["Key"])
                    data = json.loads(body["Body"].read())
                    hits.append((state, int(data["attempt"])))
                except (ClientError, json.JSONDecodeError, KeyError, ValueError):
                    logger.warning("Skipping unreadable pointer %s", obj["Key"])
        if not hits:
            return None
        if len(hits) > 1:
            logger.warning(
                "Multiple active pointers found for job_type=%s input_entity_id=%s: %s",
                job_type,
                input_entity_id,
                hits,
            )
        return max(hits, key=lambda hit: (hit[0].rank, hit[1]))

    def next_attempt(
        self,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        input_entity_id: str,
        states: Iterable[ProcessingState] = BASELINE_PROCESSING_STATES,
    ) -> int:
        """Compute the next attempt number for an entity.

        Convenience wrapper over find_active_pointer for ad hoc/backfill
        submitters who want the correct next bejm_attempt without
        hand-rolling the lookup. Pass a job_type's ExitCodeOutcomes.states()
        as states if it declares custom terminal outcomes, so an entity
        whose last attempt ended in a custom state is still found.

        Returns
        -------
        int
            1 if the entity has no active pointer, else the active
            pointer's attempt number plus one.
        """
        active = self.find_active_pointer(
            job_type=job_type,
            partition_fields=partition_fields,
            input_entity_id=input_entity_id,
            states=states,
        )
        return 1 if active is None else active[1] + 1
