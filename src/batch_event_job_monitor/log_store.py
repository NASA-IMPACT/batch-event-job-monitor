"""S3-backed three-object log store for job processing state."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from batch_event_job_monitor.models import ProcessingEventRecord, ProcessingState

logger = logging.getLogger(__name__)


def _render_partition(partition_fields: dict[str, str]) -> str:
    return "".join(f"{k}={v}/" for k, v in partition_fields.items())


@dataclass
class S3RecordStore:
    """Three-object S3 log schema.

    Objects written per entity processing attempt:

    1. Canonical record (append-only events array):
       records/job_type={job_type}/{partition_fields...}/entity_id={entity_id}/{attempt:03d}.json

    2. State pointer (write-new / delete-old, minimal JSON body):
       state/state={STATE}/job_type={job_type}/{partition_fields...}/entity_id={entity_id}/{attempt:03d}

    3. Output index (empty body, terminal states only):
       outputs/state={STATE}/job_type={job_type}/{partition_fields...}/{output_entity_id}
    """

    bucket: str
    client: Any = field(default_factory=lambda: boto3.client("s3"))

    # ------------------------------------------------------------------ keys

    @staticmethod
    def canonical_key(
        job_type: str,
        partition_fields: dict[str, str],
        entity_id: str,
        attempt: int,
    ) -> str:
        """Build the canonical record key.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        entity_id : str
            The processed entity identifier.
        attempt : int
            The attempt number.

        Returns
        -------
        str
            The S3 key for the canonical record object.
        """
        partition = _render_partition(partition_fields)
        return (
            f"records/job_type={job_type}/{partition}"
            f"entity_id={entity_id}/{attempt:03d}.json"
        )

    @staticmethod
    def _state_prefix(
        state: ProcessingState,
        job_type: str,
        partition_fields: dict[str, str],
    ) -> str:
        partition = _render_partition(partition_fields)
        return f"state/state={state.name}/job_type={job_type}/{partition}"

    @staticmethod
    def state_pointer_key(
        state: ProcessingState,
        job_type: str,
        partition_fields: dict[str, str],
        entity_id: str,
        attempt: int,
    ) -> str:
        """Build the state pointer key.

        Parameters
        ----------
        state : ProcessingState
            The state whose pointer key is being built.
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        entity_id : str
            The processed entity identifier.
        attempt : int
            The attempt number.

        Returns
        -------
        str
            The S3 key for the state pointer object.
        """
        return (
            S3RecordStore._state_prefix(state, job_type, partition_fields)
            + f"entity_id={entity_id}/{attempt:03d}"
        )

    @staticmethod
    def output_index_key(
        state: ProcessingState,
        job_type: str,
        partition_fields: dict[str, str],
        output_entity_id: str,
    ) -> str:
        """Build the output index key.

        Parameters
        ----------
        state : ProcessingState
            The terminal state whose output index key is being built.
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        output_entity_id : str
            The output entity identifier.

        Returns
        -------
        str
            The S3 key for the output index object.
        """
        partition = _render_partition(partition_fields)
        return f"outputs/state={state.name}/job_type={job_type}/{partition}{output_entity_id}"

    # -------------------------------------------------------- canonical record

    def append_canonical_event(
        self,
        *,
        entity_id: str,
        output_entity_id: str,
        job_type: str,
        partition_fields: dict[str, str],
        attempt: int,
        event: ProcessingEventRecord,
        batch_job_id: str | None = None,
    ) -> None:
        """Append a state-transition event to the canonical record.

        Creates the record on first call; overwrites with the appended events
        array on subsequent calls. Not atomic -- duplicate events on Lambda
        retries are acceptable for observability records.

        Parameters
        ----------
        entity_id : str
            The processed entity identifier.
        output_entity_id : str
            The output entity identifier.
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        attempt : int
            The attempt number.
        event : ProcessingEventRecord
            The event to append.
        batch_job_id : str or None, optional
            The AWS Batch job id associated with this attempt.
        """
        key = self.canonical_key(job_type, partition_fields, entity_id, attempt)
        record: dict[str, Any]
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            record = json.loads(resp["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchKey":
                raise
            record = {
                "entity_id": entity_id,
                "output_entity_id": output_entity_id,
                "job_type": job_type,
                "partition_fields": partition_fields,
                "attempt": attempt,
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

    # ---------------------------------------------------------- state pointer

    def write_state_pointer(
        self,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        entity_id: str,
        attempt: int,
        new_state: ProcessingState,
        old_state: ProcessingState | None,
        output_entity_id: str,
    ) -> None:
        """Write new state pointer and delete the old one.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        entity_id : str
            The processed entity identifier.
        attempt : int
            The attempt number.
        new_state : ProcessingState
            The state to write a pointer for.
        old_state : ProcessingState or None
            The previous state whose pointer should be deleted, if any.
        output_entity_id : str
            The output entity identifier.
        """
        body = json.dumps(
            {
                "entity_id": entity_id,
                "output_entity_id": output_entity_id,
                "attempt": attempt,
            }
        ).encode()
        new_key = self.state_pointer_key(
            new_state, job_type, partition_fields, entity_id, attempt
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=new_key,
            Body=body,
            ContentType="application/json",
        )
        if old_state is not None:
            old_key = self.state_pointer_key(
                old_state, job_type, partition_fields, entity_id, attempt
            )
            try:
                self.client.delete_object(Bucket=self.bucket, Key=old_key)
            except ClientError:
                logger.warning("Failed to delete old state pointer %s", old_key)

    def write_state_pointer_conditional(
        self,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        entity_id: str,
        attempt: int,
        state: ProcessingState,
        output_entity_id: str,
    ) -> bool:
        """Write state pointer only if it does not already exist.

        Uses S3 conditional write (IfNoneMatch='*') to prevent duplicate
        submissions when a trigger fires multiple times for the same
        partition.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        entity_id : str
            The processed entity identifier.
        attempt : int
            The attempt number.
        state : ProcessingState
            The state to write a pointer for.
        output_entity_id : str
            The output entity identifier.

        Returns
        -------
        bool
            True if the pointer was written, False if it already existed.
        """
        body = json.dumps(
            {
                "entity_id": entity_id,
                "output_entity_id": output_entity_id,
                "attempt": attempt,
            }
        ).encode()
        key = self.state_pointer_key(
            state, job_type, partition_fields, entity_id, attempt
        )
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
        job_type: str,
        partition_fields: dict[str, str],
        entity_id: str,
        attempt: int,
        state: ProcessingState,
    ) -> None:
        """Delete a state pointer object.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        entity_id : str
            The processed entity identifier.
        attempt : int
            The attempt number.
        state : ProcessingState
            The state whose pointer should be deleted.
        """
        key = self.state_pointer_key(
            state, job_type, partition_fields, entity_id, attempt
        )
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError:
            logger.warning("Failed to delete state pointer %s", key)

    # ---------------------------------------------------------- output index

    def write_output_index(
        self,
        *,
        job_type: str,
        partition_fields: dict[str, str],
        output_entity_id: str,
        state: ProcessingState,
    ) -> None:
        """Write an empty output index entry for a terminal state.

        Parameters
        ----------
        job_type : str
            The job type.
        partition_fields : dict[str, str]
            Ordered partition key/value pairs.
        output_entity_id : str
            The output entity identifier.
        state : ProcessingState
            The terminal state to index under.
        """
        key = self.output_index_key(state, job_type, partition_fields, output_entity_id)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=b"")

    # -------------------------------------------------------------- scanning

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
            entity_id, output_entity_id, and attempt.
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
