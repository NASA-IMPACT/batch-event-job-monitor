"""Tests for S3RecordStore (three-object log schema)."""

import json

import pytest
from mypy_boto3_s3 import S3Client

from batch_event_job_monitoring.log_store import S3RecordStore
from batch_event_job_monitoring.models import ProcessingEventRecord, ProcessingState

JOB_TYPE = "monthly-composite"
ENTITY_ID = "12TVK_2024-06_source"
OUTPUT_ENTITY_ID = "12TVK_2024-06_output"
ATTEMPT = 0

# nextgen-style partition shape
ACQ_DATE_PARTITION = {"acquisition_date": "2024-01-15"}

# monthly-composites-style partition shape (multiple, ordered fields)
TILE_MONTH_PARTITION = {"tile_id": "12TVK", "year_month": "2024-06"}


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


class TestKeyConstructors:
    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_canonical_key(self, partition_fields: dict[str, str]) -> None:
        key = S3RecordStore.canonical_key(
            JOB_TYPE, partition_fields, ENTITY_ID, ATTEMPT
        )
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"records/job_type={JOB_TYPE}/{partition}"
            f"entity_id={ENTITY_ID}/{ATTEMPT:03d}.json"
        )

    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_state_pointer_key(self, partition_fields: dict[str, str]) -> None:
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, JOB_TYPE, partition_fields, ENTITY_ID, ATTEMPT
        )
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"state/state=AWAITING/job_type={JOB_TYPE}/{partition}"
            f"entity_id={ENTITY_ID}/{ATTEMPT:03d}"
        )

    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_output_index_key(self, partition_fields: dict[str, str]) -> None:
        key = S3RecordStore.output_index_key(
            ProcessingState.SUCCESS, JOB_TYPE, partition_fields, OUTPUT_ENTITY_ID
        )
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"outputs/state=SUCCESS/job_type={JOB_TYPE}/{partition}{OUTPUT_ENTITY_ID}"
        )


class TestAppendCanonicalEvent:
    def test_creates_new_record(self, store: S3RecordStore, s3: S3Client) -> None:
        event = ProcessingEventRecord(
            state="AWAITING", timestamp="2024-01-15T00:00:00Z"
        )
        store.append_canonical_event(
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            attempt=ATTEMPT,
            event=event,
        )
        key = S3RecordStore.canonical_key(
            JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["entity_id"] == ENTITY_ID
        assert record["output_entity_id"] == OUTPUT_ENTITY_ID
        assert record["job_type"] == JOB_TYPE
        assert record["partition_fields"] == TILE_MONTH_PARTITION
        assert record["attempt"] == ATTEMPT
        assert record["current_state"] == "AWAITING"
        assert len(record["events"]) == 1

    def test_appends_to_existing_record(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        transitions = [
            ("AWAITING", "2024-01-15T00:00:00Z"),
            ("SUBMITTED", "2024-01-16T00:00:00Z"),
        ]
        for state_name, timestamp in transitions:
            store.append_canonical_event(
                entity_id=ENTITY_ID,
                output_entity_id=OUTPUT_ENTITY_ID,
                job_type=JOB_TYPE,
                partition_fields=TILE_MONTH_PARTITION,
                attempt=ATTEMPT,
                event=ProcessingEventRecord(state=state_name, timestamp=timestamp),
            )
        key = S3RecordStore.canonical_key(
            JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert len(record["events"]) == 2
        assert record["current_state"] == "SUBMITTED"

    def test_stores_batch_job_id(self, store: S3RecordStore, s3: S3Client) -> None:
        store.append_canonical_event(
            entity_id=ENTITY_ID,
            output_entity_id=OUTPUT_ENTITY_ID,
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            attempt=ATTEMPT,
            event=ProcessingEventRecord(
                state="SUBMITTED", timestamp="2024-01-15T00:00:00Z"
            ),
            batch_job_id="batch-job-123",
        )
        key = S3RecordStore.canonical_key(
            JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["batch_job_id"] == "batch-job-123"


class TestStatePointer:
    def test_write_and_read_pointer(self, store: S3RecordStore, s3: S3Client) -> None:
        store.write_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        body = json.loads(resp["Body"].read())
        assert body["entity_id"] == ENTITY_ID
        assert body["output_entity_id"] == OUTPUT_ENTITY_ID
        assert body["attempt"] == ATTEMPT

    def test_transition_deletes_old_pointer(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        store.write_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        store.write_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.SUBMITTED,
            old_state=ProcessingState.AWAITING,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=awaiting_key)
        assert resp.get("KeyCount", 0) == 0

        submitted_key = S3RecordStore.state_pointer_key(
            ProcessingState.SUBMITTED,
            JOB_TYPE,
            TILE_MONTH_PARTITION,
            ENTITY_ID,
            ATTEMPT,
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=submitted_key)
        assert resp.get("KeyCount", 0) == 1

    def test_conditional_write_first_succeeds(self, store: S3RecordStore) -> None:
        written = store.write_state_pointer_conditional(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            state=ProcessingState.SUBMITTED,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        assert written is True

    def test_conditional_write_second_returns_false(self, store: S3RecordStore) -> None:
        written_first = store.write_state_pointer_conditional(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            state=ProcessingState.SUBMITTED,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        written_again = store.write_state_pointer_conditional(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            state=ProcessingState.SUBMITTED,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        assert written_first is True
        assert written_again is False

    def test_delete_state_pointer(self, store: S3RecordStore, s3: S3Client) -> None:
        store.write_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        store.delete_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=ATTEMPT,
            state=ProcessingState.AWAITING,
        )
        key = S3RecordStore.state_pointer_key(
            ProcessingState.AWAITING, JOB_TYPE, TILE_MONTH_PARTITION, ENTITY_ID, ATTEMPT
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=key)
        assert resp.get("KeyCount", 0) == 0


class TestOutputIndex:
    def test_write_output_index(self, store: S3RecordStore, s3: S3Client) -> None:
        store.write_output_index(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            output_entity_id=OUTPUT_ENTITY_ID,
            state=ProcessingState.SUCCESS,
        )
        key = S3RecordStore.output_index_key(
            ProcessingState.SUCCESS, JOB_TYPE, TILE_MONTH_PARTITION, OUTPUT_ENTITY_ID
        )
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        assert resp["Body"].read() == b""


class TestListByState:
    def test_empty_when_no_pointers(self, store: S3RecordStore) -> None:
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingState.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []

    def test_returns_pointers_for_state(self, store: S3RecordStore) -> None:
        for i, entity in enumerate(["entity-one", "entity-two"]):
            store.write_state_pointer(
                job_type=JOB_TYPE,
                partition_fields=TILE_MONTH_PARTITION,
                entity_id=entity,
                attempt=0,
                new_state=ProcessingState.AWAITING,
                old_state=None,
                output_entity_id=f"output-{i}",
            )

        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingState.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert len(results) == 2
        entity_ids = {r["entity_id"] for r in results}
        assert entity_ids == {"entity-one", "entity-two"}

    def test_ignores_other_states(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=0,
            new_state=ProcessingState.SUBMITTED,
            old_state=None,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingState.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []

    def test_ignores_other_job_types(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            job_type="other-job-type",
            partition_fields=TILE_MONTH_PARTITION,
            entity_id=ENTITY_ID,
            attempt=0,
            new_state=ProcessingState.AWAITING,
            old_state=None,
            output_entity_id=OUTPUT_ENTITY_ID,
        )
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingState.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []
