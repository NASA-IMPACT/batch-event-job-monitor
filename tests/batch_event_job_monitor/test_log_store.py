"""Tests for S3RecordStore (three-object log schema)."""

import json

import pytest
from mypy_boto3_s3 import S3Client

from batch_event_job_monitor.log_store import S3RecordStore
from batch_event_job_monitor.models import (
    BASELINE_PROCESSING_STATES,
    ExitCodeOutcome,
    JobContext,
    ProcessingEventRecord,
    ProcessingStates,
)

JOB_TYPE = "monthly-composite"
INPUT_ENTITY_ID = "12TVK_2024-06_source"
OUTPUT_ENTITY_ID = "12TVK_2024-06_output"
ATTEMPT = 0

# nextgen-style partition shape
ACQ_DATE_PARTITION = {"acquisition_date": "2024-01-15"}

# monthly-composites-style partition shape (multiple, ordered fields)
TILE_MONTH_PARTITION = {"tile_id": "12TVK", "year_month": "2024-06"}


def make_context(**overrides: object) -> JobContext:
    defaults: dict[str, object] = {
        "job_type": JOB_TYPE,
        "partition_fields": TILE_MONTH_PARTITION,
        "input_entity_id": INPUT_ENTITY_ID,
        "output_entity_id": OUTPUT_ENTITY_ID,
        "attempt": ATTEMPT,
    }
    defaults.update(overrides)
    return JobContext(**defaults)  # type: ignore[arg-type]


@pytest.fixture
def store(bucket: str) -> S3RecordStore:
    return S3RecordStore(bucket=bucket)


class TestKeyConstructors:
    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_canonical_key(self, partition_fields: dict[str, str]) -> None:
        context = make_context(partition_fields=partition_fields)
        key = S3RecordStore.canonical_key(context)
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"records/job_type={JOB_TYPE}/{partition}"
            f"input_entity_id={INPUT_ENTITY_ID}/{ATTEMPT:03d}.json"
        )

    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_state_pointer_key(self, partition_fields: dict[str, str]) -> None:
        context = make_context(partition_fields=partition_fields)
        key = S3RecordStore.state_pointer_key(ProcessingStates.AWAITING, context)
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"state/state=AWAITING/job_type={JOB_TYPE}/{partition}"
            f"input_entity_id={INPUT_ENTITY_ID}/{ATTEMPT:03d}"
        )

    @pytest.mark.parametrize(
        "partition_fields",
        [ACQ_DATE_PARTITION, TILE_MONTH_PARTITION],
    )
    def test_output_index_key(self, partition_fields: dict[str, str]) -> None:
        context = make_context(partition_fields=partition_fields)
        key = S3RecordStore.output_index_key(ProcessingStates.SUCCESS, context)
        partition = "".join(f"{k}={v}/" for k, v in partition_fields.items())
        assert key == (
            f"outputs/state=SUCCESS/job_type={JOB_TYPE}/{partition}{OUTPUT_ENTITY_ID}"
        )


class TestAppendCanonicalEvent:
    def test_creates_new_record(self, store: S3RecordStore, s3: S3Client) -> None:
        context = make_context()
        event = ProcessingEventRecord(
            state="AWAITING", timestamp="2024-01-15T00:00:00Z"
        )
        store.append_canonical_event(context=context, event=event)
        key = S3RecordStore.canonical_key(context)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["input_entity_id"] == INPUT_ENTITY_ID
        assert record["output_entity_id"] == OUTPUT_ENTITY_ID
        assert record["job_type"] == JOB_TYPE
        assert record["partition_fields"] == TILE_MONTH_PARTITION
        assert record["attempt"] == ATTEMPT
        assert record["current_state"] == "AWAITING"
        assert len(record["events"]) == 1

    def test_appends_to_existing_record(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        context = make_context()
        transitions = [
            ("AWAITING", "2024-01-15T00:00:00Z"),
            ("SUBMITTED", "2024-01-16T00:00:00Z"),
        ]
        for state_name, timestamp in transitions:
            store.append_canonical_event(
                context=context,
                event=ProcessingEventRecord(state=state_name, timestamp=timestamp),
            )
        key = S3RecordStore.canonical_key(context)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert len(record["events"]) == 2
        assert record["current_state"] == "SUBMITTED"

    def test_stores_batch_job_id(self, store: S3RecordStore, s3: S3Client) -> None:
        context = make_context()
        store.append_canonical_event(
            context=context,
            event=ProcessingEventRecord(
                state="SUBMITTED", timestamp="2024-01-15T00:00:00Z"
            ),
            batch_job_id="batch-job-123",
        )
        key = S3RecordStore.canonical_key(context)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        record = json.loads(resp["Body"].read())
        assert record["batch_job_id"] == "batch-job-123"


class TestStatePointer:
    def test_write_and_read_pointer(self, store: S3RecordStore, s3: S3Client) -> None:
        context = make_context()
        store.write_state_pointer(
            context=context,
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        key = S3RecordStore.state_pointer_key(ProcessingStates.AWAITING, context)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        body = json.loads(resp["Body"].read())
        assert body["input_entity_id"] == INPUT_ENTITY_ID
        assert body["output_entity_id"] == OUTPUT_ENTITY_ID
        assert body["attempt"] == ATTEMPT

    def test_transition_deletes_old_pointer(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        context = make_context()
        store.write_state_pointer(
            context=context,
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        store.write_state_pointer(
            context=context,
            new_state=ProcessingStates.SUBMITTED,
            old_state=ProcessingStates.AWAITING,
        )
        awaiting_key = S3RecordStore.state_pointer_key(
            ProcessingStates.AWAITING, context
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=awaiting_key)
        assert resp.get("KeyCount", 0) == 0

        submitted_key = S3RecordStore.state_pointer_key(
            ProcessingStates.SUBMITTED, context
        )
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=submitted_key)
        assert resp.get("KeyCount", 0) == 1

    def test_conditional_write_first_succeeds(self, store: S3RecordStore) -> None:
        written = store.write_state_pointer_conditional(
            context=make_context(), state=ProcessingStates.SUBMITTED
        )
        assert written is True

    def test_conditional_write_second_returns_false(self, store: S3RecordStore) -> None:
        context = make_context()
        written_first = store.write_state_pointer_conditional(
            context=context, state=ProcessingStates.SUBMITTED
        )
        written_again = store.write_state_pointer_conditional(
            context=context, state=ProcessingStates.SUBMITTED
        )
        assert written_first is True
        assert written_again is False

    def test_delete_state_pointer(self, store: S3RecordStore, s3: S3Client) -> None:
        context = make_context()
        store.write_state_pointer(
            context=context,
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        store.delete_state_pointer(context=context, state=ProcessingStates.AWAITING)
        key = S3RecordStore.state_pointer_key(ProcessingStates.AWAITING, context)
        resp = s3.list_objects_v2(Bucket=store.bucket, Prefix=key)
        assert resp.get("KeyCount", 0) == 0


class TestOutputIndex:
    def test_write_output_index(self, store: S3RecordStore, s3: S3Client) -> None:
        context = make_context()
        store.write_output_index(context=context, state=ProcessingStates.SUCCESS)
        key = S3RecordStore.output_index_key(ProcessingStates.SUCCESS, context)
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        assert resp["Body"].read() == b""

    def test_write_output_index_with_custom_state_uses_its_own_name(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        context = make_context()
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        store.write_output_index(context=context, state=cloudy)
        key = S3RecordStore.output_index_key(cloudy, context)
        assert "state=CLOUDY/" in key
        resp = s3.get_object(Bucket=store.bucket, Key=key)
        assert resp["Body"].read() == b""

    def test_output_index_key_uses_builtin_state_name(self) -> None:
        context = make_context()
        key = S3RecordStore.output_index_key(
            ProcessingStates.FAILURE_NONRETRYABLE, context
        )
        assert "state=FAILURE_NONRETRYABLE/" in key


class TestListByState:
    def test_empty_when_no_pointers(self, store: S3RecordStore) -> None:
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingStates.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []

    def test_returns_pointers_for_state(self, store: S3RecordStore) -> None:
        for i, entity in enumerate(["entity-one", "entity-two"]):
            store.write_state_pointer(
                context=make_context(
                    input_entity_id=entity, output_entity_id=f"output-{i}"
                ),
                new_state=ProcessingStates.AWAITING,
                old_state=None,
            )

        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingStates.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert len(results) == 2
        entity_ids = {r["input_entity_id"] for r in results}
        assert entity_ids == {"entity-one", "entity-two"}

    def test_ignores_other_states(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(),
            new_state=ProcessingStates.SUBMITTED,
            old_state=None,
        )
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingStates.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []

    def test_ignores_other_job_types(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(job_type="other-job-type"),
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        results = store.list_by_state(
            job_type=JOB_TYPE,
            state=ProcessingStates.AWAITING,
            partition_fields=TILE_MONTH_PARTITION,
        )
        assert results == []


class TestFindStatePointer:
    def test_no_pointer_returns_none(self, store: S3RecordStore) -> None:
        assert store.find_state_pointer(context=make_context()) is None

    def test_returns_recorded_state(self, store: S3RecordStore) -> None:
        context = make_context()
        store.write_state_pointer(
            context=context, new_state=ProcessingStates.SUBMITTED, old_state=None
        )
        assert store.find_state_pointer(context=context) is ProcessingStates.SUBMITTED

    def test_scoped_to_exact_attempt(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(attempt=1),
            new_state=ProcessingStates.FAILURE_RETRYABLE,
            old_state=None,
        )
        assert store.find_state_pointer(context=make_context(attempt=2)) is None

    def test_multiple_pointers_returns_highest_rank(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        context = make_context()
        store.write_state_pointer(
            context=context, new_state=ProcessingStates.SUBMITTED, old_state=None
        )
        # Simulate a stale leftover pointer (e.g. a previously swallowed
        # delete_object failure) by writing a second state pointer directly
        # rather than via write_state_pointer, which would delete the first.
        key = S3RecordStore.state_pointer_key(ProcessingStates.SUCCESS, context)
        s3.put_object(Bucket=store.bucket, Key=key, Body=b"{}")
        assert store.find_state_pointer(context=context) is ProcessingStates.SUCCESS

    def test_not_found_with_default_states_when_custom_state_written(
        self, store: S3RecordStore
    ) -> None:
        """A pointer written in a custom state is invisible to the default
        (baseline-only) scan -- states must be passed explicitly."""
        context = make_context()
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        store.write_state_pointer(context=context, new_state=cloudy, old_state=None)
        assert store.find_state_pointer(context=context) is None

    def test_found_when_custom_states_passed(self, store: S3RecordStore) -> None:
        context = make_context()
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        store.write_state_pointer(context=context, new_state=cloudy, old_state=None)
        result = store.find_state_pointer(
            context=context, states=(*BASELINE_PROCESSING_STATES, cloudy)
        )
        assert result == cloudy


class TestFindActivePointer:
    def test_no_active_pointer_returns_none(self, store: S3RecordStore) -> None:
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result is None

    def test_returns_state_and_attempt(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(attempt=2),
            new_state=ProcessingStates.FAILURE_RETRYABLE,
            old_state=None,
        )
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result == (ProcessingStates.FAILURE_RETRYABLE, 2)

    def test_scoped_to_entity(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(input_entity_id="other-entity"),
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result is None

    def test_multiple_hits_returns_highest_rank_then_attempt(
        self, store: S3RecordStore
    ) -> None:
        store.write_state_pointer(
            context=make_context(attempt=1),
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        store.write_state_pointer(
            context=make_context(attempt=2),
            new_state=ProcessingStates.FAILURE_RETRYABLE,
            old_state=None,
        )
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result == (ProcessingStates.FAILURE_RETRYABLE, 2)

    def test_multiple_hits_prefers_higher_attempt_over_higher_rank(
        self, store: S3RecordStore
    ) -> None:
        """A stale terminal pointer left behind by a failed delete (see
        write_state_pointer) must not outrank a genuinely active, later
        attempt just because its state ranks higher."""
        store.write_state_pointer(
            context=make_context(attempt=1),
            new_state=ProcessingStates.FAILURE_NONRETRYABLE,
            old_state=None,
        )
        store.write_state_pointer(
            context=make_context(attempt=2),
            new_state=ProcessingStates.AWAITING,
            old_state=None,
        )
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result == (ProcessingStates.AWAITING, 2)

    def test_custom_state_found_only_when_states_passed(
        self, store: S3RecordStore
    ) -> None:
        cloudy = ExitCodeOutcome(name="CLOUDY", dlq=False).to_processing_state()
        store.write_state_pointer(
            context=make_context(attempt=3), new_state=cloudy, old_state=None
        )
        assert (
            store.find_active_pointer(
                job_type=JOB_TYPE,
                partition_fields=TILE_MONTH_PARTITION,
                input_entity_id=INPUT_ENTITY_ID,
            )
            is None
        )
        result = store.find_active_pointer(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
            states=(*BASELINE_PROCESSING_STATES, cloudy),
        )
        assert result == (cloudy, 3)


class TestNextAttempt:
    def test_no_active_pointer_returns_one(self, store: S3RecordStore) -> None:
        result = store.next_attempt(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result == 1

    def test_active_pointer_returns_next(self, store: S3RecordStore) -> None:
        store.write_state_pointer(
            context=make_context(attempt=2),
            new_state=ProcessingStates.FAILURE_RETRYABLE,
            old_state=None,
        )
        result = store.next_attempt(
            job_type=JOB_TYPE,
            partition_fields=TILE_MONTH_PARTITION,
            input_entity_id=INPUT_ENTITY_ID,
        )
        assert result == 3


class TestWriteStatePointerCrossAttempt:
    def test_old_attempt_deletes_prior_attempt_pointer(
        self, store: S3RecordStore, s3: S3Client
    ) -> None:
        old_context = make_context(attempt=1)
        store.write_state_pointer(
            context=old_context,
            new_state=ProcessingStates.FAILURE_RETRYABLE,
            old_state=None,
        )
        new_context = make_context(attempt=2)
        store.write_state_pointer(
            context=new_context,
            new_state=ProcessingStates.SUBMITTED,
            old_state=ProcessingStates.FAILURE_RETRYABLE,
            old_attempt=1,
        )
        old_key = S3RecordStore.state_pointer_key(
            ProcessingStates.FAILURE_RETRYABLE, old_context
        )
        assert (
            s3.list_objects_v2(Bucket=store.bucket, Prefix=old_key).get("KeyCount", 0)
            == 0
        )
        new_key = S3RecordStore.state_pointer_key(
            ProcessingStates.SUBMITTED, new_context
        )
        assert (
            s3.list_objects_v2(Bucket=store.bucket, Prefix=new_key).get("KeyCount", 0)
            == 1
        )
