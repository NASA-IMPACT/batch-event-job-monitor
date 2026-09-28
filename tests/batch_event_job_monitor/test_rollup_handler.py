"""Tests for the bundled rollup Lambda handler."""

from __future__ import annotations

from typing import Any

import pytest

from batch_event_job_monitor.handlers import rollup_handler

ENVIRONMENT = {
    "PROCESSING_BUCKET_NAME": "test-processing",
    "ROLLUP_QUEUE_URL": "https://sqs.us-west-2.amazonaws.com/123456789012/q",
    "ROLLUP_STAGING_PREFIX": "staging/",
    "ROLLUP_DATABASE": "test_db",
    "ROLLUP_ICEBERG_TABLE": "records_iceberg",
    "ROLLUP_STAGING_TABLE": "records_staging",
    "ROLLUP_INVENTORY_TABLE": "records_inventory",
    "ROLLUP_WORKGROUP": "wg",
    "ROLLUP_PARTITION_KEY_NAMES": "job_type,tile_id,year_month",
    "ROLLUP_MAX_KEYS": "25000",
    "ROLLUP_MAX_DEPTH": "1000",
    "AWS_LAMBDA_FUNCTION_NAME": "rollup-fn",
}


@pytest.fixture
def environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)


def test_s3_client_pool_matches_the_fetch_thread_pool() -> None:
    # rollup.fetch_rows defaults to 32 worker threads; boto3's default
    # connection pool of 10 would starve 22 of them.
    config = rollup_handler._S3_CLIENT_CONFIG
    assert config.max_pool_connections == 32  # type: ignore[attr-defined]


def test_config_reads_every_setting_from_the_environment(environment: None) -> None:
    config = rollup_handler.config_from_environment()
    assert config.bucket == "test-processing"
    assert config.partition_key_names == ["job_type", "tile_id", "year_month"]
    assert config.max_keys == 25000
    assert config.max_depth == 1000
    assert config.function_name == "rollup-fn"
    assert config.metric_namespace == "BatchEventJobMonitor"


def test_handler_dispatches_rollup_by_default(
    environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []

    def mock_run_rollup(**kwargs: Any) -> dict[str, int]:
        calls.append(kwargs["depth"])
        return {"RolledUpRecords": 0}

    monkeypatch.setattr(rollup_handler, "run_rollup", mock_run_rollup)

    rollup_handler.handler({}, _context())

    assert calls == [0]


def test_handler_dispatches_reconcile_with_continuation(
    environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    def mock_run_reconcile(**kwargs: Any) -> dict[str, int]:
        captured.update(kwargs)
        return {"ReconcileDrift": 0}

    monkeypatch.setattr(rollup_handler, "run_reconcile", mock_run_reconcile)

    context = _context()
    rollup_handler.handler(
        {
            "mode": "reconcile",
            "depth": 2,
            "query_execution_id": "qid-1",
            "next_token": "token-2",
        },
        context,
    )

    assert captured["depth"] == 2
    assert captured["query_execution_id"] == "qid-1"
    assert captured["next_token"] == "token-2"
    # Guards against a regression to the hardcoded 900s default, which
    # would silently break reconcile chaining's time-budget check.
    assert captured["time_remaining_ms"] == context.get_remaining_time_in_millis


def test_handler_rejects_an_unknown_mode(environment: None) -> None:
    with pytest.raises(ValueError, match="unknown rollup mode"):
        rollup_handler.handler({"mode": "nonsense"}, _context())


class _Context:
    def get_remaining_time_in_millis(self) -> int:
        return 900_000


def _context() -> Any:
    return _Context()
