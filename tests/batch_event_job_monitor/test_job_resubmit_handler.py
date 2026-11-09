"""Tests for the bundled job-resubmit Lambda handler."""

from __future__ import annotations

import json
import logging
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from batch_event_job_monitor.models import JobTypeConfig, RetryMessage

_JOB_QUEUE_ARN = "arn:aws:batch:us-west-2:123456789012:job-queue/queue"
_JOB_DEFINITION_ARN = "arn:aws:batch:us-west-2:123456789012:job-definition/def"
_MESSAGE_ID = "message-1"
_BODY = RetryMessage(
    job_type="composite",
    partition_fields={"year_month": "2025-01"},
    input_entity_ids=["T14TPN_2025-01"],
    output_entity_id="T14TPN_2025-01",
    attempt=1,
    batch_job_id="job-1",
    state="FAILURE",
).to_json()


@pytest.fixture
def handler_module(aws_credentials: None, monkeypatch: pytest.MonkeyPatch) -> Any:
    from batch_event_job_monitor.handlers import job_resubmit_handler

    client = MagicMock()
    client.submit_job.return_value = {"jobId": "job-2"}
    monkeypatch.setattr(job_resubmit_handler, "_batch_client", client)
    monkeypatch.setenv(
        "PROCESSING_JOB_TYPE_CONFIGS",
        json.dumps(
            {
                "composite": JobTypeConfig(
                    job_queue_arn=_JOB_QUEUE_ARN,
                    job_definition_arn=_JOB_DEFINITION_ARN,
                ).to_dict()
            }
        ),
    )
    return job_resubmit_handler


def _invoke(handler_module: Any, body: str) -> Any:
    event = {"Records": [{"messageId": _MESSAGE_ID, "body": body}]}
    return handler_module.handler(cast(Any, event), cast(Any, None))


def _access_denied() -> ClientError:
    return ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}},
        "SubmitJob",
    )


def test_submits_the_next_attempt_and_reports_no_failures(
    handler_module: Any, caplog: pytest.LogCaptureFixture
) -> None:
    result = _invoke(handler_module, _BODY)

    assert result == {"batchItemFailures": []}
    params = handler_module._batch_client.submit_job.call_args.kwargs
    assert params["jobDefinition"] == _JOB_DEFINITION_ARN
    assert params["parameters"]["bejm_attempt"] == "2"
    assert not caplog.records


def test_submit_failure_is_logged_and_reported(
    handler_module: Any, caplog: pytest.LogCaptureFixture
) -> None:
    handler_module._batch_client.submit_job.side_effect = _access_denied()

    result = _invoke(handler_module, _BODY)

    assert result == {"batchItemFailures": [{"itemIdentifier": _MESSAGE_ID}]}
    (record,) = caplog.records
    assert record.levelno == logging.ERROR
    assert _MESSAGE_ID in record.getMessage()
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ClientError)


def test_unparseable_body_is_logged_with_the_body_and_reported(
    handler_module: Any, caplog: pytest.LogCaptureFixture
) -> None:
    result = _invoke(handler_module, "not json")

    assert result == {"batchItemFailures": [{"itemIdentifier": _MESSAGE_ID}]}
    (record,) = caplog.records
    assert record.levelno == logging.ERROR
    assert "not json" in record.getMessage()
    handler_module._batch_client.submit_job.assert_not_called()
