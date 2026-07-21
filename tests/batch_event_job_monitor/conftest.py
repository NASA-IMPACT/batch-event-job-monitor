import os
from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

# ---------------------------------------------------------------------------
# AWS credentials / mocking
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_credentials() -> None:
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


@pytest.fixture
def s3(aws_credentials: None) -> Iterator[S3Client]:
    with mock_aws():
        yield boto3.client("s3", region_name="us-west-2")


@pytest.fixture
def bucket(s3: S3Client) -> str:
    bucket_name = "test-processing"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    return bucket_name


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------


@pytest.fixture
def sqs(aws_credentials: None) -> Iterator[SQSClient]:
    with mock_aws():
        yield boto3.client("sqs", region_name="us-west-2")


@pytest.fixture
def retry_queue_url(sqs: SQSClient) -> str:
    return sqs.create_queue(QueueName="test-retry-queue")["QueueUrl"]


@pytest.fixture
def dlq_url(sqs: SQSClient) -> str:
    return sqs.create_queue(QueueName="test-dlq")["QueueUrl"]
