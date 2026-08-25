"""Tests for the ProcessingBucket CDK construct."""

from __future__ import annotations

import pytest
from aws_cdk import App, RemovalPolicy, Stack
from aws_cdk.assertions import Match, Template

from batch_event_job_monitor_cdk.processing_bucket import ProcessingBucket


def _make_bucket(
    *,
    bucket_name: str = "test-processing-bucket",
    inventory_prefix: str = "inventory/",
    inventories: list[tuple[str, str]] | None = None,
) -> tuple[Stack, ProcessingBucket]:
    app = App()
    stack = Stack(app, "TestStack")
    construct = ProcessingBucket(
        stack,
        "TestProcessingBucket",
        bucket_name=bucket_name,
        inventory_prefix=inventory_prefix,
        inventories=inventories
        if inventories is not None
        else [("state", "state/"), ("outputs", "outputs/")],
    )
    return stack, construct


class TestProcessingBucketInventories:
    """Tests for the per-inventory S3 Inventory configuration."""

    def test_creates_bucket_resource(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        template.resource_count_is("AWS::S3::Bucket", 1)

    def test_state_inventory_configuration(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "InventoryConfigurations": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Id": "state",
                                "Prefix": "state/",
                                "Enabled": True,
                                "ScheduleFrequency": "Daily",
                                "Destination": Match.object_like({"Format": "Parquet"}),
                                "OptionalFields": ["LastModifiedDate"],
                            }
                        )
                    ]
                )
            },
        )

    def test_outputs_inventory_configuration(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "InventoryConfigurations": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Id": "outputs",
                                "Prefix": "outputs/",
                                "Enabled": True,
                                "ScheduleFrequency": "Daily",
                                "Destination": Match.object_like({"Format": "Parquet"}),
                                "OptionalFields": ["LastModifiedDate"],
                            }
                        )
                    ]
                )
            },
        )

    def test_both_inventories_present_simultaneously(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        resources = template.to_json()["Resources"]
        bucket_resources = [
            r for r in resources.values() if r["Type"] == "AWS::S3::Bucket"
        ]
        [bucket] = [
            r for r in bucket_resources if "InventoryConfigurations" in r["Properties"]
        ]
        ids = {
            config["Id"] for config in bucket["Properties"]["InventoryConfigurations"]
        }
        assert ids == {"state", "outputs"}

    def test_arbitrary_inventory_pairs(self) -> None:
        stack, _ = _make_bucket(
            inventories=[("records", "records/"), ("logs", "logs/prefix/")]
        )
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "InventoryConfigurations": Match.array_with(
                    [
                        Match.object_like({"Id": "records", "Prefix": "records/"}),
                        Match.object_like({"Id": "logs", "Prefix": "logs/prefix/"}),
                    ]
                )
            },
        )


class TestProcessingBucketPolicyAndLifecycle:
    """Tests for the inventory-destination bucket policy and lifecycle rule."""

    def test_bucket_policy_grants_s3_put_object_to_service_principal(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::S3::BucketPolicy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": "s3:PutObject",
                                    "Effect": "Allow",
                                    "Principal": {"Service": "s3.amazonaws.com"},
                                }
                            )
                        ]
                    )
                }
            },
        )

    def test_lifecycle_rule_expires_inventory_prefix(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "LifecycleConfiguration": {
                    "Rules": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "ExpirationInDays": 14,
                                    "Prefix": "inventory/",
                                    "Status": "Enabled",
                                }
                            )
                        ]
                    )
                }
            },
        )


class TestInventoryLocation:
    """Tests for the inventory_location helper method."""

    def test_returns_expected_s3_path_for_state(self) -> None:
        _, construct = _make_bucket(
            bucket_name="my-bucket", inventory_prefix="inventory/"
        )
        assert (
            construct.inventory_location("state")
            == "s3://my-bucket/inventory/my-bucket/state/hive/"
        )

    def test_returns_expected_s3_path_for_outputs(self) -> None:
        _, construct = _make_bucket(
            bucket_name="my-bucket", inventory_prefix="inventory/"
        )
        assert (
            construct.inventory_location("outputs")
            == "s3://my-bucket/inventory/my-bucket/outputs/hive/"
        )

    def test_reflects_custom_bucket_and_prefix(self) -> None:
        _, construct = _make_bucket(
            bucket_name="other-bucket", inventory_prefix="reports/"
        )
        assert (
            construct.inventory_location("records")
            == "s3://other-bucket/reports/other-bucket/records/hive/"
        )


class TestRemovalPolicy:
    """Tests for the removal_policy / auto_delete_objects parameters."""

    def test_defaults_to_retain_on_update_or_delete(self) -> None:
        stack, _ = _make_bucket()
        template = Template.from_stack(stack)
        [bucket] = [
            r
            for r in template.to_json()["Resources"].values()
            if r["Type"] == "AWS::S3::Bucket"
        ]
        assert bucket["DeletionPolicy"] == "RetainExceptOnCreate"
        assert bucket["UpdateReplacePolicy"] == "Retain"

    def test_destroy_is_expressible(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")
        ProcessingBucket(
            stack,
            "TestProcessingBucket",
            bucket_name="dev-bucket",
            inventory_prefix="inventory/",
            inventories=[("state", "state/")],
            removal_policy=RemovalPolicy.DESTROY,
        )
        template = Template.from_stack(stack)
        [bucket] = [
            r
            for r in template.to_json()["Resources"].values()
            if r["Type"] == "AWS::S3::Bucket"
        ]
        assert bucket["DeletionPolicy"] == "Delete"

    def test_auto_delete_objects_adds_the_custom_resource(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")
        ProcessingBucket(
            stack,
            "TestProcessingBucket",
            bucket_name="dev-bucket",
            inventory_prefix="inventory/",
            inventories=[("state", "state/")],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        template = Template.from_stack(stack)
        template.resource_count_is("Custom::S3AutoDeleteObjects", 1)

    def test_auto_delete_objects_requires_destroy(self) -> None:
        app = App()
        stack = Stack(app, "TestStack")
        with pytest.raises(ValueError, match=r"RemovalPolicy\.DESTROY"):
            ProcessingBucket(
                stack,
                "TestProcessingBucket",
                bucket_name="dev-bucket",
                inventory_prefix="inventory/",
                inventories=[("state", "state/")],
                auto_delete_objects=True,
            )
