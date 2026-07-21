"""Processing S3 bucket with daily Parquet S3 Inventory configurations.

Wraps an s3.Bucket with one S3 Inventory configuration per (inventory_id,
objects_prefix) pair, all delivered as Hive-partitioned Parquet under a shared
inventory-prefix root, plus the lifecycle rule and self-grant that inventory
delivery requires.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class ProcessingBucket(Construct):
    """Processing S3 bucket with N S3 Inventory configurations and lifecycle rules.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    bucket_name : str
        Name of the managed S3 bucket. Passed as a literal string (not a CDK
        token) because it is embedded directly in an ARN used to import the
        bucket as its own inventory destination.
    inventory_prefix : str
        Common key prefix under which every inventory's reports are delivered.
        S3 further namespaces each inventory's reports by inventory_id below
        this prefix, so a single lifecycle rule and a single resource-policy
        grant cover every inventory configured on the bucket.
    inventories : list[tuple[str, str]]
        (inventory_id, objects_prefix) pairs. One daily Parquet S3 Inventory
        configuration is created per pair, covering objects under
        objects_prefix.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    bucket : s3.Bucket
        The managed S3 bucket.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket_name: str,
        inventory_prefix: str,
        inventories: list[tuple[str, str]],
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket_name = bucket_name
        self.inventory_prefix = inventory_prefix

        self.bucket = s3.Bucket(
            self,
            "Bucket",
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
        )

        # Use from_bucket_arn with a literal ARN (not a CDK token) so CDK does
        # not call add_to_resource_policy on the destination. When source ==
        # destination that creates a BucketPolicy <-> Bucket circular
        # dependency. The explicit add_to_resource_policy call below provides
        # the grant.
        inventory_dest = s3.Bucket.from_bucket_arn(
            self,
            "InventoryDest",
            f"arn:aws:s3:::{bucket_name}",
        )

        for inventory_id, objects_prefix in inventories:
            self._add_inventory(
                destination=inventory_dest,
                destination_prefix=inventory_prefix,
                inventory_id=inventory_id,
                objects_prefix=objects_prefix,
            )

        self.bucket.add_lifecycle_rule(
            prefix=inventory_prefix,
            expiration=Duration.days(14),
        )
        self.bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[self.bucket.arn_for_objects(f"{inventory_prefix}*")],
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                effect=iam.Effect.ALLOW,
            )
        )

    def _add_inventory(
        self,
        *,
        destination: s3.IBucket,
        destination_prefix: str,
        inventory_id: str,
        objects_prefix: str,
    ) -> None:
        """Configure a daily Parquet S3 Inventory under the shared root prefix.

        Parameters
        ----------
        destination : s3.IBucket
            Bucket that receives the inventory reports.
        destination_prefix : str
            Key prefix under which the reports are delivered.
        inventory_id : str
            Identifier for this inventory configuration.
        objects_prefix : str
            Key prefix of the objects this inventory covers.
        """
        self.bucket.add_inventory(
            enabled=True,
            destination=s3.InventoryDestination(
                bucket=destination,
                prefix=destination_prefix.rstrip("/"),
            ),
            inventory_id=inventory_id,
            format=s3.InventoryFormat.PARQUET,
            frequency=s3.InventoryFrequency.DAILY,
            objects_prefix=objects_prefix,
            optional_fields=["LastModifiedDate"],
        )

    def inventory_location(self, inventory_id: str) -> str:
        """S3 path of an inventory's Hive symlink manifests.

        S3 writes reports under {prefix}{source-bucket}/{inventory-id}/, with
        the Hive-style symlink manifests (dt=.../symlink.txt) under the hive/
        subprefix that SymlinkTextInputFormat reads.

        Parameters
        ----------
        inventory_id : str
            Identifier of one of this bucket's configured inventories.

        Returns
        -------
        str
            s3:// URI of the inventory's Hive-partitioned symlink manifests.
        """
        return (
            f"s3://{self.bucket_name}/{self.inventory_prefix}"
            f"{self.bucket_name}/{inventory_id}/hive/"
        )
