"""Processing S3 bucket with daily Parquet S3 Inventory configurations.

Wraps an s3.Bucket with one S3 Inventory configuration per (inventory_id,
objects_prefix) pair, all delivered as Hive-partitioned Parquet under a shared
inventory-prefix root, plus the lifecycle rule and self-grant that inventory
delivery requires.
"""

from __future__ import annotations

from typing import Any

from aws_cdk import (
    Aws,
    Duration,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct

# Suffix CloudFormation appends to bucket_name_prefix for a bucket in the
# account regional namespace. See "Namespaces for general purpose buckets"
# in the S3 user guide.
_ACCOUNT_REGIONAL_SUFFIX = "an"


class ProcessingBucket(Construct):
    """Processing S3 bucket with N S3 Inventory configurations and lifecycle rules.

    Parameters
    ----------
    scope : Construct
        Parent construct.
    construct_id : str
        Construct id, unique within scope.
    bucket_name : str or None, optional
        Name of the managed S3 bucket in the shared global namespace.
        Mutually exclusive with bucket_name_prefix; exactly one of the two
        is required.
    bucket_name_prefix : str or None, optional
        Prefix of a bucket created in your account regional namespace,
        whose full name CloudFormation forms as
        ``{prefix}-{accountId}-{region}-an``. That namespace is reserved to
        your account, so the name can never be claimed or re-created by
        another account, and the same prefix templates cleanly across
        accounts and regions. The suffix counts against S3's 63-character
        limit, leaving 37 characters for the prefix; aws-cdk-lib validates
        the prefix's length and character set.
    inventory_prefix : str
        Common key prefix under which every inventory's reports are delivered.
        S3 further namespaces each inventory's reports by inventory_id below
        this prefix, so a single lifecycle rule and a single resource-policy
        grant cover every inventory configured on the bucket.
    inventories : list[tuple[str, str]]
        (inventory_id, objects_prefix) pairs. One daily Parquet S3 Inventory
        configuration is created per pair, covering objects under
        objects_prefix.
    removal_policy : RemovalPolicy, optional
        Removal policy for the managed bucket. Defaults to
        RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE, which keeps processing
        history when a production stack is replaced or destroyed. Pass
        RemovalPolicy.DESTROY (with auto_delete_objects=True) for an
        ephemeral dev stack that should tear down completely.
    auto_delete_objects : bool, optional
        Empty the bucket on stack deletion via CDK's auto-delete custom
        resource. Defaults to False. Requires removal_policy to be
        RemovalPolicy.DESTROY -- CloudFormation cannot delete a non-empty
        bucket, so DESTROY without this leaves the bucket behind.
    **kwargs : Any
        Additional keyword arguments forwarded to the Construct base class.

    Attributes
    ----------
    bucket : s3.Bucket
        The managed S3 bucket.
    bucket_name : str
        The bucket's full name. For an account regional bucket this is the
        prefix plus the namespace suffix, so it carries unresolved account
        and region tokens rather than being a plain literal.

    Raises
    ------
    ValueError
        If neither or both of bucket_name and bucket_name_prefix are given,
        or if auto_delete_objects is True and removal_policy is not
        RemovalPolicy.DESTROY.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        bucket_name: str | None = None,
        bucket_name_prefix: str | None = None,
        inventory_prefix: str,
        inventories: list[tuple[str, str]],
        removal_policy: RemovalPolicy = RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        auto_delete_objects: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if auto_delete_objects and removal_policy is not RemovalPolicy.DESTROY:
            raise ValueError(
                "auto_delete_objects=True requires removal_policy=RemovalPolicy.DESTROY"
            )

        if (bucket_name is None) == (bucket_name_prefix is None):
            raise ValueError(
                "exactly one of bucket_name (global namespace) or "
                "bucket_name_prefix (account regional namespace) is required"
            )

        if bucket_name_prefix is not None:
            # Compose the name CloudFormation will form rather than reading
            # it back off the bucket. bucket.bucket_name is a Ref, and this
            # bucket is its own inventory destination -- resolving that Ref
            # into the destination ARN and the bucket policy it needs would
            # be circular.
            self.bucket_name = (
                f"{bucket_name_prefix}-{Aws.ACCOUNT_ID}-{Aws.REGION}"
                f"-{_ACCOUNT_REGIONAL_SUFFIX}"
            )
            name_props: dict[str, Any] = {
                "bucket_name_prefix": bucket_name_prefix,
                "bucket_namespace": s3.BucketNamespace.ACCOUNT_REGIONAL,
            }
        else:
            assert bucket_name is not None
            self.bucket_name = bucket_name
            name_props = {"bucket_name": bucket_name}

        self.inventory_prefix = inventory_prefix

        self.bucket = s3.Bucket(
            self,
            "Bucket",
            removal_policy=removal_policy,
            auto_delete_objects=auto_delete_objects,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(expired_object_delete_marker=True),
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    noncurrent_version_expiration=Duration.days(1),
                ),
            ],
            **name_props,
        )

        # Import the destination rather than passing self.bucket, so CDK does
        # not call add_to_resource_policy on it. When source == destination
        # that creates a BucketPolicy <-> Bucket circular dependency. The
        # explicit add_to_resource_policy call below provides the grant.
        inventory_dest = s3.Bucket.from_bucket_arn(
            self,
            "InventoryDest",
            f"arn:aws:s3:::{self.bucket_name}",
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
