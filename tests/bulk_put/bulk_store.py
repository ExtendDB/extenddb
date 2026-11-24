# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""The bulk store the suite writes its inputs to and reads its reports from.

EXTENDDB_TEST_BULK_STORE names it, and must name the store the server under
test is configured with (devtools/run-tests --bulk-store sets both):

- ``filesystem`` (the default): a bucket is the directory
  ``{root}/{account}/{bucket}``, where the root is the tempfile directory,
  the layout of the server's ``[bulk.filesystem]`` store rooted there.
- ``s3``: a bucket on the S3-compatible service at EXTENDDB_TEST_S3_ENDPOINT,
  reached with path-style addressing and the keys
  EXTENDDB_TEST_S3_ACCESS_KEY_ID and EXTENDDB_TEST_S3_SECRET_ACCESS_KEY.
  Buckets are created and emptied through the S3 API. Bucket names are
  global there, so the account names no namespace; the suite's bucket names
  are unique per test.

Objects are named by their bucket and their key within it.
"""
from __future__ import annotations

import os
import shutil
import tempfile

import pytest

FILESYSTEM = "filesystem"
S3 = "s3"

STORE = os.environ.get("EXTENDDB_TEST_BULK_STORE", "").strip() or FILESYSTEM
if STORE not in (FILESYSTEM, S3):
    raise ValueError(
        f"EXTENDDB_TEST_BULK_STORE must be {FILESYSTEM!r} or {S3!r}, got {STORE!r}"
    )

_client = None


def s3_client():
    """The S3 client of the ``s3`` store, built on first use."""
    global _client
    if _client is None:
        import boto3
        from botocore.config import Config

        endpoint = os.environ.get("EXTENDDB_TEST_S3_ENDPOINT", "").strip()
        access_key = os.environ.get("EXTENDDB_TEST_S3_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("EXTENDDB_TEST_S3_SECRET_ACCESS_KEY", "")
        if not (endpoint and access_key and secret_key):
            pytest.fail(
                "MISCONFIGURED: EXTENDDB_TEST_BULK_STORE=s3 requires "
                "EXTENDDB_TEST_S3_ENDPOINT, EXTENDDB_TEST_S3_ACCESS_KEY_ID and "
                "EXTENDDB_TEST_S3_SECRET_ACCESS_KEY. devtools/run-tests "
                "--bulk-store s3 reads the same three."
            )
        _client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}),
        )
    return _client


def _error_code(error) -> str:
    return error.response.get("Error", {}).get("Code", "")


def _bucket_path(bucket: str, account: str) -> str:
    return os.path.join(tempfile.gettempdir(), account, bucket)


def bucket_dir(bucket: str, account: str) -> str:
    """Return (creating if needed) a bucket directory. Filesystem store only."""
    assert STORE == FILESYSTEM, "a bucket directory exists on the filesystem store only"
    path = _bucket_path(bucket, account)
    os.makedirs(path, exist_ok=True)
    return path


def create_bucket(bucket: str, account: str) -> None:
    """Create the bucket; an existing bucket is kept as it is."""
    if STORE == FILESYSTEM:
        bucket_dir(bucket, account)
        return
    from botocore.exceptions import ClientError

    try:
        s3_client().create_bucket(Bucket=bucket)
    except ClientError as error:
        if _error_code(error) not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def put_object(bucket: str, key: str, payload: bytes, account: str) -> None:
    """Write one object, creating the bucket when it does not exist."""
    create_bucket(bucket, account)
    if STORE == FILESYSTEM:
        path = os.path.join(_bucket_path(bucket, account), key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(payload)
        return
    s3_client().put_object(Bucket=bucket, Key=key, Body=payload)


def get_object(bucket: str, key: str, account: str) -> bytes | None:
    """The object's bytes, or None when the object or its bucket is absent."""
    if STORE == FILESYSTEM:
        path = os.path.join(_bucket_path(bucket, account), key)
        if not os.path.isfile(path):
            return None
        with open(path, "rb") as f:
            return f.read()
    from botocore.exceptions import ClientError

    try:
        return s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as error:
        if _error_code(error) in ("NoSuchKey", "NoSuchBucket", "404"):
            return None
        raise


def discard_bucket(bucket: str, account: str) -> None:
    """Remove a bucket and everything in it; an absent bucket is fine."""
    if STORE == FILESYSTEM:
        shutil.rmtree(_bucket_path(bucket, account), ignore_errors=True)
        return
    from botocore.exceptions import ClientError

    client = s3_client()
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for listed in page.get("Contents", []):
                client.delete_object(Bucket=bucket, Key=listed["Key"])
        uploads = client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        for upload in uploads:
            client.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        client.delete_bucket(Bucket=bucket)
    except ClientError as error:
        if _error_code(error) != "NoSuchBucket":
            raise
