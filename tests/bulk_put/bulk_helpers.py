# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures and helpers for the bulk operations suite."""
from __future__ import annotations

import gzip
import json
import os
import re
import sys
import time
import uuid

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from bulk_put import bulk_store

# The bulk suite is the package tests/bulk_put, one directory below the main
# suite. Its own conftest is bulk_put.conftest, so the top-level tests/conftest.py
# keeps the module name conftest and is imported here by name once tests/ is on
# the path.
_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _TESTS_DIR not in sys.path:
    sys.path.append(_TESTS_DIR)

from conftest import wait_for_active, wait_for_deleted  # noqa: E402
from management_helpers import ManagementClient  # noqa: E402

ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "http://localhost:18443").strip()
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("EXTENDDB_TEST_ACCOUNT_ID", "123456789012")

# Messages the server answers with, asserted verbatim.
MISSING_KEY_MESSAGE = (
    "One or more parameter values were invalid: Missing the key sk in the item"
)
CONDITION_FAILED_MESSAGE = "The conditional request failed"
IDEMPOTENT_MISMATCH_MESSAGE = (
    "Specified idempotent token was already used with different request "
    "parameters within the idempotency window"
)
TABLE_BUSY_MESSAGE = "Table has ongoing bulk operations, please wait and try again"
MONGODB_UNSUPPORTED_MESSAGE = (
    "Bulk operations are not supported on this storage backend"
)
ARN_SHAPE_MESSAGE = "Invalid Bulk operation ARN"
ARN_ID_MESSAGE = "Invalid Request: Bulk operation ARN is invalid."

OPERATION_ARN_PATTERN = re.compile(
    rf"^arn:aws:dynamodb:{re.escape(REGION)}:(\d{{12}}):bulk-operation/"
    r"[0-9]{14}-[0-9a-f]{8}$"
)

TERMINAL_STATUSES = {"COMPLETED", "COMPLETED_WITH_ERRORS", "FAILED", "CANCELLED"}

def _signed_post(operation: str, body: dict, creds: tuple[str, str] | None = None):
    """Send a raw DynamoDB-format request with SigV4 authentication.

    Returns ``(status_code, parsed body)`` without raising, so error tests can
    assert the exact error type and message.
    """
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }

    if creds is None:
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    else:
        access_key, secret_key = creds
    if access_key and secret_key:
        credentials = Credentials(access_key, secret_key)
        aws_req = AWSRequest(method="POST", url=ENDPOINT, data=body_bytes, headers=headers)
        SigV4Auth(credentials, "dynamodb", REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)

    resp = requests.post(
        ENDPOINT,
        data=body_bytes,
        headers=headers,
        verify=not ENDPOINT.startswith("https://"),
    )
    return resp.status_code, resp.json()


def extenddb_request(operation: str, body: dict,
                     creds: tuple[str, str] | None = None) -> dict:
    """Send a raw request and return the response body, raising on errors."""
    status_code, result = _signed_post(operation, body, creds)
    if status_code >= 400:
        error_type = result.get("__type", "Unknown")
        error_msg = result.get("message", result.get("Message", ""))
        raise RuntimeError(f"{error_type}: {error_msg} (HTTP {status_code})")
    return result


def expect_error(operation: str, body: dict, error_type: str, message: str,
                 creds: tuple[str, str] | None = None) -> None:
    """Assert a request fails with exactly this error type and message."""
    status_code, result = _signed_post(operation, body, creds)
    assert status_code >= 400, (
        f"{operation} unexpectedly succeeded: {result}"
    )
    got_type = result.get("__type", "").split("#")[-1]
    got_message = result.get("message", result.get("Message", ""))
    assert got_type == error_type, (
        f"{operation}: expected {error_type}, got {got_type}: {got_message}"
    )
    assert got_message == message, (
        f"{operation}: expected message {message!r}, got {got_message!r}"
    )


# ---------------------------------------------------------------------------
# Bucket and object helpers
#
# Inputs are objects under a bucket and reports are written to the output
# bucket under the operation's keys, on whichever store bulk_store selects.
# On the filesystem store a bucket is {root}/{account}/{S3Bucket}.
# ---------------------------------------------------------------------------

BULK_STORE = bulk_store.STORE

filesystem_store_only = pytest.mark.skipif(
    BULK_STORE != bulk_store.FILESYSTEM,
    reason="holds on the filesystem store only (EXTENDDB_TEST_BULK_STORE)",
)
s3_store_only = pytest.mark.skipif(
    BULK_STORE != bulk_store.S3,
    reason="holds on the S3 store only (EXTENDDB_TEST_BULK_STORE=s3)",
)


def bucket_dir(bucket: str, account: str = ACCOUNT_ID) -> str:
    """Return (creating if needed) a bucket directory in an account subtree.

    Filesystem store only; create_bucket serves both stores.
    """
    return bulk_store.bucket_dir(bucket, account)


def create_bucket(bucket: str, account: str = ACCOUNT_ID) -> None:
    """Create a bucket on the configured store."""
    bulk_store.create_bucket(bucket, account)


def write_object(bucket: str, key: str, payload: bytes,
                 account: str = ACCOUNT_ID) -> int:
    """Write one object under a bucket; return its size in bytes."""
    bulk_store.put_object(bucket, key, payload, account)
    return len(payload)


def write_rows(bucket: str, key: str, rows: list[dict],
               account: str = ACCOUNT_ID, compress: bool = False) -> int:
    """Write rows as newline-delimited JSON under a bucket; return the
    object's size in bytes."""
    payload = "".join(json.dumps(row) + "\n" for row in rows).encode("utf-8")
    if compress:
        payload = gzip.compress(payload)
    return write_object(bucket, key, payload, account)


def discard_bucket(bucket: str, account: str = ACCOUNT_ID) -> None:
    """Remove a bucket and its objects; the bucket may or may not exist."""
    bulk_store.discard_bucket(bucket, account)


def _output_prefix(prefix: str) -> str:
    # The configured output prefix as it precedes AWSDynamoDB/ in every
    # output key: an empty prefix contributes nothing, a prefix ending in
    # "/" is used as it is, any other prefix gets one "/" appended.
    return prefix if prefix == "" or prefix.endswith("/") else f"{prefix}/"


def report_key(prefix: str, operation_arn: str, kind: str,
               account: str = ACCOUNT_ID, index: int = 0) -> str:
    """The key of one report object of an operation, in the output bucket."""
    operation_id = operation_arn.rsplit("/", 1)[1]
    return (
        f"{_output_prefix(prefix)}AWSDynamoDB/BulkOperation/"
        f"{account}:{operation_id}-0/output/{kind}/output-{index:05d}.json.gz"
    )


def manifest_key(prefix: str, operation_arn: str,
                 account: str = ACCOUNT_ID) -> str:
    """The key of an operation's manifest, in the output bucket."""
    operation_id = operation_arn.rsplit("/", 1)[1]
    return (
        f"{_output_prefix(prefix)}AWSDynamoDB/BulkOperation/"
        f"{account}:{operation_id}/manifest-files.json"
    )


def read_report(bucket: str, prefix: str, operation_arn: str, kind: str,
                account: str = ACCOUNT_ID) -> list[dict] | None:
    """The records of an operation's first report object of one kind, or
    None when the object was not written."""
    payload = bulk_store.get_object(
        bucket, report_key(prefix, operation_arn, kind, account), account
    )
    if payload is None:
        return None
    text = gzip.decompress(payload).decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def read_manifest(bucket: str, prefix: str, operation_arn: str,
                  account: str = ACCOUNT_ID) -> list[dict] | None:
    """The lines of an operation's manifest, or None when it was not written."""
    payload = bulk_store.get_object(
        bucket, manifest_key(prefix, operation_arn, account), account
    )
    if payload is None:
        return None
    text = payload.decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Request and polling helpers
# ---------------------------------------------------------------------------


def start_body(table: str, bucket: str, in_prefix: str = "in/",
               out_prefix: str = "reports",
               output_types: list[str] | None = None, **extra) -> dict:
    """A StartBulkPutItems body reading one bucket and reporting to it."""
    if output_types is None:
        output_types = ["ERROR", "CONDITION_CHECK_FAILURE"]
    body = {
        "TableName": table,
        "InputS3Configuration": {
            "S3Bucket": bucket,
            "S3KeyPrefix": in_prefix,
            "InputFormat": "DYNAMODB_JSON",
        },
        "OutputS3Configuration": {
            "S3Bucket": bucket,
            "S3KeyPrefix": out_prefix,
            "OutputType": output_types,
        },
    }
    body.update(extra)
    return body


def wait_until_terminal(operation_arn: str, timeout: float = 30.0,
                        creds: tuple[str, str] | None = None) -> dict:
    """Poll DescribeBulkOperation until a terminal status; return it."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = extenddb_request(
            "DescribeBulkOperation", {"BulkOperationArn": operation_arn}, creds
        )["BulkOperationDescription"]
        if desc["BulkOperationStatus"] in TERMINAL_STATUSES:
            return desc
        time.sleep(0.05)
    raise TimeoutError(
        f"operation {operation_arn} not terminal within {timeout}s: "
        f"{desc['BulkOperationStatus']}"
    )


def list_all_operations(creds: tuple[str, str] | None = None) -> list[dict]:
    """Every operation summary the caller can list, across all pages."""
    summaries: list[dict] = []
    token = None
    while True:
        body: dict = {}
        if token is not None:
            body["NextToken"] = token
        resp = extenddb_request("ListBulkOperations", body, creds)
        summaries.extend(resp["BulkOperationsSummaryList"])
        token = resp.get("NextToken")
        if token is None:
            return summaries


def row(pk: str, sk: str | None = None, **attrs) -> dict:
    """One input row with string key attributes."""
    item: dict = {"pk": {"S": pk}}
    if sk is not None:
        item["sk"] = {"S": sk}
    item.update(attrs)
    return {"Item": item}


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def _require_admin_env() -> ManagementClient:
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: account tests require EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD. These must be set by devtools/run-tests "
            "before test execution."
        )
    return ManagementClient(ENDPOINT, admin_user, admin_pass)

S3_ACTIONS = ["s3:ListBucket", "s3:GetObject", "s3:PutObject"]


def _provision_account(mgmt: ManagementClient, user: str):
    """Create an account with one full-access user; return (account, creds).

    The user holds dynamodb:* in the policy named "full" and, in the policy
    named "s3", the three S3 actions a bulk operation performs as its caller.
    """
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    assert mgmt.create_account(account_id, f"{user}-acct-{account_id}").status_code == 201
    assert mgmt.create_user(account_id, user, f"{user.title()}Pass123!").status_code == 201
    key = mgmt.create_access_key(account_id, user).json()
    policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
    }
    assert mgmt.put_user_policy(account_id, user, "full", policy).status_code == 204
    s3 = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": S3_ACTIONS, "Resource": "*"}],
    }
    assert mgmt.put_user_policy(account_id, user, "s3", s3).status_code == 204
    return account_id, (key["access_key_id"], key["secret_access_key"])

def _release_account(mgmt: ManagementClient, account_id: str, user: str) -> None:
    mgmt.delete_user(account_id, user)
    mgmt.delete_account(account_id)

def _boto_client(creds: tuple[str, str]):
    import boto3
    from botocore.config import Config as BotoConfig

    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=ENDPOINT,
        aws_access_key_id=creds[0],
        aws_secret_access_key=creds[1],
        region_name=REGION,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if ENDPOINT.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

__all__ = [
    'ENDPOINT',
    'REGION',
    'ACCOUNT_ID',
    'MISSING_KEY_MESSAGE',
    'CONDITION_FAILED_MESSAGE',
    'IDEMPOTENT_MISMATCH_MESSAGE',
    'TABLE_BUSY_MESSAGE',
    'MONGODB_UNSUPPORTED_MESSAGE',
    'ARN_SHAPE_MESSAGE',
    'ARN_ID_MESSAGE',
    'OPERATION_ARN_PATTERN',
    'TERMINAL_STATUSES',
    '_signed_post',
    'extenddb_request',
    'expect_error',
    'BULK_STORE',
    'filesystem_store_only',
    's3_store_only',
    'bucket_dir',
    'create_bucket',
    'write_object',
    'write_rows',
    'discard_bucket',
    'report_key',
    'manifest_key',
    'read_report',
    'read_manifest',
    'start_body',
    'wait_until_terminal',
    'list_all_operations',
    'row',
    '_require_admin_env',
    '_provision_account',
    '_release_account',
    '_boto_client',
    'wait_for_active',
    'wait_for_deleted',
    'ManagementClient',
]
