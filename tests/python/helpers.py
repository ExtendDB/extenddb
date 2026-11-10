# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared helpers for the comprehensive test suite.

These are plain functions (not fixtures) that test modules import directly.
Fixtures live in conftest.py and are auto-discovered by pytest.
"""

from __future__ import annotations

import os
import time
import uuid

import boto3
import botocore.config


def make_dynamodb_client(endpoint_url: str | None):
    """A boto3 DynamoDB client with SDK retries off, for the configured target.

    The session fixture in conftest is one of these. A test that drives the
    server from several threads must build one per thread with this function:
    a boto3 client is not safe for concurrent use, and sharing one across
    threads interleaves requests on its TLS connections, which surfaces as
    spurious `SSLError: TLSV1_ALERT_DECODE_ERROR` under load.
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": botocore.config.Config(
            retries={"max_attempts": 0},  # No SDK retries — we want raw errors
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


def wait_for_active(client, table_name: str, timeout: float = 60.0) -> None:
    """Poll DescribeTable until status is ACTIVE with exponential backoff."""
    deadline = time.monotonic() + timeout
    delay = 0.1
    while time.monotonic() < deadline:
        resp = client.describe_table(TableName=table_name)
        if resp["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(max(0, min(delay, deadline - time.monotonic())))
        delay = min(delay * 2, 2.0)
    raise TimeoutError(f"Table {table_name} did not become ACTIVE within {timeout}s")


def wait_for_deleted(client, table_name: str, timeout: float = 60.0) -> None:
    """Poll DescribeTable until the table no longer exists."""
    deadline = time.monotonic() + timeout
    delay = 0.1
    while time.monotonic() < deadline:
        try:
            client.describe_table(TableName=table_name)
        except client.exceptions.ResourceNotFoundException:
            return
        time.sleep(max(0, min(delay, deadline - time.monotonic())))
        delay = min(delay * 2, 2.0)
    raise TimeoutError(f"Table {table_name} was not deleted within {timeout}s")


def unique_name(prefix: str = "test") -> str:
    """Generate a unique name for test isolation."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# What GetShardIterator answers for a stream ARN that names no stream of the
# caller's account (measured 2026-10-24). GetRecords answers the same for such
# an ARN and for a shard that is not one of that stream's shards.
STREAM_DOES_NOT_EXIST = "Requested resource not found: Stream does not exist"


def describe_stream_not_found_message(stream_arn: str) -> str:
    """What DescribeStream answers for a stream ARN that names no stream of the
    caller's account (measured 2026-10-24): the ARN, and no trailing period."""
    return f"Requested resource not found: Stream: {stream_arn} not found"


def make_raw_client(endpoint_url: str | None, service_name: str = "dynamodb"):
    """A boto3 client with SDK parameter validation off and retries off, so the
    service, not the SDK, decides what a request is answered with. Used by the
    tests that send names and ARNs the SDK's model would refuse client-side."""
    kwargs: dict = {
        "service_name": service_name,
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": botocore.config.Config(
            parameter_validation=False, retries={"max_attempts": 0}
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
            kwargs["verify"] = ca_cert if ca_cert else False
    return boto3.client(**kwargs)


MEMBER_PATTERN = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"


def assert_validation(exc, message: str) -> None:
    """A 400 ValidationException with exactly this message."""
    resp = exc.response
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400, resp
    assert resp["Error"]["Code"] == "ValidationException", resp["Error"]
    assert resp["Error"]["Message"] == message, resp["Error"]["Message"]


def assert_member_pattern(exc, location: str, value: str) -> None:
    """The service's one-constraint member pattern sentence, byte for byte.

    The offending value is rendered inside ``Value '...'`` with a U+0000 as the
    raw character (the JSON body carries ``\\u0000``, which the SDK decodes).
    """
    assert_validation(
        exc,
        f"1 validation error detected: Value '{value}' at '{location}' "
        f"failed to satisfy constraint: {MEMBER_PATTERN}",
    )
