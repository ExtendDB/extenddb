# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures for extenddb dual-target tests.

Tests run against both real DynamoDB and extenddb with identical assertions.
The target is controlled by the EXTENDDB_TEST_ENDPOINT environment variable:
  - Unset or empty: tests run against real DynamoDB (requires AWS credentials)
  - Set to a URL: tests run against extenddb at that URL

REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import os
import time
import uuid

import boto3
import pytest
import urllib3
from botocore.config import Config

# D4: Suppress InsecureRequestWarning for self-signed TLS certs from ``extenddb init``.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
@pytest.fixture(scope="session")
def endpoint_url() -> str | None:
    """Return the endpoint URL if targeting extenddb, None for real DynamoDB."""
    url = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    return url if url else None
@pytest.fixture(scope="session")
def dynamodb_client(endpoint_url: str | None):
    """Create a boto3 DynamoDB client targeting either extenddb or real DynamoDB.

    When targeting extenddb over HTTPS with a self-signed certificate, SSL
    verification is disabled (the default ``extenddb init`` cert is self-signed).
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        # D4: Self-signed certs from `extenddb init` — disable SSL verification.
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)
@pytest.fixture(scope="session")
def dynamodb_client_no_validation(endpoint_url: str | None):
    """DynamoDB client with parameter validation disabled.

    Use this when testing that the *service* rejects invalid parameters,
    bypassing botocore's client-side validation.
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(parameter_validation=False),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)
@pytest.fixture()
def unique_table_name() -> str:
    """Generate a unique table name for test isolation."""
    return f"extenddb-test-{uuid.uuid4().hex[:12]}"
def _is_real_dynamodb() -> bool:
    """Return True when tests target real DynamoDB (no EXTENDDB_TEST_ENDPOINT)."""
    return not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()


def _poll_interval() -> float:
    """Return polling interval: 200ms for real DynamoDB, 20ms for ExtendDB."""
    return 0.2 if _is_real_dynamodb() else 0.02


def wait_for_active(client, table_name: str, timeout: float = 120.0) -> None:
    """Poll DescribeTable until status is ACTIVE.

    Shared helper — import from conftest instead of duplicating per-module.
    """
    interval = _poll_interval()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.describe_table(TableName=table_name)
        if resp["Table"]["TableStatus"] == "ACTIVE":
            return
        time.sleep(interval)
    raise TimeoutError(f"Table {table_name} did not become ACTIVE within {timeout}s")
@pytest.fixture()
def create_and_cleanup_table(dynamodb_client, unique_table_name):
    """Create a table and ensure it's deleted after the test (REQ-TEST-005)."""
    created_tables: list[str] = []

    def _create(table_name: str | None = None, **kwargs) -> dict:
        name = table_name or unique_table_name
        defaults = {
            "TableName": name,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        defaults.update(kwargs)
        result = dynamodb_client.create_table(**defaults)
        created_tables.append(name)
        # D-2: Always wait for ACTIVE — matches real DynamoDB behavior.
        wait_for_active(dynamodb_client, name)
        return result

    yield _create

    # Cleanup: delete all tables created during the test, then wait for
    # deletion to complete so teardown doesn't race with the next test.
    for name in created_tables:
        try:
            dynamodb_client.delete_table(TableName=name)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            continue
        except dynamodb_client.exceptions.ResourceInUseException:
            # Table may be UPDATING (e.g., billing mode switch). Wait for
            # ACTIVE then retry the delete.
            wait_for_active(dynamodb_client, name)
            try:
                dynamodb_client.delete_table(TableName=name)
            except dynamodb_client.exceptions.ResourceNotFoundException:
                continue
        # Wait for the table to be fully removed.
        wait_for_deleted(dynamodb_client, name)
def wait_for_deleted(client, table_name: str, timeout: float = 300.0) -> None:
    """Poll DescribeTable until the table no longer exists."""
    interval = _poll_interval()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.describe_table(TableName=table_name)
        except client.exceptions.ResourceNotFoundException:
            return
        time.sleep(interval)
    raise TimeoutError(f"Table {table_name} was not deleted within {timeout}s")


from contextlib import contextmanager
from typing import Generator


@contextmanager
def scoped_table(
    client,
    attribute_definitions: list[dict] | None = None,
    key_schema: list[dict] | None = None,
    **extra_kwargs,
) -> Generator[str, None, None]:
    """Create a uniquely-named table, yield its name, delete on exit.

    Use this in class/module-scoped fixtures to avoid repeating
    create/wait/delete boilerplate:

        @pytest.fixture(scope="class")
        def my_table(dynamodb_client):
            with scoped_table(dynamodb_client) as name:
                yield name
    """
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    create_kwargs: dict = {
        "TableName": name,
        "AttributeDefinitions": attribute_definitions or [
            {"AttributeName": "pk", "AttributeType": "S"},
        ],
        "KeySchema": key_schema or [
            {"AttributeName": "pk", "KeyType": "HASH"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
    }
    create_kwargs.update(extra_kwargs)
    client.create_table(**create_kwargs)
    wait_for_active(client, name)
    try:
        yield name
    finally:
        try:
            client.delete_table(TableName=name)
        except client.exceptions.ResourceNotFoundException:
            pass  # Already deleted by the test itself.
