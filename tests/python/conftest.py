# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared pytest configuration for the comprehensive test suite.

Environment variables:
    DYNAMODB_ENDPOINT   — extenddb endpoint URL (omit for real DynamoDB)
    AWS_ACCESS_KEY_ID   — AWS access key
    AWS_SECRET_ACCESS_KEY — AWS secret key
    AWS_DEFAULT_REGION  — AWS region (default: us-east-1)
    EXTENDDB_CA_CERT        — Path to CA cert for self-signed TLS (optional)
    EXTENDDB_VALIDATION_MODE — When "true", skips extenddb_only tests

REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import os
import sys

# Ensure tests/python/ is on the import path so test modules can import helpers.
sys.path.insert(0, os.path.dirname(__file__))

import boto3
import botocore.config
import pytest
import urllib3

from helpers import make_dynamodb_client, unique_name, wait_for_active, wait_for_deleted  # noqa: F401

# Suppress InsecureRequestWarning for self-signed TLS certs.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _is_validation_mode() -> bool:
    """Return True if running in validation mode (against real DynamoDB)."""
    return os.environ.get("EXTENDDB_VALIDATION_MODE", "").lower() == "true"


# Custom markers
extenddb_only = pytest.mark.skipif(
    _is_validation_mode(),
    reason="extenddb_only: skipped in validation mode",
)


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "extenddb_only: test only runs against extenddb")


@pytest.fixture(scope="session")
def endpoint_url() -> str | None:
    """Return the endpoint URL if targeting extenddb, None for real DynamoDB."""
    url = os.environ.get("DYNAMODB_ENDPOINT", "").strip()
    return url if url else None


@pytest.fixture(scope="session")
def is_extenddb(endpoint_url) -> bool:
    """True when targeting a extenddb instance."""
    return endpoint_url is not None


@pytest.fixture(scope="session")
def dynamodb_client(endpoint_url: str | None):
    """Session-scoped boto3 DynamoDB client.

    Not safe to share across threads; a test that drives the server from a
    thread pool builds one client per thread with
    `helpers.make_dynamodb_client` instead.
    """
    return make_dynamodb_client(endpoint_url)


@pytest.fixture(scope="session")
def dynamodb_resource(endpoint_url: str | None):
    """Session-scoped boto3 DynamoDB resource."""
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
    return boto3.resource(**kwargs)


@pytest.fixture()
def table_factory(dynamodb_client):
    """Factory fixture: creates tables and cleans them up after the test.

    Usage:
        def test_something(table_factory, dynamodb_client):
            name = table_factory(
                AttributeDefinitions=[...],
                KeySchema=[...],
            )
            # table is ACTIVE, use it
    """
    created: list[str] = []

    def _create(
        table_name: str | None = None,
        *,
        hash_key: str = "pk",
        hash_type: str = "S",
        range_key: str | None = None,
        range_type: str = "S",
        **kwargs,
    ) -> str:
        name = table_name or unique_name("tbl")
        attrs = [{"AttributeName": hash_key, "AttributeType": hash_type}]
        schema = [{"AttributeName": hash_key, "KeyType": "HASH"}]
        if range_key:
            attrs.append({"AttributeName": range_key, "AttributeType": range_type})
            schema.append({"AttributeName": range_key, "KeyType": "RANGE"})

        call_kwargs = {
            "TableName": name,
            "AttributeDefinitions": attrs,
            "KeySchema": schema,
            "BillingMode": "PAY_PER_REQUEST",
        }
        call_kwargs.update(kwargs)
        dynamodb_client.create_table(**call_kwargs)
        created.append(name)
        wait_for_active(dynamodb_client, name)
        return name

    yield _create

    for name in created:
        try:
            dynamodb_client.delete_table(TableName=name)
        except Exception:
            pass
        try:
            wait_for_deleted(dynamodb_client, name)
        except Exception:
            pass
