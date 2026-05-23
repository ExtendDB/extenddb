# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end tests for write-through cache invalidation.

These tests exercise the round-trip: mutate IAM via the management API,
issue a SigV4 request immediately, observe the new behavior **without
waiting for any TTL**.

If the cache invalidation path is broken, these tests fail because the
old (stale) state still applies for up to `auth.cache.ttl_seconds`.

Prerequisites mirror tests/test_auth_integration.py: a running extenddb
with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT, plus admin
credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Cache-coherence tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, EXTENDDB_ADMIN_PASSWORD."
        )
    return endpoint, admin_user, admin_pass


@pytest.fixture(scope="module")
def auth_env() -> tuple[str, str, str]:
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"cache-coh-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


@pytest.fixture
def user(mgmt, account_id):
    user_name = f"cache-user-{uuid.uuid4().hex[:8]}"
    resp = mgmt.create_user(account_id, user_name, password=None)
    assert resp.status_code == 201, resp.text
    yield user_name
    # Best-effort delete; some tests delete the user themselves.
    try:
        mgmt.delete_user(account_id, user_name)
    except Exception:
        pass


def _ddb_client(endpoint_url: str, access_key: str, secret_key: str) -> Any:
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    cfg = BotoConfig(
        region_name=region,
        signature_version="v4",
        retries={"max_attempts": 0, "mode": "standard"},
    )
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
        verify=False,  # local TLS uses a self-signed cert
    )


def _put_allow_policy(mgmt, account_id, user_name, action: str):
    """Replace the user's policy with one allowing `action` on `*`."""
    doc = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": action, "Resource": "*"}],
    }
    resp = mgmt.put_user_policy(account_id, user_name, "test-policy", doc)
    assert resp.status_code in (200, 204), resp.text


def _put_deny_policy(mgmt, account_id, user_name, action: str):
    """Replace the user's policy with one explicitly denying `action`."""
    doc = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Deny", "Action": action, "Resource": "*"}],
    }
    resp = mgmt.put_user_policy(account_id, user_name, "test-policy", doc)
    assert resp.status_code in (200, 204), resp.text


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_put_user_policy_takes_effect_immediately(auth_env, mgmt, account_id, user):
    """After PutUserPolicy, the next request sees the new policy without TTL wait."""
    endpoint, _, _ = auth_env

    # Issue a key for the user.
    resp = mgmt.create_access_key(account_id, user)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    access_key, secret_key = creds["access_key_id"], creds["secret_access_key"]

    # Initially the user has no policy → ListTables denied.
    ddb = _ddb_client(endpoint, access_key, secret_key)
    with pytest.raises(ClientError) as exc:
        ddb.list_tables()
    assert "AccessDenied" in str(exc.value) or "AccessDeniedException" in str(exc.value)

    # Grant ListTables.
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")

    # Same user, same key — must succeed on the very next call (cache
    # invalidation is supposed to be instant for self-induced changes).
    resp = ddb.list_tables()
    assert "TableNames" in resp


def test_put_deny_policy_takes_effect_immediately(auth_env, mgmt, account_id, user):
    """An attached Deny shadows a prior Allow without TTL wait."""
    endpoint, _, _ = auth_env

    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])

    # Allow ListTables.
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")
    ddb.list_tables()  # warms the cache with the Allow policy

    # Replace with a Deny policy.
    _put_deny_policy(mgmt, account_id, user, "dynamodb:ListTables")

    # Next call must be denied.
    with pytest.raises(ClientError) as exc:
        ddb.list_tables()
    assert "AccessDenied" in str(exc.value) or "AccessDeniedException" in str(exc.value)


def test_delete_access_key_takes_effect_immediately(auth_env, mgmt, account_id, user):
    """A deleted access key is rejected on the next request without TTL wait."""
    endpoint, _, _ = auth_env

    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    access_key = creds["access_key_id"]
    ddb = _ddb_client(endpoint, access_key, creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")
    ddb.list_tables()  # warms credential + policy caches

    # Delete the key.
    resp = mgmt.delete_access_key(account_id, user, access_key)
    assert resp.status_code in (200, 204)

    # Next SigV4 request rejects with UnrecognizedClientException.
    with pytest.raises(ClientError) as exc:
        ddb.list_tables()
    msg = str(exc.value)
    assert (
        "UnrecognizedClientException" in msg
        or "InvalidSignatureException" in msg
        or "security token" in msg.lower()
    ), f"unexpected error: {msg}"


def test_delete_user_drops_all_cached_state_for_user(auth_env, mgmt, account_id, user):
    """DeleteUser cascades: access keys, policies, tags all invalidated."""
    endpoint, _, _ = auth_env

    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    access_key = creds["access_key_id"]
    ddb = _ddb_client(endpoint, access_key, creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")
    ddb.list_tables()  # warm caches

    # Delete the user.
    resp = mgmt.delete_user(account_id, user)
    assert resp.status_code in (200, 204), resp.text

    # The cached credential must be invalid on the next call.
    with pytest.raises(ClientError) as exc:
        ddb.list_tables()
    msg = str(exc.value)
    assert (
        "UnrecognizedClientException" in msg
        or "InvalidSignatureException" in msg
        or "AccessDenied" in msg
    ), f"unexpected error after user delete: {msg}"


def test_create_table_visible_to_authorized_caller_immediately(
    auth_env, mgmt, account_id, user
):
    """CreateTable invalidates TableKeyInfo cache; subsequent reads see it."""
    endpoint, _, _ = auth_env
    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:*")

    # Probe a not-yet-existing table — ResourceNotFoundException seeds the
    # negative cache.
    table_name = f"cache-test-{uuid.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        ddb.describe_table(TableName=table_name)
    assert "ResourceNotFound" in str(exc.value)

    # Create the table.
    ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(ddb, table_name)

    # describe_table now finds it. The negative-cache entry must have been
    # dropped by the CreateTable invalidation hook (engine layer).
    resp = ddb.describe_table(TableName=table_name)
    assert resp["Table"]["TableName"] == table_name

    # Cleanup.
    ddb.delete_table(TableName=table_name)
    wait_for_deleted(ddb, table_name)


def test_delete_table_invalidates_table_key_info_cache(
    auth_env, mgmt, account_id, user
):
    """DeleteTable invalidates the cached TableKeyInfo entry."""
    endpoint, _, _ = auth_env
    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:*")

    table_name = f"cache-test-{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(ddb, table_name)
    ddb.describe_table(TableName=table_name)  # warm cache

    ddb.delete_table(TableName=table_name)
    wait_for_deleted(ddb, table_name)

    # Subsequent describe must see ResourceNotFoundException, not the cached
    # pre-delete description.
    with pytest.raises(ClientError) as exc:
        ddb.describe_table(TableName=table_name)
    assert "ResourceNotFound" in str(exc.value)
