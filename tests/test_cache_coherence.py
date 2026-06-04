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


# ---------------------------------------------------------------------------
# Manual cache invalidation API (POST /management/cache/invalidate)
# Covered by docs/design/12-auth-authz-cache.md §6.1.
# ---------------------------------------------------------------------------


def _metrics_counter(mgmt, subcache_path: list[str], counter: str) -> int:
    """Read the auth-cache-metrics endpoint and return one named counter
    on a specific subcache.

    `subcache_path` example: `["authz", "user_policies"]` or `["credential"]`.
    `counter` is the JSON field name (e.g. `invalidations`, `misses`, `hits`).
    """
    import requests
    url = f"{mgmt.base_url}/auth-cache-metrics"
    resp = requests.get(url, auth=mgmt.admin_auth,
                        timeout=mgmt.timeout, verify=mgmt.verify)
    assert resp.status_code == 200, resp.text
    node = resp.json()
    for k in subcache_path:
        node = node[k]
    return int(node[counter])


def _metrics_invalidations(mgmt, subcache_path: list[str]) -> int:
    return _metrics_counter(mgmt, subcache_path, "invalidations")


def test_manual_invalidate_user_forces_refetch(auth_env, mgmt, account_id, user):
    """scope=user actually drops the cached entry: the next request
    misses the cache (forcing a re-fetch) instead of getting served
    from the warm slot."""
    endpoint, _, _ = auth_env
    # Prime the per-user policy cache so we have a warm hit-able entry.
    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")
    ddb.list_tables()  # populates user_policies cache
    ddb.list_tables()  # second call should be a hit (proves warm)

    invalidations_before = _metrics_invalidations(mgmt, ["authz", "user_policies"])
    misses_before = _metrics_counter(mgmt, ["authz", "user_policies"], "misses")

    resp = mgmt.invalidate_cache(
        "user", {"account_id": account_id, "user_name": user}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["scope"] == "user"
    # Composite scope reports each subcache it touched.
    expected = {
        "user_policies",
        "user_group_policies",
        "user_boundary",
        "user_tags",
        "principal_credentials",
    }
    assert expected.issubset(set(body["invalidated"]))

    invalidations_after = _metrics_invalidations(mgmt, ["authz", "user_policies"])
    assert invalidations_after > invalidations_before, (
        f"user_policies invalidations: {invalidations_before} → {invalidations_after}"
    )

    # Real proof of behavior: the next request must miss the cache and
    # re-load from the inner store. If invalidation were broken, the
    # warm entry would still be served and `misses` wouldn't move.
    ddb.list_tables()
    misses_after = _metrics_counter(mgmt, ["authz", "user_policies"], "misses")
    assert misses_after > misses_before, (
        f"expected user_policies miss after invalidation; "
        f"misses: {misses_before} → {misses_after}"
    )


def test_manual_invalidate_account_does_not_break_subsequent_traffic(
    auth_env, mgmt, account_id, user
):
    """scope=account sweeps every cache for the account; the user must still
    work (re-fetched from the catalog) on the next request."""
    endpoint, _, _ = auth_env
    resp = mgmt.create_access_key(account_id, user)
    creds = resp.json()
    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])
    _put_allow_policy(mgmt, account_id, user, "dynamodb:ListTables")
    ddb.list_tables()  # warm

    resp = mgmt.invalidate_cache("account", {"account_id": account_id})
    assert resp.status_code == 200, resp.text

    # Cache is cold but the IAM state is unchanged → request succeeds.
    ddb.list_tables()


def test_manual_invalidate_all_requires_confirmation(auth_env, mgmt):
    """scope=all without confirm:true is rejected with 400."""
    resp = mgmt.invalidate_cache("all", {})
    assert resp.status_code == 400, resp.text
    assert "confirm" in resp.text.lower()


def test_manual_invalidate_all_with_confirmation(auth_env, mgmt):
    """scope=all with confirm:true sweeps every cache and returns 200."""
    resp = mgmt.invalidate_cache("all", {"confirm": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["scope"] == "all"
    assert {"authz", "table_key_info", "credentials"} == set(body["invalidated"])


def test_manual_invalidate_missing_selector_returns_400(auth_env, mgmt, account_id):
    """scope=user without user_name is a 400, not a silent no-op."""
    resp = mgmt.invalidate_cache("user", {"account_id": account_id})
    assert resp.status_code == 400, resp.text
    assert "user_name" in resp.text


def test_manual_invalidate_requires_admin_auth(auth_env, mgmt, account_id, user):
    """IAM users cannot invalidate cache; missing/wrong creds → 401."""
    import requests
    # IAM user with valid console password but not admin → 403
    pw = uuid.uuid4().hex
    iam_user = f"cache-iam-{uuid.uuid4().hex[:8]}"
    mgmt.create_user(account_id, iam_user, password=pw)
    try:
        resp = mgmt.invalidate_cache(
            "all",
            {"confirm": True},
            auth=(f"{account_id}/{iam_user}", pw),
        )
        assert resp.status_code == 403, resp.text
    finally:
        mgmt.delete_user(account_id, iam_user)

    # No credentials at all → 401.
    resp = requests.post(
        f"{mgmt.base_url}/cache/invalidate",
        json={"scope": "all", "selectors": {"confirm": True}},
        timeout=mgmt.timeout, verify=mgmt.verify,
    )
    assert resp.status_code == 401, resp.text


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
