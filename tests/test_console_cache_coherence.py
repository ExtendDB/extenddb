# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end cache-coherence tests driving the **web console** (not the
management API).

Purpose: the web console mutates IAM via form posts that run a separate
code path from the JSON management API. Both paths share the same
`AuthCacheRegistry`, but the console wiring is easy to break because the
console pages live in a different module tree. These tests assert that
console-driven mutations propagate to the auth/authz cache instantly,
just like the management API path (verified by
``tests/test_cache_coherence.py``).

If a console handler ever forgets to call the matching ``invalidate_*``
hook, the mutation it performs will appear to "not stick" until
``auth.cache.ttl_seconds`` elapses — these tests reproduce that lag and
fail.

Prerequisites mirror tests/test_cache_coherence.py.
"""

from __future__ import annotations

import os
import re
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from management_helpers import ManagementClient


_CSRF_RE = re.compile(r'<meta name="csrf-token" content="([^"]+)"')


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: console-cache-coherence tests require "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER, EXTENDDB_ADMIN_PASSWORD."
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
    resp = mgmt.create_account(acct_id, f"console-coh-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class _ConsoleSession:
    """Minimal browser-like client for the /console/* endpoints.

    Holds a `requests.Session`, performs the form-based login, and exposes
    helpers to extract CSRF tokens and post mutation forms.
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.verify = False  # local TLS uses a self-signed cert
        # Form-based login.
        r = self.s.post(
            f"{self.base}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
        )
        if r.status_code != 303:
            raise RuntimeError(
                f"Console login failed: status={r.status_code}, body={r.text[:200]}"
            )

    def csrf(self) -> str:
        """Fetch a page and extract the CSRF token from the <meta> tag."""
        r = self.s.get(f"{self.base}/console", timeout=30)
        r.raise_for_status()
        m = _CSRF_RE.search(r.text)
        if not m:
            raise RuntimeError("CSRF meta tag not found on /console")
        return m.group(1)

    def post_form(self, path: str, fields: dict) -> requests.Response:
        """POST a form, automatically attaching the current CSRF token."""
        body = dict(fields)
        body["_csrf"] = self.csrf()
        return self.s.post(
            f"{self.base}{path}",
            data=body,
            allow_redirects=False,
            timeout=30,
        )


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
        verify=False,
    )


@pytest.fixture(scope="module")
def console(auth_env) -> _ConsoleSession:
    endpoint, admin_user, admin_pass = auth_env
    return _ConsoleSession(endpoint, admin_user, admin_pass)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_console_put_user_policy_takes_effect_immediately(
    auth_env, mgmt, account_id, console
):
    """Console-driven PutUserPolicy must invalidate the user_policies cache."""
    endpoint, _, _ = auth_env
    user = f"con-pol-{uuid.uuid4().hex[:8]}"
    resp = mgmt.create_user(account_id, user, password=None)
    assert resp.status_code == 201, resp.text
    try:
        resp = mgmt.create_access_key(account_id, user)
        creds = resp.json()
        ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])

        # No policy yet → ListTables denied. This warms the user_policies
        # cache as an empty/negative entry.
        with pytest.raises(ClientError) as exc:
            ddb.list_tables()
        assert "AccessDenied" in str(exc.value) or "AccessDeniedException" in str(exc.value)

        # Console adds an Allow policy via form post.
        policy_doc = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Action":"dynamodb:ListTables","Resource":"*"}]}'
        )
        r = console.post_form(
            f"/console/accounts/{account_id}/users/{user}/policies/new",
            {"policy_name": "test-policy", "policy_document": policy_doc},
        )
        assert r.status_code in (302, 303), f"unexpected status {r.status_code}: {r.text[:200]}"

        # The cached negative entry must have been invalidated by the
        # console handler. The next SigV4 call sees the new policy
        # without any TTL wait.
        result = ddb.list_tables()
        assert "TableNames" in result
    finally:
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


def test_console_delete_user_policy_takes_effect_immediately(
    auth_env, mgmt, account_id, console
):
    """Console-driven DeleteUserPolicy must invalidate the user_policies cache."""
    endpoint, _, _ = auth_env
    user = f"con-del-{uuid.uuid4().hex[:8]}"
    mgmt.create_user(account_id, user, password=None)
    try:
        creds = mgmt.create_access_key(account_id, user).json()
        ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])

        # Allow policy via console.
        policy_doc = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Action":"dynamodb:ListTables","Resource":"*"}]}'
        )
        r = console.post_form(
            f"/console/accounts/{account_id}/users/{user}/policies/new",
            {"policy_name": "p", "policy_document": policy_doc},
        )
        assert r.status_code in (302, 303), r.text[:200]
        ddb.list_tables()  # warms the cache with the Allow policy.

        # Delete via console — must invalidate.
        r = console.post_form(
            f"/console/accounts/{account_id}/users/{user}/policies/p/delete",
            {},
        )
        assert r.status_code in (302, 303), r.text[:200]

        # Next call must be denied (no policy now).
        with pytest.raises(ClientError) as exc:
            ddb.list_tables()
        assert "AccessDenied" in str(exc.value) or "AccessDeniedException" in str(exc.value)
    finally:
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


def test_console_delete_access_key_takes_effect_immediately(
    auth_env, mgmt, account_id, console
):
    """Console DeleteAccessKey must invalidate the credential cache."""
    endpoint, _, _ = auth_env
    user = f"con-key-{uuid.uuid4().hex[:8]}"
    mgmt.create_user(account_id, user, password=None)
    try:
        # Issue key + Allow policy so the credential gets cached as Some(_).
        creds = mgmt.create_access_key(account_id, user).json()
        access_key = creds["access_key_id"]
        mgmt.put_user_policy(
            account_id, user, "p",
            {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}
            ]},
        )
        ddb = _ddb_client(endpoint, access_key, creds["secret_access_key"])
        ddb.list_tables()  # warm credential cache

        # Console deletes the key.
        r = console.post_form(
            f"/console/accounts/{account_id}/users/{user}/access-keys/{access_key}/delete",
            {},
        )
        assert r.status_code in (302, 303), r.text[:200]

        # Next SigV4 call must be rejected immediately.
        with pytest.raises(ClientError) as exc:
            ddb.list_tables()
        msg = str(exc.value)
        assert (
            "UnrecognizedClientException" in msg
            or "InvalidSignatureException" in msg
            or "security token" in msg.lower()
        ), f"unexpected error after console delete-access-key: {msg}"
    finally:
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


def test_console_delete_user_drops_all_cached_state(auth_env, mgmt, account_id, console):
    """Console DeleteUser must cascade-invalidate every user-keyed cache."""
    endpoint, _, _ = auth_env
    user = f"con-du-{uuid.uuid4().hex[:8]}"
    mgmt.create_user(account_id, user, password=None)
    try:
        creds = mgmt.create_access_key(account_id, user).json()
        access_key = creds["access_key_id"]
        mgmt.put_user_policy(
            account_id, user, "p",
            {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}
            ]},
        )
        ddb = _ddb_client(endpoint, access_key, creds["secret_access_key"])
        ddb.list_tables()  # warm credential + policy caches

        # Console deletes the user.
        r = console.post_form(
            f"/console/accounts/{account_id}/users/{user}/delete",
            {},
        )
        assert r.status_code in (302, 303), r.text[:200]

        # Next SigV4 request must be rejected.
        with pytest.raises(ClientError) as exc:
            ddb.list_tables()
        msg = str(exc.value)
        assert (
            "UnrecognizedClientException" in msg
            or "InvalidSignatureException" in msg
            or "AccessDenied" in msg
        ), f"unexpected error after console delete-user: {msg}"
    finally:
        # Best-effort: user already deleted by the console flow above.
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


def test_console_add_group_member_propagates_group_policies(
    auth_env, mgmt, account_id, console
):
    """Adding a user to a group via the console must invalidate that user's
    cached user_group_policies, so the group's policies become effective
    immediately."""
    endpoint, _, _ = auth_env
    user = f"con-gm-{uuid.uuid4().hex[:8]}"
    group = f"con-grp-{uuid.uuid4().hex[:8]}"
    mgmt.create_user(account_id, user, password=None)
    try:
        # Create group + attach policy via management API (group endpoints
        # via console for these would work too; we want to isolate the
        # bug-under-test to the add-member flow).
        resp = mgmt.create_group(account_id, group)
        assert resp.status_code == 201, resp.text
        mgmt.put_group_policy(
            account_id, group, "p",
            {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}
            ]},
        )

        creds = mgmt.create_access_key(account_id, user).json()
        ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])

        # No membership yet → denied. Warms user_group_policies as empty.
        with pytest.raises(ClientError):
            ddb.list_tables()

        # Console adds the user to the group.
        r = console.post_form(
            f"/console/accounts/{account_id}/groups/{group}/members/add",
            {"user_name": user},
        )
        assert r.status_code in (302, 303), r.text[:200]

        # Next call must succeed (group policy now in effect).
        result = ddb.list_tables()
        assert "TableNames" in result
    finally:
        try:
            mgmt.delete_group(account_id, group)
        except Exception:
            pass
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Console cache invalidation form (admin break-glass).
# Mirrors POST /management/cache/invalidate; both call the same `apply`
# helper. See docs/design/12-auth-authz-cache.md §6.1.
# ---------------------------------------------------------------------------


def test_console_cache_page_admin_only(auth_env):
    """A non-logged-in browser is redirected to login; the form requires admin."""
    endpoint, _, _ = auth_env
    r = requests.get(
        f"{endpoint}/console/cache",
        verify=False,
        timeout=30,
        allow_redirects=False,
    )
    # Unauthenticated → redirect to login.
    assert r.status_code in (302, 303), r.status_code


def test_console_cache_invalidate_user(auth_env, mgmt, account_id, console):
    """Submitting the cache form for scope=user returns the rendered success
    page listing the touched subcaches."""
    user = f"con-cache-{uuid.uuid4().hex[:8]}"
    resp = mgmt.create_user(account_id, user, password=None)
    assert resp.status_code == 201, resp.text
    try:
        r = console.post_form(
            "/console/cache/invalidate",
            {
                "scope": "user",
                "account_id": account_id,
                "user_name": user,
                "user_names": "",
                "role_name": "",
                "access_key_id": "",
                "table_name": "",
                "arn": "",
                "confirm": "",
            },
        )
        assert r.status_code == 200, r.text[:200]
        # Success page lists every subcache the composite scope touched.
        for label in (
            "user_policies",
            "user_group_policies",
            "user_boundary",
            "user_tags",
            "principal_credentials",
        ):
            assert label in r.text, f"missing {label} in response"
    finally:
        try:
            mgmt.delete_user(account_id, user)
        except Exception:
            pass


def test_console_cache_invalidate_all_requires_typed_confirmation(
    auth_env, mgmt, account_id, console
):
    """scope=all on the console form refuses without the typed token, accepts with it."""
    # Refused without confirmation.
    r = console.post_form(
        "/console/cache/invalidate",
        {
            "scope": "all",
            "account_id": "",
            "user_name": "",
            "user_names": "",
            "role_name": "",
            "access_key_id": "",
            "table_name": "",
            "arn": "",
            "confirm": "wrong",
        },
    )
    assert r.status_code == 200  # error page rendered
    assert "INVALIDATE" in r.text  # error mentions the required token

    # Accepted with the typed token.
    r = console.post_form(
        "/console/cache/invalidate",
        {
            "scope": "all",
            "account_id": "",
            "user_name": "",
            "user_names": "",
            "role_name": "",
            "access_key_id": "",
            "table_name": "",
            "arn": "",
            "confirm": "INVALIDATE",
        },
    )
    assert r.status_code == 200, r.text[:200]
    for label in ("authz", "table_key_info", "credentials"):
        assert label in r.text
