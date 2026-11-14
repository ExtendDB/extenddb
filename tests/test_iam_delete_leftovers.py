# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A deleted principal or account leaves nothing for one created later with
its name or id (#693, #694).

Inline policies and permissions boundaries are stored by principal name, and
on MongoDB and Cassandra nothing links any IAM record to its principal or
account. Each test gives a principal or an account everything it can own,
deletes it, checks that its credentials stop working, creates it again with
the same name or id, and checks that the new one starts empty and is refused.

The last test checks that a policy or boundary cannot be put on a principal
that does not exist, as IAM answers ``NoSuchEntity``.

Prerequisites: a running extenddb with ``auth.provider = "builtin"`` on
EXTENDDB_TEST_ENDPOINT, and admin credentials in EXTENDDB_ADMIN_USER /
EXTENDDB_ADMIN_PASSWORD. The tests use the management API, which Amazon
DynamoDB does not have.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from management_helpers import ManagementClient


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: these tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER and EXTENDDB_ADMIN_PASSWORD."
        )
    return endpoint, admin_user, admin_pass


@pytest.fixture(scope="module")
def endpoint() -> str:
    return _require_auth_env()[0]


@pytest.fixture(scope="module")
def mgmt() -> ManagementClient:
    endpoint, admin_user, admin_pass = _require_auth_env()
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture
def account_id(mgmt):
    """A new account, deleted on teardown with whatever is left in it."""
    acct = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct, f"leftovers-{acct}")
    assert resp.status_code == 201, resp.text
    yield acct
    for user in _names(mgmt.list_users(acct), "user_name"):
        mgmt.delete_user(acct, user)
    for group in _names(mgmt.list_groups(acct), "group_name"):
        mgmt.delete_group(acct, group)
    for role in _names(mgmt.list_roles(acct), "role_name"):
        mgmt.delete_role(acct, role)
    mgmt.delete_account(acct)


def _names(resp: requests.Response, field: str) -> list[str]:
    return [entry[field] for entry in resp.json()] if resp.status_code == 200 else []


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _allow(*actions: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": list(actions), "Resource": "*"}],
    }


def _ddb(endpoint: str, creds: dict) -> Any:
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        aws_session_token=creds.get("session_token"),
        config=BotoConfig(
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            signature_version="v4",
            retries={"max_attempts": 0, "mode": "standard"},
        ),
        verify=False,  # local TLS uses a self-signed cert
    )


def _error_code(call) -> str:
    """The error code `call` raises, or ``OK`` if it succeeds."""
    try:
        call()
    except ClientError as e:
        return e.response["Error"]["Code"]
    return "OK"


def _unknown_credential(call) -> bool:
    """Whether `call` is refused because its credential is unknown."""
    return _error_code(call) in ("UnrecognizedClientException", "InvalidSignatureException")


def _access_key(mgmt: ManagementClient, account_id: str, user: str) -> dict:
    resp = mgmt.create_access_key(account_id, user)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _trust(account_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


def _session(mgmt: ManagementClient, account_id: str, role: str) -> dict:
    resp = mgmt.assume_role(account_id, role, f"arn:aws:iam::{account_id}:root", _unique("s"))
    assert resp.status_code == 201, resp.text
    return resp.json()


def _tags(mgmt: ManagementClient, account_id: str, kind: str, name: str) -> list:
    resp = requests.get(
        f"{mgmt.base_url}/accounts/{account_id}/{kind}s/{name}/tags",
        auth=mgmt.admin_auth,
        timeout=mgmt.timeout,
        verify=mgmt.verify,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _policy_names(resp: requests.Response) -> list[str]:
    assert resp.status_code == 200, resp.text
    return sorted(p["policy_name"] for p in resp.json())


def _ok(resp: requests.Response, *codes: int) -> None:
    assert resp.status_code in (codes or (200, 201, 204)), resp.text


class TestDeletedPrincipalLeavesNothing:
    def test_role(self, endpoint, mgmt, account_id):
        role = _unique("role")
        _ok(mgmt.create_role(account_id, role, _trust(account_id)), 201)
        _ok(mgmt.put_role_policy(account_id, role, "old", _allow("dynamodb:ListTables")))
        _ok(mgmt.set_role_boundary(account_id, role, _allow("dynamodb:*")))
        _ok(mgmt.tag_role(account_id, role, {"team": "old"}))
        old_session = _ddb(endpoint, _session(mgmt, account_id, role))
        assert _error_code(old_session.list_tables) == "OK"

        _ok(mgmt.delete_role(account_id, role), 204)
        assert _unknown_credential(old_session.list_tables)

        _ok(mgmt.create_role(account_id, role, _trust(account_id)), 201)
        assert _policy_names(mgmt.list_role_policies(account_id, role)) == []
        assert mgmt.get_role_boundary(account_id, role).status_code == 404
        assert _tags(mgmt, account_id, "role", role) == []
        assert _unknown_credential(old_session.list_tables)
        new_session = _ddb(endpoint, _session(mgmt, account_id, role))
        assert _error_code(new_session.list_tables) == "AccessDeniedException"

    def test_user(self, endpoint, mgmt, account_id):
        user, group = _unique("user"), _unique("group")
        _ok(mgmt.create_user(account_id, user), 201)
        _ok(mgmt.put_user_policy(account_id, user, "old", _allow("dynamodb:ListTables")))
        _ok(mgmt.set_user_boundary(account_id, user, _allow("dynamodb:*")))
        _ok(mgmt.tag_user(account_id, user, {"team": "old"}))
        _ok(mgmt.create_group(account_id, group), 201)
        _ok(mgmt.put_group_policy(account_id, group, "g", _allow("dynamodb:DescribeLimits")))
        _ok(mgmt.add_group_member(account_id, group, user))
        old_key = _ddb(endpoint, _access_key(mgmt, account_id, user))
        assert _error_code(old_key.list_tables) == "OK"
        assert _error_code(old_key.describe_limits) == "OK"

        _ok(mgmt.delete_user(account_id, user), 204)
        assert _unknown_credential(old_key.list_tables)

        _ok(mgmt.create_user(account_id, user), 201)
        # Every new user gets the self-service policy, and nothing else.
        assert _policy_names(mgmt.list_user_policies(account_id, user)) == ["SelfServicePolicy"]
        assert mgmt.get_user_boundary(account_id, user).status_code == 404
        assert _tags(mgmt, account_id, "user", user) == []
        resp = mgmt.list_access_keys(account_id, user)
        assert resp.status_code == 200 and resp.json() == [], resp.text
        assert _unknown_credential(old_key.list_tables)
        new_key = _ddb(endpoint, _access_key(mgmt, account_id, user))
        assert _error_code(new_key.list_tables) == "AccessDeniedException"
        # The old user's membership is gone with it: the group's policy no
        # longer applies.
        assert _error_code(new_key.describe_limits) == "AccessDeniedException"

    def test_group(self, endpoint, mgmt, account_id):
        group, member = _unique("group"), _unique("member")
        _ok(mgmt.create_group(account_id, group), 201)
        _ok(mgmt.put_group_policy(account_id, group, "old", _allow("dynamodb:ListTables")))
        _ok(mgmt.create_user(account_id, member), 201)
        _ok(mgmt.add_group_member(account_id, group, member))
        key = _ddb(endpoint, _access_key(mgmt, account_id, member))
        assert _error_code(key.list_tables) == "OK"

        _ok(mgmt.delete_group(account_id, group), 204)
        assert _error_code(key.list_tables) == "AccessDeniedException"

        _ok(mgmt.create_group(account_id, group), 201)
        assert _policy_names(mgmt.list_group_policies(account_id, group)) == []
        # The member is not a member of the new group, and has no policy
        # that allows ListTables from it.
        assert _error_code(key.list_tables) == "AccessDeniedException"
        _ok(mgmt.put_group_policy(account_id, group, "new", _allow("dynamodb:ListTables")))
        assert _error_code(key.list_tables) == "AccessDeniedException"


class TestDeletedAccountLeavesNothing:
    def test_account(self, endpoint, mgmt):
        account_id = f"{uuid.uuid4().int % 10**12:012d}"
        user, role, password = _unique("user"), _unique("role"), uuid.uuid4().hex
        _ok(mgmt.create_account(account_id, f"leftovers-{account_id}"), 201)
        try:
            _ok(mgmt.create_user(account_id, user, password), 201)
            _ok(mgmt.put_user_policy(account_id, user, "old", _allow("dynamodb:ListTables")))
            key = _ddb(endpoint, _access_key(mgmt, account_id, user))
            _ok(mgmt.create_role(account_id, role, _trust(account_id)), 201)
            _ok(mgmt.put_role_policy(account_id, role, "old", _allow("dynamodb:ListTables")))
            session = _ddb(endpoint, _session(mgmt, account_id, role))
            group = _unique("group")
            _ok(mgmt.create_group(account_id, group), 201)
            assert _error_code(key.list_tables) == "OK"
            assert _error_code(session.list_tables) == "OK"
            self_service = requests.get(
                f"{mgmt.base_url}/accounts/{account_id}/users/{user}/access-keys",
                auth=(f"{account_id}/{user}", password),
                timeout=mgmt.timeout,
                verify=mgmt.verify,
            )
            assert self_service.status_code == 200, self_service.text

            _ok(mgmt.delete_account(account_id), 204)

            assert _unknown_credential(key.list_tables)
            assert _unknown_credential(session.list_tables)
            self_service = requests.get(
                f"{mgmt.base_url}/accounts/{account_id}/users/{user}/access-keys",
                auth=(f"{account_id}/{user}", password),
                timeout=mgmt.timeout,
                verify=mgmt.verify,
            )
            assert self_service.status_code == 401, self_service.text
            login = requests.post(
                f"{endpoint.rstrip('/')}/console/login",
                data={"username": f"{account_id}/{user}", "password": password},
                allow_redirects=False,
                timeout=30,
                verify=mgmt.verify,
            )
            # A successful login answers 303 to the dashboard.
            assert login.status_code != 303, login.headers

            _ok(mgmt.create_account(account_id, f"leftovers-again-{account_id}"), 201)
            assert _names(mgmt.list_users(account_id), "user_name") == []
            assert _names(mgmt.list_groups(account_id), "group_name") == []
            assert _names(mgmt.list_roles(account_id), "role_name") == []
            assert _unknown_credential(key.list_tables)
            assert _unknown_credential(session.list_tables)
            # A principal created again in the new account starts empty too.
            _ok(mgmt.create_role(account_id, role, _trust(account_id)), 201)
            assert _policy_names(mgmt.list_role_policies(account_id, role)) == []
            _ok(mgmt.create_user(account_id, user), 201)
            assert _policy_names(mgmt.list_user_policies(account_id, user)) == [
                "SelfServicePolicy"
            ]
        finally:
            mgmt.delete_user(account_id, user)
            mgmt.delete_role(account_id, role)
            mgmt.delete_account(account_id)


class TestAttachToMissingPrincipal:
    @pytest.mark.parametrize("kind", ["user", "group", "role"])
    def test_inline_policy_is_refused(self, mgmt, account_id, kind):
        name = _unique(kind)
        put = getattr(mgmt, f"put_{kind}_policy")
        resp = put(account_id, name, "early", _allow("dynamodb:ListTables"))
        assert resp.status_code == 404, resp.text
        assert resp.text == f"IAM {kind} not found"
        resp = getattr(mgmt, f"list_{kind}_policies")(account_id, name)
        assert resp.status_code != 200 or resp.json() == [], resp.text

    @pytest.mark.parametrize("kind", ["user", "role"])
    def test_boundary_is_refused(self, mgmt, account_id, kind):
        name = _unique(kind)
        resp = getattr(mgmt, f"set_{kind}_boundary")(account_id, name, _allow("dynamodb:*"))
        assert resp.status_code == 404, resp.text
        assert resp.text == f"IAM {kind} not found"
        assert getattr(mgmt, f"get_{kind}_boundary")(account_id, name).status_code == 404
