# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Policy variables in `Resource` and `NotResource`, and the `Version` a policy
write accepts (issue #533).

`TestResourceVariable` gives a user tagged `team=blue` an `Allow dynamodb:*` on
`*` and a `Deny dynamodb:GetItem` on `table/${aws:PrincipalTag/team}`, written as
`Resource` or as `NotResource`, and reads the tables `blue` and `red`. The expected
answers repeat four rows measured with `iam simulate-custom-policy` on 2026-10-24
(PR #535 body table): under `Resource` the read of `blue` is denied and the read
of `red` is allowed, and under `NotResource` the reverse.

`TestVersionRefused` writes documents whose `Version` is not the string
`2012-10-17` or `2008-10-17` through the management API (user, group and role
policies, user and role permissions boundaries) and the console (user, group and
role policies). IAM refuses such a document: Access Analyzer reports
`INVALID_VERSION` and the policy simulator answers `InvalidInput` (measured
2026-10-28). The messages are ExtendDB's own.

Both classes provision builtin IAM principals through the management API, so they
run against ExtendDB only. The dual-target harness has no way to create IAM users
on the service, and the expectations come from the measurements above.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_resource_policy_variables.py -v
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient
from test_console_integration import ConsoleClient
from test_tag_request_conditions import (
    ALLOW_ALL,
    _error_code,
    _make_client,
    _require_auth_env,
    _user_with_key,
)

TEAM_VARIABLE = "${aws:PrincipalTag/team}"

# Each refused value, as JSON, and the text the refusal ends with.
REFUSED_VERSIONS = [
    pytest.param("2012-10-18", '"2012-10-18"', id="2012-10-18"),
    pytest.param("2012-10-17 ", '"2012-10-17 "', id="trailing-space"),
    pytest.param(20121017, "20121017", id="number"),
]

VERSION_RULE = "invalid Version (must be the string 2012-10-17 or 2008-10-17)"


def _document(version) -> dict:
    return {
        "Version": version,
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}],
    }


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"resvar-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class TestResourceVariable:
    """`table/${aws:PrincipalTag/team}` names the caller's team table."""

    @pytest.fixture(scope="class")
    def tables(self, auth_env, mgmt, account_id, region):
        name = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(mgmt, account_id, name)
        resp = mgmt.put_user_policy(account_id, name, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        admin = _make_client(auth_env[0], ak, sk, region)
        # The account is new, so the two names cannot collide with other tests.
        for table in ("blue", "red"):
            admin.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin, table)
        yield
        for table in ("blue", "red"):
            try:
                admin.delete_table(TableName=table)
            except ClientError:
                pass
        mgmt.delete_user(account_id, name)

    @pytest.mark.parametrize(
        "element, denied, allowed",
        [("Resource", "blue", "red"), ("NotResource", "red", "blue")],
    )
    def test_deny_on_the_team_variable(
        self, tables, auth_env, mgmt, account_id, region, element, denied, allowed
    ):
        name = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(mgmt, account_id, name)
        resp = mgmt.tag_user(account_id, name, {"team": "blue"})
        assert resp.status_code in (200, 204), resp.text
        pattern = f"arn:aws:dynamodb:{region}:{account_id}:table/" + TEAM_VARIABLE
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:GetItem", element: pattern},
            ],
        }
        resp = mgmt.put_user_policy(account_id, name, "team", policy)
        assert resp.status_code == 204, resp.text
        client = _make_client(auth_env[0], ak, sk, region)
        try:
            with pytest.raises(ClientError) as exc:
                client.get_item(TableName=denied, Key={"pk": {"S": "k"}})
            assert _error_code(exc.value) == "AccessDeniedException", exc.value.response
            resp = client.get_item(TableName=allowed, Key={"pk": {"S": "k"}})
            assert "Item" not in resp
        finally:
            mgmt.delete_user(account_id, name)


class TestVersionRefused:
    """A policy write refuses a `Version` other than the two IAM accepts."""

    @pytest.fixture()
    def user(self, mgmt, account_id):
        name = f"vu-{uuid.uuid4().hex[:8]}"
        resp = mgmt.create_user(account_id, name, "TestPass123!")
        assert resp.status_code == 201, resp.text
        yield name
        mgmt.delete_user(account_id, name)

    @pytest.fixture()
    def group(self, mgmt, account_id):
        name = f"vg-{uuid.uuid4().hex[:8]}"
        resp = mgmt.create_group(account_id, name)
        assert resp.status_code == 201, resp.text
        yield name
        mgmt.delete_group(account_id, name)

    @pytest.fixture()
    def role(self, mgmt, account_id):
        name = f"vr-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                "Action": "sts:AssumeRole",
            }],
        }
        resp = mgmt.create_role(account_id, name, trust)
        assert resp.status_code == 201, resp.text
        yield name
        mgmt.delete_role(account_id, name)

    @pytest.fixture()
    def console(self, auth_env) -> ConsoleClient:
        client = ConsoleClient(auth_env[0])
        resp = client.login(auth_env[1], auth_env[2])
        assert resp.status_code == 303, f"Login failed: {resp.status_code}"
        return client

    @pytest.mark.parametrize("version, shown", REFUSED_VERSIONS)
    @pytest.mark.parametrize("principal_type", ["user", "group", "role"])
    def test_management_api_policy_write(
        self, request, mgmt, account_id, principal_type, version, shown
    ):
        principal = request.getfixturevalue(principal_type)
        put = getattr(mgmt, f"put_{principal_type}_policy")
        listing = getattr(mgmt, f"list_{principal_type}_policies")
        resp = put(account_id, principal, "odd", _document(version))
        assert resp.status_code == 400, resp.text
        assert resp.text == f"Invalid policy document: policy parse error: {VERSION_RULE}: {shown}"
        names = [p["policy_name"] for p in listing(account_id, principal).json()]
        assert "odd" not in names
        # The same document with `Version` 2008-10-17 is stored.
        resp = put(account_id, principal, "old", _document("2008-10-17"))
        assert resp.status_code == 204, resp.text

    @pytest.mark.parametrize("version, shown", REFUSED_VERSIONS)
    @pytest.mark.parametrize("principal_type", ["user", "role"])
    def test_management_api_boundary_write(
        self, request, mgmt, account_id, principal_type, version, shown
    ):
        principal = request.getfixturevalue(principal_type)
        put = getattr(mgmt, f"set_{principal_type}_boundary")
        get = getattr(mgmt, f"get_{principal_type}_boundary")
        resp = put(account_id, principal, _document(version))
        assert resp.status_code == 400, resp.text
        assert resp.text == (
            f"Invalid permissions boundary document: policy parse error: {VERSION_RULE}: {shown}"
        )
        assert get(account_id, principal).status_code == 404
        resp = put(account_id, principal, _document("2008-10-17"))
        assert resp.status_code == 204, resp.text

    @pytest.mark.parametrize("version, shown", REFUSED_VERSIONS)
    @pytest.mark.parametrize("principal_type", ["user", "group", "role"])
    def test_console_policy_write(
        self, request, console, account_id, principal_type, version, shown
    ):
        principal = request.getfixturevalue(principal_type)
        base = f"/accounts/{account_id}/{principal_type}s/{principal}"
        resp = console.post(
            f"{base}/policies/new",
            {"policy_name": "odd", "policy_document": json.dumps(_document(version))},
        )
        # The form is shown again with the error instead of redirecting.
        assert resp.status_code == 200
        assert f"Invalid policy document: policy parse error: {VERSION_RULE}" in resp.text
        resp = console.get(base)
        assert resp.status_code == 200
        assert "odd" not in resp.text
        resp = console.post(
            f"{base}/policies/new",
            {"policy_name": "old", "policy_document": json.dumps(_document("2008-10-17"))},
        )
        assert resp.status_code == 303
