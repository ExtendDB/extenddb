# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""P90: Comprehensive auth/permissions test suite.

Covers IAM user/group/role CRUD, access key management, policy management,
permissions boundaries, authorization enforcement, cross-account isolation,
and credential validation.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run

REQ-AUTH-001, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from management_helpers import ManagementClient
from conftest import wait_for_active, wait_for_deleted

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Auth tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"test-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _make_client(endpoint_url, access_key, secret_key, region, session_token=None):
    kwargs = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _full_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
    }


def _readonly_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem", "dynamodb:Query", "dynamodb:Scan",
                "dynamodb:DescribeTable", "dynamodb:ListTables",
                "dynamodb:BatchGetItem",
            ],
            "Resource": "*",
        }],
    }


# ---------------------------------------------------------------------------
# IAM User CRUD
# ---------------------------------------------------------------------------


class TestUserCRUD:
    """IAM user create, list, delete lifecycle."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id

    def test_create_user(self):
        name = f"u-{uuid.uuid4().hex[:8]}"
        try:
            resp = self.mgmt.create_user(self.account_id, name, "Pass123!")
            assert resp.status_code == 201
        finally:
            self.mgmt.delete_user(self.account_id, name)

    def test_create_duplicate_user(self):
        name = f"u-{uuid.uuid4().hex[:8]}"
        try:
            resp = self.mgmt.create_user(self.account_id, name, "Pass123!")
            assert resp.status_code == 201
            resp2 = self.mgmt.create_user(self.account_id, name, "Pass123!")
            assert resp2.status_code == 409
        finally:
            self.mgmt.delete_user(self.account_id, name)

    def test_list_users(self):
        name = f"u-{uuid.uuid4().hex[:8]}"
        try:
            self.mgmt.create_user(self.account_id, name, "Pass123!")
            resp = self.mgmt.list_users(self.account_id)
            assert resp.status_code == 200
            names = [u["user_name"] for u in resp.json()]
            assert name in names
        finally:
            self.mgmt.delete_user(self.account_id, name)

    def test_delete_user(self):
        name = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, name, "Pass123!")
        resp = self.mgmt.delete_user(self.account_id, name)
        assert resp.status_code == 204

    def test_delete_nonexistent_user(self):
        resp = self.mgmt.delete_user(self.account_id, "nonexistent-user-xyz")
        assert resp.status_code == 404

    def test_create_user_invalid_name(self):
        resp = self.mgmt.create_user(self.account_id, "", "Pass123!")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Access Key Management
# ---------------------------------------------------------------------------


class TestAccessKeys:
    """Access key create, list, delete, import."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id

    def test_create_access_key(self):
        user = f"ak-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            resp = self.mgmt.create_access_key(self.account_id, user)
            assert resp.status_code == 201
            creds = resp.json()
            assert "access_key_id" in creds
            assert "secret_access_key" in creds
            assert len(creds["access_key_id"]) == 20
            assert len(creds["secret_access_key"]) == 40
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_list_access_keys(self):
        user = f"ak-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            self.mgmt.create_access_key(self.account_id, user)
            resp = self.mgmt.list_access_keys(self.account_id, user)
            assert resp.status_code == 200
            keys = resp.json()
            assert len(keys) >= 1
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_delete_access_key(self):
        user = f"ak-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            resp = self.mgmt.create_access_key(self.account_id, user)
            key_id = resp.json()["access_key_id"]
            resp = self.mgmt.delete_access_key(self.account_id, user, key_id)
            assert resp.status_code == 204
            # Verify key is gone.
            resp = self.mgmt.list_access_keys(self.account_id, user)
            key_ids = [k["access_key_id"] for k in resp.json()]
            assert key_id not in key_ids
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_import_access_key(self):
        user = f"ak-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            ak = "AKIA" + uuid.uuid4().hex[:16].upper()
            sk = uuid.uuid4().hex + uuid.uuid4().hex[:8]
            resp = self.mgmt.import_access_key(self.account_id, user, ak, sk)
            assert resp.status_code in (200, 201), resp.text
            resp = self.mgmt.list_access_keys(self.account_id, user)
            key_ids = [k["access_key_id"] for k in resp.json()]
            assert ak in key_ids
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_create_access_key_for_nonexistent_user(self):
        resp = self.mgmt.create_access_key(self.account_id, "no-such-user-xyz")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Group Management
# ---------------------------------------------------------------------------


class TestGroupCRUD:
    """IAM group create, list, delete, membership."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id

    def test_create_group(self):
        name = f"g-{uuid.uuid4().hex[:8]}"
        try:
            resp = self.mgmt.create_group(self.account_id, name)
            assert resp.status_code == 201
        finally:
            self.mgmt.delete_group(self.account_id, name)

    def test_create_duplicate_group(self):
        name = f"g-{uuid.uuid4().hex[:8]}"
        try:
            self.mgmt.create_group(self.account_id, name)
            resp = self.mgmt.create_group(self.account_id, name)
            assert resp.status_code == 409
        finally:
            self.mgmt.delete_group(self.account_id, name)

    def test_list_groups(self):
        name = f"g-{uuid.uuid4().hex[:8]}"
        try:
            self.mgmt.create_group(self.account_id, name)
            resp = self.mgmt.list_groups(self.account_id)
            assert resp.status_code == 200
            names = [g["group_name"] for g in resp.json()]
            assert name in names
        finally:
            self.mgmt.delete_group(self.account_id, name)

    def test_delete_group(self):
        name = f"g-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_group(self.account_id, name)
        resp = self.mgmt.delete_group(self.account_id, name)
        assert resp.status_code == 204

    def test_add_member(self):
        group = f"g-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_group(self.account_id, group)
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            resp = self.mgmt.add_group_member(self.account_id, group, user)
            assert resp.status_code in (200, 201, 204)
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_group(self.account_id, group)

    def test_remove_member(self):
        group = f"g-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_group(self.account_id, group)
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            self.mgmt.add_group_member(self.account_id, group, user)
            resp = self.mgmt.remove_group_member(self.account_id, group, user)
            assert resp.status_code == 204
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_group(self.account_id, group)

    def test_delete_nonexistent_group(self):
        resp = self.mgmt.delete_group(self.account_id, "no-such-group-xyz")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Policy Management
# ---------------------------------------------------------------------------


class TestPolicyManagement:
    """User, group, and role policy put/list/delete."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id

    def test_put_and_list_user_policy(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            resp = self.mgmt.put_user_policy(
                self.account_id, user, "test-pol", _full_policy()
            )
            assert resp.status_code == 204
            resp = self.mgmt.list_user_policies(self.account_id, user)
            assert resp.status_code == 200
            names = [p["policy_name"] for p in resp.json()]
            assert "test-pol" in names
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_delete_user_policy(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            self.mgmt.put_user_policy(self.account_id, user, "del-pol", _full_policy())
            resp = self.mgmt.delete_user_policy(self.account_id, user, "del-pol")
            assert resp.status_code == 204
            resp = self.mgmt.list_user_policies(self.account_id, user)
            names = [p["policy_name"] for p in resp.json()]
            assert "del-pol" not in names
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_put_and_list_group_policy(self):
        group = f"g-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_group(self.account_id, group)
        try:
            resp = self.mgmt.put_group_policy(
                self.account_id, group, "grp-pol", _full_policy()
            )
            assert resp.status_code == 204
            resp = self.mgmt.list_group_policies(self.account_id, group)
            assert resp.status_code == 200
            names = [p["policy_name"] for p in resp.json()]
            assert "grp-pol" in names
        finally:
            self.mgmt.delete_group(self.account_id, group)

    def test_delete_group_policy(self):
        group = f"g-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_group(self.account_id, group)
        try:
            self.mgmt.put_group_policy(self.account_id, group, "del-pol", _full_policy())
            resp = self.mgmt.delete_group_policy(self.account_id, group, "del-pol")
            assert resp.status_code == 204
        finally:
            self.mgmt.delete_group(self.account_id, group)

    def test_put_and_list_role_policy(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        try:
            resp = self.mgmt.put_role_policy(
                self.account_id, role, "role-pol", _full_policy()
            )
            assert resp.status_code == 204
            resp = self.mgmt.list_role_policies(self.account_id, role)
            assert resp.status_code == 200
            names = [p["policy_name"] for p in resp.json()]
            assert "role-pol" in names
        finally:
            self.mgmt.delete_role(self.account_id, role)

    def test_delete_role_policy(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        try:
            self.mgmt.put_role_policy(self.account_id, role, "del-pol", _full_policy())
            resp = self.mgmt.delete_role_policy(self.account_id, role, "del-pol")
            assert resp.status_code == 204
        finally:
            self.mgmt.delete_role(self.account_id, role)

    def test_put_invalid_policy_document(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            resp = self.mgmt.put_user_policy(
                self.account_id, user, "bad-pol", {"not": "a policy"}
            )
            assert resp.status_code == 400
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_replace_existing_policy(self):
        """Putting a policy with the same name replaces it."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        try:
            self.mgmt.put_user_policy(self.account_id, user, "pol", _full_policy())
            self.mgmt.put_user_policy(self.account_id, user, "pol", _readonly_policy())
            resp = self.mgmt.list_user_policies(self.account_id, user)
            policies = resp.json()
            pol = [p for p in policies if p["policy_name"] == "pol"]
            assert len(pol) == 1
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Role Management
# ---------------------------------------------------------------------------


class TestRoleCRUD:
    """IAM role create, list, delete."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id

    def _trust(self):
        return {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}],
        }

    def test_create_role(self):
        name = f"r-{uuid.uuid4().hex[:8]}"
        try:
            resp = self.mgmt.create_role(self.account_id, name, self._trust())
            assert resp.status_code == 201
        finally:
            self.mgmt.delete_role(self.account_id, name)

    def test_create_duplicate_role(self):
        name = f"r-{uuid.uuid4().hex[:8]}"
        try:
            self.mgmt.create_role(self.account_id, name, self._trust())
            resp = self.mgmt.create_role(self.account_id, name, self._trust())
            assert resp.status_code == 409
        finally:
            self.mgmt.delete_role(self.account_id, name)

    def test_list_roles(self):
        name = f"r-{uuid.uuid4().hex[:8]}"
        try:
            self.mgmt.create_role(self.account_id, name, self._trust())
            resp = self.mgmt.list_roles(self.account_id)
            assert resp.status_code == 200
            names = [r["role_name"] for r in resp.json()]
            assert name in names
        finally:
            self.mgmt.delete_role(self.account_id, name)

    def test_delete_role(self):
        name = f"r-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_role(self.account_id, name, self._trust())
        resp = self.mgmt.delete_role(self.account_id, name)
        assert resp.status_code == 204

    def test_delete_nonexistent_role(self):
        resp = self.mgmt.delete_role(self.account_id, "no-such-role-xyz")
        assert resp.status_code == 404

    def test_create_role_invalid_trust_policy(self):
        name = f"r-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_role(self.account_id, name, {"bad": "trust"})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# AssumeRole
# ---------------------------------------------------------------------------


class TestAssumeRole:
    """AssumeRole lifecycle and authorization."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_assume_role_returns_temp_creds(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": caller_arn}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        self.mgmt.put_role_policy(self.account_id, role, "full", _full_policy())

        try:
            resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "sess")
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            assert "access_key_id" in creds
            assert "secret_access_key" in creds
            assert "session_token" in creds
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)

    def test_assume_role_temp_creds_work(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": caller_arn}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        self.mgmt.put_role_policy(self.account_id, role, "full", _full_policy())

        try:
            resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "sess")
            creds = resp.json()
            client = _make_client(
                self.endpoint, creds["access_key_id"],
                creds["secret_access_key"], self.region,
                session_token=creds["session_token"],
            )
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)

    def test_assume_role_untrusted_principal_denied(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{self.account_id}:user/specific-user"},
                "Action": "sts:AssumeRole",
            }],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        try:
            # Try to assume with a different principal.
            resp = self.mgmt.assume_role(
                self.account_id, role,
                f"arn:aws:iam::{self.account_id}:user/other-user", "sess",
            )
            assert resp.status_code in (403, 400), resp.text
        finally:
            self.mgmt.delete_role(self.account_id, role)

    def test_assume_nonexistent_role(self):
        resp = self.mgmt.assume_role(
            self.account_id, "no-such-role-xyz",
            f"arn:aws:iam::{self.account_id}:user/someone", "sess",
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Authorization Enforcement
# ---------------------------------------------------------------------------


class TestAuthorizationEnforcement:
    """Verify allow/deny policy evaluation against DynamoDB operations."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _user_with_key(self, name):
        self.mgmt.create_user(self.account_id, name, "Pass123!")
        resp = self.mgmt.create_access_key(self.account_id, name)
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def test_no_policy_implicit_deny(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_all_grants_access(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_readonly_allows_get_denies_put(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        admin_ak, admin_sk = self._user_with_key(admin)
        self.mgmt.put_user_policy(self.account_id, user, "ro", _readonly_policy())
        self.mgmt.put_user_policy(self.account_id, admin, "full", _full_policy())

        admin_client = _make_client(self.endpoint, admin_ak, admin_sk, self.region)
        table = f"t-{uuid.uuid4().hex[:8]}"
        try:
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table)

            ro_client = _make_client(self.endpoint, ak, sk, self.region)
            # Read allowed.
            ro_client.get_item(TableName=table, Key={"pk": {"S": "x"}})
            # Write denied.
            with pytest.raises(ClientError) as exc:
                ro_client.put_item(TableName=table, Item={"pk": {"S": "x"}})
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            try:
                admin_client.delete_table(TableName=table)
                wait_for_deleted(admin_client, table)
            except Exception:
                pass
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin)

    def test_explicit_deny_overrides_allow(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, user, "allow", _full_policy())
        self.mgmt.put_user_policy(self.account_id, user, "deny", {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Deny", "Action": "dynamodb:ListTables", "Resource": "*"}],
        })
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_group_policy_grants_access(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        group = f"g-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.create_group(self.account_id, group)
        self.mgmt.add_group_member(self.account_id, group, user)
        self.mgmt.put_group_policy(self.account_id, group, "full", _full_policy())
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_group(self.account_id, group)

    def test_removing_group_membership_revokes_access(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        group = f"g-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.create_group(self.account_id, group)
        self.mgmt.add_group_member(self.account_id, group, user)
        self.mgmt.put_group_policy(self.account_id, group, "full", _full_policy())

        client = _make_client(self.endpoint, ak, sk, self.region)
        # Access works.
        client.list_tables()
        # Remove from group.
        self.mgmt.remove_group_member(self.account_id, group, user)
        try:
            # Access revoked (no user policy, no group membership).
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_group(self.account_id, group)

    def test_deleting_policy_revokes_access(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        client = _make_client(self.endpoint, ak, sk, self.region)
        client.list_tables()
        self.mgmt.delete_user_policy(self.account_id, user, "full")
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_action_specific_allow(self):
        """Policy allowing only CreateTable denies ListTables."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, user, "create-only", {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:CreateTable", "Resource": "*"}],
        })
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_access_denied_message_contains_arn(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            msg = exc.value.response["Error"].get("Message", "")
            assert "is not authorized to perform" in msg
            assert "dynamodb:" in msg
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Permissions Boundaries
# ---------------------------------------------------------------------------


class TestPermissionsBoundary:
    """Permissions boundary restricts effective permissions."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _user_with_key(self, name):
        self.mgmt.create_user(self.account_id, name, "Pass123!")
        resp = self.mgmt.create_access_key(self.account_id, name)
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def test_set_and_get_user_boundary(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        resp = self.mgmt.set_user_boundary(
            self.account_id, user, _readonly_policy()
        )
        assert resp.status_code in (200, 204), resp.text
        resp = self.mgmt.get_user_boundary(self.account_id, user)
        assert resp.status_code == 200
        self.mgmt.delete_user(self.account_id, user)

    def test_delete_user_boundary(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        self.mgmt.set_user_boundary(self.account_id, user, _readonly_policy())
        resp = self.mgmt.delete_user_boundary(self.account_id, user)
        assert resp.status_code == 204
        self.mgmt.delete_user(self.account_id, user)

    def test_boundary_restricts_effective_permissions(self):
        """User with full-access policy but read-only boundary can only read."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        admin_ak, admin_sk = self._user_with_key(admin)
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        self.mgmt.set_user_boundary(self.account_id, user, _readonly_policy())
        self.mgmt.put_user_policy(self.account_id, admin, "full", _full_policy())

        admin_client = _make_client(self.endpoint, admin_ak, admin_sk, self.region)
        table = f"t-{uuid.uuid4().hex[:8]}"
        try:
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table)

            client = _make_client(self.endpoint, ak, sk, self.region)
            # Read allowed (in both policy and boundary).
            client.get_item(TableName=table, Key={"pk": {"S": "x"}})
            # Write denied (not in boundary).
            with pytest.raises(ClientError) as exc:
                client.put_item(TableName=table, Item={"pk": {"S": "x"}})
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            try:
                admin_client.delete_table(TableName=table)
                wait_for_deleted(admin_client, table)
            except Exception:
                pass
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin)

    def test_set_and_get_role_boundary(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        resp = self.mgmt.set_role_boundary(self.account_id, role, _readonly_policy())
        assert resp.status_code in (200, 204), resp.text
        resp = self.mgmt.get_role_boundary(self.account_id, role)
        assert resp.status_code == 200
        self.mgmt.delete_role(self.account_id, role)


# ---------------------------------------------------------------------------
# Cross-Account Isolation
# ---------------------------------------------------------------------------


class TestCrossAccountIsolation:
    """Tables in one account are invisible to another account."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.region = region

    def test_cross_account_table_invisible(self):
        acct1 = f"{uuid.uuid4().int % 10**12:012d}"
        acct2 = f"{uuid.uuid4().int % 10**12:012d}"
        self.mgmt.create_account(acct1, f"test-{acct1}")
        self.mgmt.create_account(acct2, f"test-{acct2}")

        user1 = f"u1-{uuid.uuid4().hex[:8]}"
        user2 = f"u2-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(acct1, user1, "Pass123!")
        self.mgmt.create_user(acct2, user2, "Pass123!")
        resp1 = self.mgmt.create_access_key(acct1, user1)
        resp2 = self.mgmt.create_access_key(acct2, user2)
        creds1 = resp1.json()
        creds2 = resp2.json()
        self.mgmt.put_user_policy(acct1, user1, "full", _full_policy())
        self.mgmt.put_user_policy(acct2, user2, "full", _full_policy())

        c1 = _make_client(self.endpoint, creds1["access_key_id"], creds1["secret_access_key"], self.region)
        c2 = _make_client(self.endpoint, creds2["access_key_id"], creds2["secret_access_key"], self.region)

        table = f"iso-{uuid.uuid4().hex[:8]}"
        try:
            c1.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(c1, table)

            # Account 2 should not see account 1's table.
            resp = c2.list_tables()
            assert table not in resp["TableNames"]

            # Account 2 should get ResourceNotFoundException.
            with pytest.raises(ClientError) as exc:
                c2.describe_table(TableName=table)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            try:
                c1.delete_table(TableName=table)
                wait_for_deleted(c1, table)
            except Exception:
                pass
            self.mgmt.delete_user(acct1, user1)
            self.mgmt.delete_user(acct2, user2)
            self.mgmt.delete_account(acct1)
            self.mgmt.delete_account(acct2)


# ---------------------------------------------------------------------------
# Credential Validation
# ---------------------------------------------------------------------------


class TestCredentialValidation:
    """Invalid, missing, and malformed credentials."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_bogus_access_key_rejected(self):
        client = _make_client(
            self.endpoint, "AKIAXXXXXXXXXXXXXXXX", "x" * 40, self.region
        )
        with pytest.raises(ClientError) as exc:
            client.list_tables()
        assert exc.value.response["Error"]["Code"] in (
            "UnrecognizedClientException", "InvalidSignatureException",
        )

    def test_wrong_secret_key_rejected(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        resp = self.mgmt.create_access_key(self.account_id, user)
        creds = resp.json()
        client = _make_client(
            self.endpoint, creds["access_key_id"], "wrong" * 8, self.region
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] in (
                "UnrecognizedClientException", "InvalidSignatureException",
            )
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_deleted_access_key_rejected(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        resp = self.mgmt.create_access_key(self.account_id, user)
        creds = resp.json()
        # Verify it works first.
        client = _make_client(
            self.endpoint, creds["access_key_id"], creds["secret_access_key"], self.region
        )
        client.list_tables()
        # Delete the key.
        self.mgmt.delete_access_key(self.account_id, user, creds["access_key_id"])
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] in (
                "UnrecognizedClientException", "InvalidSignatureException",
            )
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_missing_auth_header(self):
        resp = requests.post(
            self.endpoint,
            headers={
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Target": "DynamoDB_20120810.ListTables",
            },
            json={},
            timeout=30,
            verify=not self.endpoint.startswith("https://"),
        )
        assert resp.status_code in (400, 403)
        body = resp.json()
        assert body.get("__type", "").endswith("MissingAuthenticationToken")

    def test_deleted_user_access_key_rejected(self):
        """After deleting a user, their access key no longer works."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        resp = self.mgmt.create_access_key(self.account_id, user)
        creds = resp.json()
        client = _make_client(
            self.endpoint, creds["access_key_id"], creds["secret_access_key"], self.region
        )
        client.list_tables()
        # Delete the user entirely.
        self.mgmt.delete_user(self.account_id, user)
        with pytest.raises(ClientError) as exc:
            client.list_tables()
        assert exc.value.response["Error"]["Code"] in (
            "UnrecognizedClientException", "InvalidSignatureException",
            "AccessDeniedException",
        )

    def test_multiple_access_keys_per_user(self):
        """User can have multiple access keys, each works independently."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        self.mgmt.put_user_policy(self.account_id, user, "full", _full_policy())
        resp1 = self.mgmt.create_access_key(self.account_id, user)
        resp2 = self.mgmt.create_access_key(self.account_id, user)
        creds1 = resp1.json()
        creds2 = resp2.json()
        c1 = _make_client(self.endpoint, creds1["access_key_id"], creds1["secret_access_key"], self.region)
        c2 = _make_client(self.endpoint, creds2["access_key_id"], creds2["secret_access_key"], self.region)
        try:
            c1.list_tables()
            c2.list_tables()
            # Delete first key, second still works.
            self.mgmt.delete_access_key(self.account_id, user, creds1["access_key_id"])
            c2.list_tables()
            with pytest.raises(ClientError):
                c1.list_tables()
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Spoofed Session Token
# ---------------------------------------------------------------------------


class TestSpoofedSessionToken:
    """Verify that a valid ASIA* key with a wrong session token is rejected."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_spoofed_session_token_rejected(self):
        """ASIA* credentials with an incorrect session token must fail auth."""
        role = f"r-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        self.mgmt.create_user(self.account_id, user, "Pass123!")
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": caller_arn}, "Action": "sts:AssumeRole"}],
        }
        self.mgmt.create_role(self.account_id, role, trust)
        self.mgmt.put_role_policy(self.account_id, role, "full", _full_policy())

        try:
            resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "sess")
            assert resp.status_code == 201, resp.text
            creds = resp.json()

            # Use correct access key and secret but a spoofed session token.
            spoofed_token = "SPOOFED" + "A" * 57
            client = _make_client(
                self.endpoint, creds["access_key_id"],
                creds["secret_access_key"], self.region,
                session_token=spoofed_token,
            )
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] in (
                "UnrecognizedClientException",
                "InvalidSignatureException",
            )
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)


# ---------------------------------------------------------------------------
# Deny All Service Operations (dynamodb:*)
# ---------------------------------------------------------------------------


class TestDenyAllServiceOperations:
    """Verify explicit deny with dynamodb:* blocks all operations."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _user_with_key(self, name):
        self.mgmt.create_user(self.account_id, name, "Pass123!")
        resp = self.mgmt.create_access_key(self.account_id, name)
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def test_explicit_deny_all_service_operations(self):
        """Deny with 'dynamodb:*' blocks every authorized operation even with a separate Allow."""
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, user, "allow-all", _full_policy())
        self.mgmt.put_user_policy(self.account_id, user, "deny-all", {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Deny", "Action": "dynamodb:*", "Resource": "*"}],
        })
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            with pytest.raises(ClientError) as exc:
                client.list_tables()
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

            # DescribeEndpoints needs a signed request but no IAM permission,
            # so a Deny on dynamodb:* does not block it (measured on DynamoDB
            # 2026-10-20).
            client.describe_endpoints()
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Resource-Level Permissions (Integration)
# ---------------------------------------------------------------------------


class TestResourceLevelPermissions:
    """Verify resource ARN scoping in policies works end-to-end."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _user_with_key(self, name):
        self.mgmt.create_user(self.account_id, name, "Pass123!")
        resp = self.mgmt.create_access_key(self.account_id, name)
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def test_allow_scoped_to_specific_table(self):
        """Allow policy scoped to a specific table ARN grants access only to that table."""
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = self._user_with_key(admin)
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, admin, "full", _full_policy())

        allowed_table = f"t-allowed-{uuid.uuid4().hex[:6]}"
        denied_table = f"t-denied-{uuid.uuid4().hex[:6]}"

        self.mgmt.put_user_policy(self.account_id, user, "scoped", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": f"arn:aws:dynamodb:*:*:table/{allowed_table}",
            }],
        })

        admin_client = _make_client(self.endpoint, admin_ak, admin_sk, self.region)
        user_client = _make_client(self.endpoint, ak, sk, self.region)

        try:
            for t in (allowed_table, denied_table):
                admin_client.create_table(
                    TableName=t,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                wait_for_active(admin_client, t)

            # Access to the allowed table succeeds.
            user_client.describe_table(TableName=allowed_table)

            # Access to the denied table fails.
            with pytest.raises(ClientError) as exc:
                user_client.describe_table(TableName=denied_table)
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            for t in (allowed_table, denied_table):
                try:
                    admin_client.delete_table(TableName=t)
                    wait_for_deleted(admin_client, t)
                except Exception:
                    pass
            self.mgmt.delete_user(self.account_id, admin)
            self.mgmt.delete_user(self.account_id, user)

    def test_deny_scoped_to_specific_table(self):
        """Deny policy scoped to a specific table ARN blocks only that table."""
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = self._user_with_key(admin)
        ak, sk = self._user_with_key(user)
        self.mgmt.put_user_policy(self.account_id, admin, "full", _full_policy())

        blocked_table = f"t-blocked-{uuid.uuid4().hex[:6]}"
        other_table = f"t-other-{uuid.uuid4().hex[:6]}"

        self.mgmt.put_user_policy(self.account_id, user, "allow-all", _full_policy())
        self.mgmt.put_user_policy(self.account_id, user, "deny-one", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Action": "dynamodb:*",
                "Resource": f"arn:aws:dynamodb:*:*:table/{blocked_table}",
            }],
        })

        admin_client = _make_client(self.endpoint, admin_ak, admin_sk, self.region)
        user_client = _make_client(self.endpoint, ak, sk, self.region)

        try:
            for t in (blocked_table, other_table):
                admin_client.create_table(
                    TableName=t,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                wait_for_active(admin_client, t)

            # Access to the other table succeeds.
            user_client.describe_table(TableName=other_table)

            # Access to the blocked table is denied.
            with pytest.raises(ClientError) as exc:
                user_client.describe_table(TableName=blocked_table)
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            for t in (blocked_table, other_table):
                try:
                    admin_client.delete_table(TableName=t)
                    wait_for_deleted(admin_client, t)
                except Exception:
                    pass
            self.mgmt.delete_user(self.account_id, admin)
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Backup and restore authorization (the service's resources and actions)
# ---------------------------------------------------------------------------

RESTORE_TARGET_ACTIONS = [
    "dynamodb:Scan", "dynamodb:Query", "dynamodb:UpdateItem",
    "dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem",
    "dynamodb:BatchWriteItem",
]


class TestBackupAuthorization:
    """ListBackups and RestoreTableFromBackup are authorized as the service
    does it (measured 2026-10-20): ListBackups on ``table/*/backup/*`` or
    ``table/<TableName>/backup/*``; a restore on the backup ARN under
    ``dynamodb:RestoreTableFromBackup`` and on the target table under the
    seven item actions it depends on, never under RestoreTableFromBackup.
    """

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _user_with_key(self, name):
        resp = self.mgmt.create_user(self.account_id, name, "Pass123!")
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.create_access_key(self.account_id, name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def _set_policy(self, user, statements):
        resp = self.mgmt.put_user_policy(self.account_id, user, "p", {
            "Version": "2012-10-17", "Statement": statements,
        })
        assert resp.status_code == 204, resp.text

    def _table_arn(self, name):
        return f"arn:aws:dynamodb:*:{self.account_id}:table/{name}"

    @staticmethod
    def _denied(call):
        with pytest.raises(ClientError) as exc:
            call()
        err = exc.value.response["Error"]
        assert err["Code"] == "AccessDeniedException", err
        return err.get("Message", "")

    def _assert_denied_on(self, msg, action, resource_tail):
        """The message names the first (action, resource) pair denied."""
        assert f"is not authorized to perform: dynamodb:{action} on resource: " \
            f"arn:aws:dynamodb:" in msg, msg
        # The resource ends the message, or is followed by the reason the
        # evaluator gives ("because no identity-based policy allows ...",
        # "with an explicit deny ...").
        resource = f":{self.account_id}:{resource_tail}"
        assert msg.endswith(resource) or f"{resource} " in msg, \
            (msg, resource_tail)

    @pytest.fixture
    def source_backup(self):
        """An admin, a source table with one item, and a backup of it.

        Yields ``(admin_client, table, backup_arn)``; tears them down, with
        any table named in ``self.created`` by the test.
        """
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = self._user_with_key(admin)
        self.mgmt.put_user_policy(self.account_id, admin, "full", _full_policy())
        admin_client = _make_client(self.endpoint, admin_ak, admin_sk, self.region)
        table = f"bk-src-{uuid.uuid4().hex[:8]}"
        self.created = []
        backup_arn = None
        try:
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table)
            admin_client.put_item(TableName=table, Item={"pk": {"S": "a"}})
            backup_arn = admin_client.create_backup(
                TableName=table, BackupName="authz",
            )["BackupDetails"]["BackupArn"]
            yield admin_client, table, backup_arn
        finally:
            for t in self.created:
                try:
                    wait_for_active(admin_client, t)
                except Exception:
                    pass
            if backup_arn is not None:
                try:
                    admin_client.delete_backup(BackupArn=backup_arn)
                except Exception:
                    pass
            for t in [table, *self.created]:
                try:
                    admin_client.delete_table(TableName=t)
                except Exception:
                    continue
                try:
                    wait_for_deleted(admin_client, t)
                except Exception:
                    pass
            self.mgmt.delete_user(self.account_id, admin)

    def test_list_backups_is_authorized_on_the_backup_wildcard(self, source_backup):
        """``table/<T>/backup/*`` lists T's backups and nothing else; the
        table's own ARN lists nothing (shipped builds evaluated the filtered
        list on ``table/<T>``, so it used to); ``table/*`` lists everything."""
        _, table, backup_arn = source_backup
        other = f"bk-other-{uuid.uuid4().hex[:8]}"
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            # Scoped to one table's backups.
            self._set_policy(user, [{
                "Effect": "Allow", "Action": "dynamodb:ListBackups",
                "Resource": self._table_arn(f"{table}/backup/*"),
            }])
            arns = [s["BackupArn"] for s in
                    client.list_backups(TableName=table)["BackupSummaries"]]
            assert backup_arn in arns, arns
            msg = self._denied(lambda: client.list_backups())
            self._assert_denied_on(msg, "ListBackups", "table/*/backup/*")
            msg = self._denied(lambda: client.list_backups(TableName=other))
            self._assert_denied_on(msg, "ListBackups", f"table/{other}/backup/*")

            # The table's own ARN does not cover its backups.
            self._set_policy(user, [{
                "Effect": "Allow", "Action": "dynamodb:ListBackups",
                "Resource": self._table_arn(table),
            }])
            msg = self._denied(lambda: client.list_backups(TableName=table))
            self._assert_denied_on(msg, "ListBackups", f"table/{table}/backup/*")
            self._denied(lambda: client.list_backups())

            # table/* covers the filtered and the unfiltered list.
            self._set_policy(user, [{
                "Effect": "Allow", "Action": "dynamodb:ListBackups",
                "Resource": "arn:aws:dynamodb:*:*:table/*",
            }])
            assert backup_arn in [
                s["BackupArn"] for s in client.list_backups()["BackupSummaries"]]
            assert backup_arn in [
                s["BackupArn"] for s in
                client.list_backups(TableName=table)["BackupSummaries"]]
            # A Deny on one table's backups denies only the list filtered to it.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:ListBackups",
                 "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:ListBackups",
                 "Resource": self._table_arn(f"{table}/backup/*")},
            ])
            self._denied(lambda: client.list_backups(TableName=table))
            client.list_backups(TableName=other)
            assert backup_arn in [
                s["BackupArn"] for s in client.list_backups()["BackupSummaries"]]
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_restore_needs_the_item_actions_on_the_target(self, source_backup):
        """RestoreTableFromBackup on the backup plus the seven item
        actions on the target, each required; RestoreTableFromBackup on the
        target is neither required nor evaluated."""
        admin_client, table, backup_arn = source_backup
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        client = _make_client(self.endpoint, ak, sk, self.region)
        t1, t2, t3, t4 = (f"bk-rst-{uuid.uuid4().hex[:8]}" for _ in range(4))

        def restore(target):
            return client.restore_table_from_backup(
                TargetTableName=target, BackupArn=backup_arn)

        def nothing_created(target):
            with pytest.raises(ClientError) as exc:
                admin_client.describe_table(TableName=target)
            assert exc.value.response["Error"]["Code"] == \
                "ResourceNotFoundException"

        try:
            # What used to suffice: RestoreTableFromBackup on everything.
            self._set_policy(user, [{
                "Effect": "Allow", "Action": "dynamodb:RestoreTableFromBackup",
                "Resource": "*",
            }])
            msg = self._denied(lambda: restore(t1))
            self._assert_denied_on(msg, "Scan", f"table/{t1}")
            nothing_created(t1)

            # Six of the seven item actions on the target (and the restore
            # action on it, which does not stand in): the missing one is named.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:RestoreTableFromBackup",
                 "Resource": backup_arn},
                {"Effect": "Allow",
                 "Action": [a for a in RESTORE_TARGET_ACTIONS
                            if a != "dynamodb:BatchWriteItem"]
                 + ["dynamodb:RestoreTableFromBackup"],
                 "Resource": self._table_arn(t1)},
            ])
            msg = self._denied(lambda: restore(t1))
            self._assert_denied_on(msg, "BatchWriteItem", f"table/{t1}")
            nothing_created(t1)

            # All seven on t1: the restore runs; t2 is not covered.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:RestoreTableFromBackup",
                 "Resource": backup_arn},
                {"Effect": "Allow", "Action": RESTORE_TARGET_ACTIONS,
                 "Resource": self._table_arn(t1)},
            ])
            desc = restore(t1)["TableDescription"]
            self.created.append(t1)
            assert desc["TableName"] == t1
            assert desc["TableStatus"] in ("CREATING", "ACTIVE")
            msg = self._denied(lambda: restore(t2))
            self._assert_denied_on(msg, "Scan", f"table/{t2}")
            nothing_created(t2)
            wait_for_active(admin_client, t1)
            assert admin_client.get_item(
                TableName=t1, Key={"pk": {"S": "a"}})["Item"] == {"pk": {"S": "a"}}

            # Deny RestoreTableFromBackup on the target: not evaluated there,
            # the restore runs.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:RestoreTableFromBackup",
                 "Resource": self._table_arn(t2)},
            ])
            assert restore(t2)["TableDescription"]["TableName"] == t2
            self.created.append(t2)
            wait_for_active(admin_client, t2)

            # Deny one item action on the target: refused, naming it.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:PutItem",
                 "Resource": self._table_arn(t3)},
            ])
            msg = self._denied(lambda: restore(t3))
            self._assert_denied_on(msg, "PutItem", f"table/{t3}")
            nothing_created(t3)
            # ...and the same Deny on another table does not touch t4.
            assert restore(t4)["TableDescription"]["TableName"] == t4
            self.created.append(t4)

            # The backup itself is still required under the restore action.
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:RestoreTableFromBackup",
                 "Resource": backup_arn},
            ])
            msg = self._denied(lambda: restore(t3))
            assert f"on resource: {backup_arn}" in msg, msg
            assert "dynamodb:RestoreTableFromBackup on resource:" in msg, msg
            nothing_created(t3)
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_an_existing_target_is_evaluated_with_its_tags(self, source_backup):
        """The seven item actions on ``table/<TargetTableName>`` read that
        ARN's resource tags. A target that exists is evaluated with its tags:
        a tag-conditioned Allow passes authorization and the restore then
        fails ``TableAlreadyExistsException``; without the tag it fails
        ``AccessDeniedException`` naming the first item action. A target
        that does not exist has no tags, so the same Allow denies it."""
        admin_client, table, backup_arn = source_backup
        user = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = self._user_with_key(user)
        client = _make_client(self.endpoint, ak, sk, self.region)
        existing = f"bk-tgt-{uuid.uuid4().hex[:8]}"
        missing = f"bk-tgt-{uuid.uuid4().hex[:8]}"
        self.created.append(existing)
        admin_client.create_table(
            TableName=existing,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(admin_client, existing)
        existing_arn = admin_client.describe_table(
            TableName=existing)["Table"]["TableArn"]

        def restore(target):
            return client.restore_table_from_backup(
                TargetTableName=target, BackupArn=backup_arn)

        try:
            self._set_policy(user, [
                {"Effect": "Allow", "Action": "dynamodb:RestoreTableFromBackup",
                 "Resource": backup_arn},
                {"Effect": "Allow", "Action": RESTORE_TARGET_ACTIONS,
                 "Resource": "*",
                 "Condition": {"StringEquals": {
                     "aws:ResourceTag/classification": "restorable"}}},
            ])
            # The existing target has no such tag: denied on the first item
            # action, and the table is untouched.
            msg = self._denied(lambda: restore(existing))
            self._assert_denied_on(msg, "Scan", f"table/{existing}")
            # Tagged: authorization passes on its tags and the restore fails
            # because the name exists.
            admin_client.tag_resource(
                ResourceArn=existing_arn,
                Tags=[{"Key": "classification", "Value": "restorable"}],
            )
            with pytest.raises(ClientError) as exc:
                restore(existing)
            assert exc.value.response["Error"]["Code"] == \
                "TableAlreadyExistsException", exc.value.response["Error"]
            assert admin_client.describe_table(
                TableName=existing)["Table"]["TableStatus"] == "ACTIVE"
            # A name that does not exist has no tags: denied, nothing created.
            msg = self._denied(lambda: restore(missing))
            self._assert_denied_on(msg, "Scan", f"table/{missing}")
            with pytest.raises(ClientError) as exc:
                admin_client.describe_table(TableName=missing)
            assert exc.value.response["Error"]["Code"] == \
                "ResourceNotFoundException"
            # The tag is read on each request: once removed, the existing
            # target is denied again.
            admin_client.untag_resource(
                ResourceArn=existing_arn, TagKeys=["classification"])
            msg = self._denied(lambda: restore(existing))
            self._assert_denied_on(msg, "Scan", f"table/{existing}")
        finally:
            self.mgmt.delete_user(self.account_id, user)
