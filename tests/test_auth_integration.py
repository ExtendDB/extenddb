# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 12i: SDK integration tests for extenddb auth.

Verifies that real AWS SDKs (boto3) work against extenddb with builtin auth.
Tests IBAC, RBAC patterns, error paths, and Mode 1 backward compat.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run (encryption key + admin user exist)

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_auth_integration.py -v

REQ-TEST-001, REQ-AUTH-001, REQ-AUTH-002
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
from conftest import wait_for_active, wait_for_deleted
# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _require_auth_env() -> tuple[str, str, str]:
    """Skip tests if auth environment is not configured."""
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Auth integration tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass
@pytest.fixture(scope="module")
def auth_env():
    """Return (endpoint_url, admin_user, admin_password)."""
    return _require_auth_env()
@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)
@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    """Create a test account for the module, clean up after."""
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"test-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)
@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
def _make_dynamodb_client(endpoint_url: str, access_key: str, secret_key: str,
                          region: str, session_token: str | None = None) -> Any:
    """Create a boto3 DynamoDB client with explicit credentials."""
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
def _full_access_policy() -> dict:
    """Policy granting full DynamoDB access."""
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "dynamodb:*",
            "Resource": "*",
        }],
    }
def _readonly_policy() -> dict:
    """Policy granting read-only DynamoDB access."""
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:DescribeTable",
                "dynamodb:ListTables",
            ],
            "Resource": "*",
        }],
    }
# ---------------------------------------------------------------------------
# IBAC Tests — Identity-Based Access Control
# ---------------------------------------------------------------------------

class TestIBAC:
    """Test identity-based access control with user policies."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _create_user_with_key(self, user_name, password="TestPass123!"):
        """Create an IAM user and access key, return (access_key, secret_key)."""
        resp = self.mgmt.create_user(self.account_id, user_name, password)
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.create_access_key(self.account_id, user_name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        return creds["access_key_id"], creds["secret_access_key"]

    def test_full_access_user_can_crud(self):
        """User with full DynamoDB access can create, put, get, delete."""
        user = f"full-{uuid.uuid4().hex[:8]}"
        ak, sk = self._create_user_with_key(user)
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "full-access", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        client = _make_dynamodb_client(self.endpoint, ak, sk, self.region)
        table_name = f"auth-test-{uuid.uuid4().hex[:8]}"

        try:
            # CreateTable
            client.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(client, table_name)
            # PutItem
            client.put_item(
                TableName=table_name,
                Item={"pk": {"S": "key1"}, "data": {"S": "value1"}},
            )
            # GetItem
            resp = client.get_item(
                TableName=table_name, Key={"pk": {"S": "key1"}}
            )
            assert resp["Item"]["data"]["S"] == "value1"
            # DeleteItem
            client.delete_item(
                TableName=table_name, Key={"pk": {"S": "key1"}}
            )
        finally:
            try:
                client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(client, table_name)
            self.mgmt.delete_user(self.account_id, user)

    def test_readonly_user_denied_write(self):
        """User with read-only policy is denied PutItem."""
        user = f"ro-{uuid.uuid4().hex[:8]}"
        ak, sk = self._create_user_with_key(user)
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "readonly", _readonly_policy()
        )
        assert resp.status_code == 204, resp.text

        # Need a table to test against — create with admin-level user.
        admin_user = f"adm-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = self._create_user_with_key(admin_user)
        resp = self.mgmt.put_user_policy(
            self.account_id, admin_user, "full", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text
        admin_client = _make_dynamodb_client(
            self.endpoint, admin_ak, admin_sk, self.region
        )
        table_name = f"auth-ro-{uuid.uuid4().hex[:8]}"

        try:
            admin_client.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table_name)

            ro_client = _make_dynamodb_client(self.endpoint, ak, sk, self.region)

            # GetItem should succeed (read-only allows it).
            ro_client.get_item(
                TableName=table_name, Key={"pk": {"S": "nonexistent"}}
            )

            # PutItem should be denied.
            with pytest.raises(ClientError) as exc_info:
                ro_client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "key1"}, "data": {"S": "value1"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            try:
                admin_client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(admin_client, table_name)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin_user)

    def test_no_policy_user_denied(self):
        """User with no policies is denied all operations (implicit deny)."""
        user = f"nopol-{uuid.uuid4().hex[:8]}"
        ak, sk = self._create_user_with_key(user)
        # No policy attached — only the default self-service policy exists.

        client = _make_dynamodb_client(self.endpoint, ak, sk, self.region)

        try:
            with pytest.raises(ClientError) as exc_info:
                client.list_tables()
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_group_policy_grants_access(self):
        """User inherits permissions from group membership."""
        user = f"grp-{uuid.uuid4().hex[:8]}"
        group = f"devs-{uuid.uuid4().hex[:8]}"
        ak, sk = self._create_user_with_key(user)

        resp = self.mgmt.create_group(self.account_id, group)
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.add_group_member(self.account_id, group, user)
        assert resp.status_code in (200, 201, 204), resp.text
        resp = self.mgmt.put_group_policy(
            self.account_id, group, "full-access", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        client = _make_dynamodb_client(self.endpoint, ak, sk, self.region)
        # ListTables should succeed via group policy.
        try:
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_group(self.account_id, group)

    def test_explicit_deny_overrides_allow(self):
        """Explicit Deny in one policy overrides Allow in another."""
        user = f"deny-{uuid.uuid4().hex[:8]}"
        ak, sk = self._create_user_with_key(user)

        # Allow all DynamoDB.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "allow-all", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text
        # Explicit deny on PutItem.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "deny-put",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Deny",
                    "Action": "dynamodb:PutItem",
                    "Resource": "*",
                }],
            },
        )
        assert resp.status_code == 204, resp.text

        client = _make_dynamodb_client(self.endpoint, ak, sk, self.region)
        table_name = f"auth-deny-{uuid.uuid4().hex[:8]}"

        try:
            client.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(client, table_name)

            # PutItem denied despite Allow-all.
            with pytest.raises(ClientError) as exc_info:
                client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "key1"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"

            # GetItem still allowed.
            client.get_item(
                TableName=table_name, Key={"pk": {"S": "nonexistent"}}
            )
        finally:
            try:
                client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(client, table_name)
            self.mgmt.delete_user(self.account_id, user)
# ---------------------------------------------------------------------------
# RBAC Tests — Role-Based Access Control
# ---------------------------------------------------------------------------

class TestRBAC:
    """Test role-based access control via AssumeRole temporary credentials."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_assume_role_with_full_access(self):
        """Temporary credentials from AssumeRole can perform DynamoDB ops."""
        role = f"role-{uuid.uuid4().hex[:8]}"
        user = f"caller-{uuid.uuid4().hex[:8]}"

        # Create a user to be the caller.
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"

        # Create role with trust policy allowing the user.
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": caller_arn},
                "Action": "sts:AssumeRole",
            }],
        }
        resp = self.mgmt.create_role(self.account_id, role, trust_policy)
        assert resp.status_code == 201, resp.text

        # Attach full DynamoDB access to the role.
        resp = self.mgmt.put_role_policy(
            self.account_id, role, "full-access", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        # Assume the role.
        resp = self.mgmt.assume_role(
            self.account_id, role, caller_arn, "test-session"
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        client = _make_dynamodb_client(
            self.endpoint,
            creds["access_key_id"],
            creds["secret_access_key"],
            self.region,
            session_token=creds["session_token"],
        )

        table_name = f"auth-role-{uuid.uuid4().hex[:8]}"
        try:
            client.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(client, table_name)
            client.put_item(
                TableName=table_name,
                Item={"pk": {"S": "key1"}, "data": {"S": "role-data"}},
            )
            resp = client.get_item(
                TableName=table_name, Key={"pk": {"S": "key1"}}
            )
            assert resp["Item"]["data"]["S"] == "role-data"
        finally:
            try:
                client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(client, table_name)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)

    def test_assume_role_readonly_denies_write(self):
        """Role with read-only policy denies PutItem via temporary creds."""
        role = f"rorole-{uuid.uuid4().hex[:8]}"
        user = f"caller-{uuid.uuid4().hex[:8]}"
        admin_user = f"adm-{uuid.uuid4().hex[:8]}"

        # Create caller user.
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"

        # Create admin user for table setup.
        resp = self.mgmt.create_user(self.account_id, admin_user, "TestPass123!")
        assert resp.status_code == 201
        resp = self.mgmt.create_access_key(self.account_id, admin_user)
        assert resp.status_code == 201, resp.text
        admin_creds = resp.json()
        resp = self.mgmt.put_user_policy(
            self.account_id, admin_user, "full", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        # Create role with read-only policy.
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": caller_arn},
                "Action": "sts:AssumeRole",
            }],
        }
        resp = self.mgmt.create_role(self.account_id, role, trust_policy)
        assert resp.status_code == 201
        resp = self.mgmt.put_role_policy(
            self.account_id, role, "readonly", _readonly_policy()
        )
        assert resp.status_code == 204, resp.text

        # Assume role.
        resp = self.mgmt.assume_role(
            self.account_id, role, caller_arn, "ro-session"
        )
        assert resp.status_code == 201
        creds = resp.json()

        admin_client = _make_dynamodb_client(
            self.endpoint, admin_creds["access_key_id"],
            admin_creds["secret_access_key"], self.region,
        )
        role_client = _make_dynamodb_client(
            self.endpoint, creds["access_key_id"],
            creds["secret_access_key"], self.region,
            session_token=creds["session_token"],
        )

        table_name = f"auth-rorole-{uuid.uuid4().hex[:8]}"
        try:
            admin_client.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table_name)

            # Read should work.
            role_client.get_item(
                TableName=table_name, Key={"pk": {"S": "x"}}
            )

            # Write should be denied.
            with pytest.raises(ClientError) as exc_info:
                role_client.put_item(
                    TableName=table_name,
                    Item={"pk": {"S": "key1"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            try:
                admin_client.delete_table(TableName=table_name)
            except Exception:
                pass
            else:
                wait_for_deleted(admin_client, table_name)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin_user)
            self.mgmt.delete_role(self.account_id, role)

    def test_assume_role_wrong_session_token_rejected(self):
        """A mismatched X-Amz-Security-Token is rejected even with a valid
        signature; the correct token authenticates. Regression: the session
        token is compared in constant time and enforced after signature
        verification."""
        role = f"tokrole-{uuid.uuid4().hex[:8]}"
        user = f"caller-{uuid.uuid4().hex[:8]}"

        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": caller_arn},
                "Action": "sts:AssumeRole",
            }],
        }
        resp = self.mgmt.create_role(self.account_id, role, trust_policy)
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.put_role_policy(
            self.account_id, role, "full-access", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        resp = self.mgmt.assume_role(
            self.account_id, role, caller_arn, "tok-session"
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        try:
            # Positive control: the correct session token authenticates.
            good = _make_dynamodb_client(
                self.endpoint,
                creds["access_key_id"],
                creds["secret_access_key"],
                self.region,
                session_token=creds["session_token"],
            )
            good.list_tables()

            # Same access key + secret (so the SigV4 signature is valid), but a
            # different value in X-Amz-Security-Token. The server must reject it
            # with UnrecognizedClientException, not accept it or 500.
            bad = _make_dynamodb_client(
                self.endpoint,
                creds["access_key_id"],
                creds["secret_access_key"],
                self.region,
                session_token="FwoGZXIvYXdzEBmismatchedSessionToken0123456789ABCDEF",
            )
            with pytest.raises(ClientError) as exc_info:
                bad.list_tables()
            err = exc_info.value.response["Error"]
            msg = err.get("Message", "")
            assert (
                "security token" in msg.lower() or "invalid" in msg.lower()
            ), f"expected security-token rejection, got: {err}"
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)
# ---------------------------------------------------------------------------
# Error Path Tests
# ---------------------------------------------------------------------------

class TestAuthErrors:
    """Test auth error responses match DynamoDB format."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_invalid_access_key(self):
        """Bogus access key returns UnrecognizedClientException."""
        client = _make_dynamodb_client(
            self.endpoint, "AKIAXXXXXXXXXXXXXXXX", "fakesecret" * 4, self.region
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()
        err = exc_info.value.response["Error"]["Code"]
        assert err in (
            "UnrecognizedClientException",
            "InvalidSignatureException",
        )

    def test_wrong_secret_key(self):
        """Valid access key with wrong secret returns UnrecognizedClientException."""
        user = f"badsec-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        resp = self.mgmt.create_access_key(self.account_id, user)
        creds = resp.json()

        client = _make_dynamodb_client(
            self.endpoint, creds["access_key_id"],
            "wrongsecretwrongsecretwrongsecretwrongse!", self.region,
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                client.list_tables()
            err = exc_info.value.response["Error"]["Code"]
            assert err in (
                "UnrecognizedClientException",
                "InvalidSignatureException",
            )
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_single_char_typo_secret_key(self):
        """Single-character typo in secret key is rejected (SigV4 fidelity).

        Per human requirement: even a minimal change in the secret key must
        produce a completely different signature and be rejected. This verifies
        that SigV4 signature verification is precise, not approximate.
        """
        user = f"typo-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.create_access_key(self.account_id, user)
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        real_secret = creds["secret_access_key"]

        # First: verify the real secret works.
        good_client = _make_dynamodb_client(
            self.endpoint, creds["access_key_id"], real_secret, self.region,
        )
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "full", _full_access_policy()
        )
        assert resp.status_code == 204, resp.text

        try:
            good_resp = good_client.list_tables()
            assert "TableNames" in good_resp

            # Now: flip exactly one character in the secret key.
            chars = list(real_secret)
            chars[0] = "A" if chars[0] != "A" else "B"
            bad_secret = "".join(chars)
            assert bad_secret != real_secret

            bad_client = _make_dynamodb_client(
                self.endpoint, creds["access_key_id"], bad_secret, self.region,
            )
            with pytest.raises(ClientError) as exc_info:
                bad_client.list_tables()
            err = exc_info.value.response["Error"]["Code"]
            assert err in (
                "UnrecognizedClientException",
                "InvalidSignatureException",
            )
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_access_denied_message_format(self):
        """AccessDeniedException message includes ARN and action."""
        user = f"denied-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        resp = self.mgmt.create_access_key(self.account_id, user)
        creds = resp.json()
        # No policy — implicit deny.

        client = _make_dynamodb_client(
            self.endpoint, creds["access_key_id"],
            creds["secret_access_key"], self.region,
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                client.list_tables()
            err = exc_info.value.response["Error"]
            assert err["Code"] == "AccessDeniedException"
            # Message should contain the user ARN and the action.
            msg = err.get("Message", "")
            assert "is not authorized to perform" in msg
            assert "dynamodb:" in msg
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_missing_auth_header(self):
        """Request with no auth header returns MissingAuthenticationToken."""
        # Use a raw HTTP request to avoid boto3 adding auth.
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
# ---------------------------------------------------------------------------
# Mode 1 Backward Compatibility
# ---------------------------------------------------------------------------

class TestMode1Compat:
    """Verify that unauthenticated requests are REJECTED.

    When extenddb runs with auth.provider = 'builtin' (the shipped default),
    requests using placeholder/fake credentials must be denied. If they are
    accepted, auth enforcement is broken.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        self.endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    def test_unauthenticated_list_tables_rejected(self):
        """Placeholder credentials must be rejected for ListTables."""
        client = _make_dynamodb_client(
            self.endpoint, "local-dev-key", "local-dev-secret", self.region
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()
        err = exc_info.value.response["Error"]
        assert err["Code"] in ("UnrecognizedClientException", "InvalidSignatureException", "AccessDeniedException")

    def test_unauthenticated_create_table_rejected(self):
        """Placeholder credentials must be rejected for CreateTable."""
        client = _make_dynamodb_client(
            self.endpoint, "local-dev-key", "local-dev-secret", self.region
        )
        with pytest.raises(ClientError) as exc_info:
            client.create_table(
                TableName=f"mode1-{uuid.uuid4().hex[:8]}",
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] in ("UnrecognizedClientException", "InvalidSignatureException", "AccessDeniedException")
