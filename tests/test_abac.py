# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""ABAC (Attribute-Based Access Control) tests.

Verifies that IAM policies using condition keys with resource tags and
principal tags work correctly. Requires Phase 15c resource tag support.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run (encryption key + admin user exist)

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_abac.py -v

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import base64
import hashlib
import os
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
            "MISCONFIGURED: ABAC tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass
def _make_client(endpoint_url: str, access_key: str, secret_key: str,
                 region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
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
    resp = mgmt.create_account(acct_id, f"abac-test-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)
def _create_user_with_key(mgmt: ManagementClient, account_id: str,
                          user_name: str) -> tuple[str, str]:
    """Create IAM user + access key, return (ak, sk)."""
    resp = mgmt.create_user(account_id, user_name, "TestPass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, user_name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return creds["access_key_id"], creds["secret_access_key"]
# ---------------------------------------------------------------------------
# Tag-based condition tests
# ---------------------------------------------------------------------------

class TestResourceTagCondition:
    """Policies with aws:ResourceTag/* conditions."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_resource_tag_allows_matching_table(self):
        """User with tag condition can access table whose tag matches."""
        user = f"tag-ok-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)

        # Admin user to create and tag the table.
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = _create_user_with_key(
            self.mgmt, self.account_id, admin
        )
        resp = self.mgmt.put_user_policy(
            self.account_id, admin, "full",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
            }]},
        )
        assert resp.status_code == 204

        # Policy: allow only when resource tag Env=dev.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "tag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["dynamodb:GetItem", "dynamodb:PutItem",
                               "dynamodb:DescribeTable"],
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "aws:ResourceTag/Env": "dev",
                        },
                    },
                }],
            },
        )
        assert resp.status_code == 204

        admin_client = _make_client(
            self.endpoint, admin_ak, admin_sk, self.region
        )
        table = f"abac-{uuid.uuid4().hex[:8]}"

        try:
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"}
                ],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                Tags=[{"Key": "Env", "Value": "dev"}],
            )
            wait_for_active(admin_client, table)

            # User with tag condition should succeed.
            user_client = _make_client(
                self.endpoint, ak, sk, self.region
            )
            user_client.put_item(
                TableName=table,
                Item={"pk": {"S": "k1"}, "data": {"S": "v1"}},
            )
            resp = user_client.get_item(
                TableName=table, Key={"pk": {"S": "k1"}}
            )
            assert resp["Item"]["data"]["S"] == "v1"
        finally:
            try:
                admin_client.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(admin_client, table)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin)

    def test_resource_tag_denies_non_matching_table(self):
        """User with tag condition is denied on table with different tag."""
        user = f"tag-no-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)

        admin = f"adm-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = _create_user_with_key(
            self.mgmt, self.account_id, admin
        )
        resp = self.mgmt.put_user_policy(
            self.account_id, admin, "full",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
            }]},
        )
        assert resp.status_code == 204

        # Policy: allow only Env=dev.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "tag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["dynamodb:GetItem", "dynamodb:PutItem",
                               "dynamodb:DescribeTable"],
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "aws:ResourceTag/Env": "dev",
                        },
                    },
                }],
            },
        )
        assert resp.status_code == 204

        admin_client = _make_client(
            self.endpoint, admin_ak, admin_sk, self.region
        )
        table = f"abac-prod-{uuid.uuid4().hex[:8]}"

        try:
            # Table tagged Env=prod — should NOT match the user's policy.
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"}
                ],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
                Tags=[{"Key": "Env", "Value": "prod"}],
            )
            wait_for_active(admin_client, table)

            user_client = _make_client(
                self.endpoint, ak, sk, self.region
            )
            with pytest.raises(ClientError) as exc_info:
                user_client.put_item(
                    TableName=table,
                    Item={"pk": {"S": "k1"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            try:
                admin_client.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(admin_client, table)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin)
class TestPrincipalTagCondition:
    """Policies with aws:PrincipalTag/* conditions."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def test_principal_tag_allows_matching_user(self):
        """User tagged Team=backend is allowed by PrincipalTag condition."""
        user = f"ptag-ok-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)

        # Tag the user.
        resp = self.mgmt.tag_user(
            self.account_id, user, {"Team": "backend"}
        )
        assert resp.status_code in (200, 204), resp.text

        # Policy: allow when PrincipalTag/Team = backend.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "ptag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "dynamodb:ListTables",
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalTag/Team": "backend",
                        },
                    },
                }],
            },
        )
        assert resp.status_code == 204

        try:
            client = _make_client(self.endpoint, ak, sk, self.region)
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_principal_tag_denies_non_matching_user(self):
        """User tagged Team=frontend is denied by PrincipalTag/Team=backend."""
        user = f"ptag-no-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)

        resp = self.mgmt.tag_user(
            self.account_id, user, {"Team": "frontend"}
        )
        assert resp.status_code in (200, 204), resp.text

        resp = self.mgmt.put_user_policy(
            self.account_id, user, "ptag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "dynamodb:ListTables",
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalTag/Team": "backend",
                        },
                    },
                }],
            },
        )
        assert resp.status_code == 204

        try:
            client = _make_client(self.endpoint, ak, sk, self.region)
            with pytest.raises(ClientError) as exc_info:
                client.list_tables()
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)
class TestRoleSessionTagCondition:
    """ABAC with session tags passed via AssumeRole."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _setup_role_with_session_tag_policy(self, user, role):
        """Create user + role with a PrincipalTag/Project=extenddb condition."""
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": caller_arn},
                "Action": ["sts:AssumeRole", "sts:TagSession"],
            }],
        }
        resp = self.mgmt.create_role(self.account_id, role, trust_policy)
        assert resp.status_code == 201

        resp = self.mgmt.put_role_policy(
            self.account_id, role, "stag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "dynamodb:ListTables",
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalTag/Project": "extenddb",
                        },
                    },
                }],
            },
        )
        assert resp.status_code == 204
        return caller_arn

    def test_session_tag_allows_matching_role(self):
        """Role session with matching session tag is allowed."""
        user = f"stag-caller-{uuid.uuid4().hex[:8]}"
        role = f"stag-role-{uuid.uuid4().hex[:8]}"
        caller_arn = self._setup_role_with_session_tag_policy(user, role)

        resp = self.mgmt.assume_role(
            self.account_id, role, caller_arn, "test-session",
            session_tags={"Project": "extenddb"},
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        try:
            client = boto3.client(
                "dynamodb",
                endpoint_url=self.endpoint,
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                aws_session_token=creds["session_token"],
                region_name=self.region,
                config=BotoConfig(retries={"max_attempts": 0}),
                verify=not self.endpoint.startswith("https://"),
            )
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)

    def test_session_tag_denies_non_matching_role(self):
        """Role session with non-matching session tag is denied."""
        user = f"stag-deny-{uuid.uuid4().hex[:8]}"
        role = f"stag-deny-r-{uuid.uuid4().hex[:8]}"
        caller_arn = self._setup_role_with_session_tag_policy(user, role)

        resp = self.mgmt.assume_role(
            self.account_id, role, caller_arn, "deny-session",
            session_tags={"Project": "other"},
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        try:
            client = boto3.client(
                "dynamodb",
                endpoint_url=self.endpoint,
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                aws_session_token=creds["session_token"],
                region_name=self.region,
                config=BotoConfig(retries={"max_attempts": 0}),
                verify=not self.endpoint.startswith("https://"),
            )
            with pytest.raises(ClientError) as exc_info:
                client.list_tables()
            assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_role(self.account_id, role)


class TestOrganizationKeysAbsent:
    """Identity policies gated on an organization key (for example
    aws:PrincipalOrgID). An ExtendDB account is a standalone AWS account, so
    IAM leaves these keys absent and absent-key semantics apply: a positive
    operator does not match, and a negated operator on an absent single-valued
    key matches. A key ExtendDB resolves (aws:PrincipalArn) evaluates normally.
    """

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _admin_table(self) -> tuple[Any, str]:
        admin = f"ur-adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, admin)
        resp = self.mgmt.put_user_policy(
            self.account_id, admin, "full",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]},
        )
        assert resp.status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        table = f"urk-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table)
        client.put_item(TableName=table, Item={"pk": {"S": "k1"}, "data": {"S": "v1"}})
        return client, table

    def test_deny_on_absent_orgid_does_not_block(self):
        admin_client, table = self._admin_table()
        user = f"urk-deny-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "p",
            {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
                 "Condition": {"StringEquals": {"aws:PrincipalOrgID": "o-example1"}}}]},
        )
        assert resp.status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            # An ExtendDB account is a standalone AWS account, so
            # aws:PrincipalOrgID is absent; StringEquals on an absent key does
            # not match, so the Deny is inert and GetItem is allowed. Matches
            # live IAM for a standalone account.
            out = client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            assert out["Item"]["data"]["S"] == "v1"
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_negated_allow_on_absent_orgid_grants(self):
        admin_client, table = self._admin_table()
        user = f"urk-neg-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "p",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                "Condition": {"StringNotEquals": {"aws:PrincipalOrgID": "o-example1"}}}]},
        )
        assert resp.status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            # Org key absent for a standalone account; a negated operator on an
            # absent single-valued key matches, so the Allow grants. Matches live
            # IAM for a standalone account.
            out = client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            assert out["Item"]["data"]["S"] == "v1"
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_gated_on_resolved_principal_arn_grants(self):
        admin_client, table = self._admin_table()
        user = f"urk-arn-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        user_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "p",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                "Condition": {"StringEquals": {"aws:PrincipalArn": user_arn}}}]},
        )
        assert resp.status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            out = client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            assert out["Item"]["data"]["S"] == "v1"
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Revised global-key rule: resolvable keys, service-populated keys, allowlist
# ---------------------------------------------------------------------------

def _run_get(mgmt: ManagementClient, endpoint: str, region: str, account_id: str,
             table: str, policy: dict) -> str:
    """Create a fresh user with `policy`, call GetItem on `table`, return the
    outcome, and clean the user up. ExtendDB serves fresh policy on write, so no
    retry is needed."""
    user = f"gk-{uuid.uuid4().hex[:8]}"
    ak, sk = _create_user_with_key(mgmt, account_id, user)
    resp = mgmt.put_user_policy(account_id, user, "p", policy)
    assert resp.status_code == 204, resp.text
    client = _make_client(endpoint, ak, sk, region)
    try:
        client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
        return "ALLOW"
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
        return "DENY"
    finally:
        mgmt.delete_user(account_id, user)


class _TaggedTableBase:
    """Shared env=prod tagged table for the global-key tests."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    @pytest.fixture
    def table(self):
        admin = f"gk-adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, admin)
        resp = self.mgmt.put_user_policy(
            self.account_id, admin, "full",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]},
        )
        assert resp.status_code == 204
        admin_client = _make_client(self.endpoint, ak, sk, self.region)
        name = f"gk-{uuid.uuid4().hex[:8]}"
        admin_client.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        wait_for_active(admin_client, name)
        admin_client.put_item(TableName=name, Item={"pk": {"S": "k1"}, "data": {"S": "v1"}})
        yield name
        try:
            admin_client.delete_table(TableName=name)
            wait_for_deleted(admin_client, name)
        except Exception:
            pass
        self.mgmt.delete_user(self.account_id, admin)

    def _get(self, table: str, policy: dict) -> str:
        return _run_get(self.mgmt, self.endpoint, self.region, self.account_id, table, policy)

    @staticmethod
    def _allow_get_if(condition: dict) -> dict:
        return {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": condition}]}

    @staticmethod
    def _allow_all_deny_get_if(condition: dict) -> dict:
        return {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": condition}]}


class TestResolvableGlobalKeys(_TaggedTableBase):
    """Guard statements on keys ExtendDB resolves no longer fail closed.
    All four grant, matching live DynamoDB."""

    def test_securetransport_deny_guard_allows(self, table):
        # Deny if not over TLS; the request is over TLS so the deny is inert.
        assert self._get(table, self._allow_all_deny_get_if(
            {"Bool": {"aws:SecureTransport": "false"}})) == "ALLOW"

    def test_requestedregion_deny_guard_allows(self, table):
        # Deny if region is not the server region; it is, so the deny is inert.
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringNotEquals": {"aws:RequestedRegion": self.region}})) == "ALLOW"

    def test_principaltype_user_allows(self, table):
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:PrincipalType": "User"}})) == "ALLOW"

    def test_currenttime_allows(self, table):
        assert self._get(table, self._allow_get_if(
            {"DateGreaterThan": {"aws:CurrentTime": "2020-01-01T00:00:00Z"}})) == "ALLOW"


class TestServicePopulatedGlobalKeys(_TaggedTableBase):
    """The allowlist: keys the service populates evaluate as present, and
    an unknown aws: key fails closed."""

    def test_viaawsservice_negated_allow_denies(self, table):
        # ViaAWSService is present and false; StringNotEquals=false is false, so
        # the Allow does not grant. Live DynamoDB: DENY.
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:ViaAWSService": "false"}})) == "DENY"

    def test_principalisawsservice_negated_allow_denies(self, table):
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:PrincipalIsAWSService": "false"}})) == "DENY"

    def test_resourcetag_negated_allow_denies(self, table):
        # Table tagged env=prod; StringNotEquals env=prod is false. Live: DENY.
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:ResourceTag/env": "prod"}})) == "DENY"

    def test_resourcetag_positive_allow_grants(self, table):
        # aws:ResourceTag/env resolves to prod. Live: ALLOW.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:ResourceTag/env": "prod"}})) == "ALLOW"

    def test_mfa_boolifexists_allows_non_mfa_caller(self, table):
        # A non-MFA caller has aws:MultiFactorAuthPresent absent; BoolIfExists
        # passes when the key is absent. Live: ALLOW.
        assert self._get(table, self._allow_get_if(
            {"BoolIfExists": {"aws:MultiFactorAuthPresent": "false"}})) == "ALLOW"

    def test_unknown_key_negated_allow_grants(self, table):
        # An unknown aws: key is absent, as on the service, so the negated
        # operator matches an absent single-valued key and the Allow grants.
        # Live DynamoDB: ALLOW. (Round two failed closed and denied here; this
        # row changes from DENY to ALLOW under the revised rule.)
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:NoSuchGlobalKey": "x"}})) == "ALLOW"

    def test_unknown_key_deny_negated_blocks(self, table):
        # A Deny gated on StringNotEquals of an unknown aws: key: the key is
        # absent, the negated operator matches, so the Deny applies and blocks.
        # Live DynamoDB also denies here.
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringNotEquals": {"aws:NoSuchGlobalKey": "x"}})) == "DENY"


class TestResourceTagConditionKeyForms(_TaggedTableBase):
    """Resource tags evaluate only as aws:ResourceTag/*, matching live DynamoDB.
    aws:ResourceTag/env resolves to the table's tag value; dynamodb:ResourceTag/env
    is absent and keeps IAM absent-key semantics. Table is tagged env=prod. Every
    expected outcome was measured live on 2026-10-21."""

    def test_aws_resourcetag_eq_grants(self, table):
        # aws:ResourceTag/env resolves to prod, so StringEquals matches. Live: ALLOW.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:ResourceTag/env": "prod"}})) == "ALLOW"

    def test_aws_resourcetag_ne_denies(self, table):
        # aws:ResourceTag/env resolves to prod, so StringNotEquals is false and the
        # Allow does not grant. Live: DENY.
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:ResourceTag/env": "prod"}})) == "DENY"

    def test_aws_resourcetag_deny_eq_blocks(self, table):
        # A Deny gated on StringEquals aws:ResourceTag/env=prod fires on the prod
        # table. Live: DENY.
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringEquals": {"aws:ResourceTag/env": "prod"}})) == "DENY"

    def test_dynamodb_resourcetag_eq_denies_absent_key(self, table):
        # dynamodb:ResourceTag/env is absent, so StringEquals never matches and the
        # Allow does not grant. Live: DENY.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"dynamodb:ResourceTag/env": "prod"}})) == "DENY"

    def test_dynamodb_resourcetag_ne_grants_absent_key(self, table):
        # dynamodb:ResourceTag/env is absent, so StringNotEquals on an absent
        # single-valued key matches and the Allow grants. Live: ALLOW.
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"dynamodb:ResourceTag/env": "prod"}})) == "ALLOW"

    def test_dynamodb_resourcetag_deny_eq_does_not_block_absent_key(self, table):
        # A Deny gated on StringEquals dynamodb:ResourceTag/env=prod does not fire,
        # because the key is absent, so the base Allow stands. Live: ALLOW.
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringEquals": {"dynamodb:ResourceTag/env": "prod"}})) == "ALLOW"


class TestRound3GlobalKeys(_TaggedTableBase):
    """Round three keys on the identity path: aws:userid, aws:SourceIp,
    aws:UserAgent, aws:ResourceAccount, and the organization keys absent for a
    standalone account. Every expected outcome was measured live."""

    def test_userid_present_denies_null_true(self, table):
        # aws:userid is populated, so Null true is false and the Allow does not
        # grant.
        assert self._get(table, self._allow_get_if(
            {"Null": {"aws:userid": "true"}})) == "DENY"

    def test_userid_like_aida_grants(self, table):
        # A user's aws:userid begins with AIDA.
        assert self._get(table, self._allow_get_if(
            {"StringLike": {"aws:userid": "AIDA*"}})) == "ALLOW"

    def test_sourceip_loopback_grants(self, table):
        # The test client connects over the IPv4 loopback, so aws:SourceIp is
        # 127.0.0.1 and IpAddress on the loopback grants.
        assert self._get(table, self._allow_get_if(
            {"IpAddress": {"aws:SourceIp": "127.0.0.1/32"}})) == "ALLOW"

    def test_sourceip_other_range_denies(self, table):
        assert self._get(table, self._allow_get_if(
            {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}})) == "DENY"

    def test_sourceip_notipaddress_loopback_does_not_grant(self, table):
        # NotIpAddress excludes the caller's own loopback address, so the Allow
        # does not grant.
        assert self._get(table, self._allow_get_if(
            {"NotIpAddress": {"aws:SourceIp": "127.0.0.1/32"}})) == "DENY"

    def test_useragent_stringlike_grants(self, table):
        # The boto3 client's User-Agent contains Botocore.
        assert self._get(table, self._allow_get_if(
            {"StringLike": {"aws:UserAgent": "*Botocore*"}})) == "ALLOW"

    def test_useragent_stringlike_no_match_denies(self, table):
        assert self._get(table, self._allow_get_if(
            {"StringLike": {"aws:UserAgent": "*ZZZNoSuchAgentZZZ*"}})) == "DENY"

    def test_resourceaccount_eq_grants(self, table):
        # aws:ResourceAccount is the table owner account.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:ResourceAccount": self.account_id}})) == "ALLOW"

    def test_resourceaccount_present_denies_null_true(self, table):
        assert self._get(table, self._allow_get_if(
            {"Null": {"aws:ResourceAccount": "true"}})) == "DENY"

    def test_principalorgid_absent_null_true_grants(self, table):
        # Standalone account: the organization key is absent, so Null true
        # grants.
        assert self._get(table, self._allow_get_if(
            {"Null": {"aws:PrincipalOrgID": "true"}})) == "ALLOW"

    def test_principalorgid_negated_allow_grants(self, table):
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:PrincipalOrgID": "o-example1"}})) == "ALLOW"


class TestConditionKeyNameCaseInsensitive(_TaggedTableBase):
    """Condition key NAMES resolve case-insensitively on the identity path,
    matching the live service. A miscased key resolves the same value as its
    canonical form; a case-sensitive lookup would read it as absent and flip the
    outcome."""

    def test_principaltype_lowercase_key_allows(self, table):
        # aws:principaltype resolves to User like aws:PrincipalType.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:principaltype": "User"}})) == "ALLOW"

    def test_securetransport_lowercase_key_deny_guard_allows(self, table):
        # aws:securetransport resolves to true over TLS, so the Deny is inert.
        assert self._get(table, self._allow_all_deny_get_if(
            {"Bool": {"aws:securetransport": "false"}})) == "ALLOW"

    def test_sourceip_lowercase_key_grants(self, table):
        # aws:sourceip resolves like aws:SourceIp; 0.0.0.0/0 matches any caller.
        assert self._get(table, self._allow_get_if(
            {"IpAddress": {"aws:sourceip": "0.0.0.0/0"}})) == "ALLOW"

    def test_leadingkeys_lowercase_key_grants(self, table):
        # dynamodb:leadingkeys resolves like dynamodb:LeadingKeys; the GetItem
        # key is k1.
        assert self._get(table, self._allow_get_if(
            {"ForAllValues:StringEquals": {"dynamodb:leadingkeys": ["k1"]}})) == "ALLOW"

    def test_resourcetag_lowercase_prefix_grants(self, table):
        # The tag prefix aws:resourcetag/ (lower) resolves like aws:ResourceTag/;
        # the table is tagged env=prod.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"aws:resourcetag/env": "prod"}})) == "ALLOW"

    def test_dynamodb_resourcetag_lowercase_prefix_is_absent(self, table):
        # dynamodb:ResourceTag/* is absent in any case, as on DynamoDB, so a
        # positive operator on it never matches and the Allow does not grant.
        assert self._get(table, self._allow_get_if(
            {"StringEquals": {"dynamodb:resourcetag/env": "prod"}})) == "DENY"

    def test_principaltype_deny_lowercase_key_blocks(self, table):
        # A Deny gated on a miscased key must still be enforced. aws:principaltype
        # resolves to User, so the Deny applies and blocks. A case-sensitive
        # lookup would read the key as absent, drop the Deny, and grant.
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringEquals": {"aws:principaltype": "User"}})) == "DENY"


class TestConditionKeyNameUnicodeFold(_TaggedTableBase):
    """Condition key NAMES and tag keys resolve under Unicode full case
    folding on the identity path, matching the live service. A key spelled with
    the long s (U+017F) or the Kelvin sign (U+212A) folds to its ASCII twin; a
    case-sensitive or ASCII-only fold would read it as absent and flip the
    outcome."""

    LS = "\u017f"   # long s -> s
    KV = "\u212a"   # Kelvin -> k

    def test_longs_securetransport_resolves(self, table):
        # aws:SecureTran[s]port folds to securetransport=true; the positive gate
        # grants.
        assert self._get(table, self._allow_get_if(
            {"Bool": {f"aws:SecureTran{self.LS}port": "true"}})) == "ALLOW"

    def test_longs_securetransport_negated_allow_does_not_overgrant(self, table):
        # Allow if StringNotEquals aws:SecureTran[s]port=true. The key folds and
        # resolves to true, so NotEquals is false and the Allow does not grant.
        # ASCII-only folding would read the key as absent, the negated-absent
        # rule would fire, and the request would be granted (the bypass).
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {f"aws:SecureTran{self.LS}port": "true"}})) == "DENY"

    def test_kelvin_leadingkeys_match_grants(self, table):
        # dynamodb:Leading[k]eys folds to leadingkeys=[k1]; ForAllValues against
        # [k1] holds, the Allow grants.
        assert self._get(table, self._allow_get_if(
            {"ForAllValues:StringEquals": {f"dynamodb:Leading{self.KV}eys": ["k1"]}})) == "ALLOW"

    def test_kelvin_leadingkeys_nonmatch_does_not_overgrant(self, table):
        # Allow if ForAllValues:StringEquals dynamodb:Leading[k]eys=[zzz] on
        # GetItem pk=k1. The key folds to leadingkeys=[k1], which is not zzz, so
        # ForAllValues fails and the Allow does not grant. ASCII-only folding
        # would read the key absent, ForAllValues would be vacuously true, and
        # the request would be granted (the bypass).
        assert self._get(table, self._allow_get_if(
            {"ForAllValues:StringEquals": {f"dynamodb:Leading{self.KV}eys": ["zzz"]}})) == "DENY"


class TestLeadingZeroCidr(_TaggedTableBase):
    """A leading-zero octet in an IpAddress CIDR is accepted at write and
    read as decimal at evaluation, matching the live service (072 is 72, not
    octal 58; 0110 is 110). The test client connects from 127.0.0.1."""

    def test_leading_zero_octet_accepted_and_matches_decimal(self, table):
        # 0127.0.0.0/8 is the 127.0.0.0/8 network (decimal); the loopback caller
        # is in it, so the gate grants. Before the fix the write was rejected.
        assert self._get(table, self._allow_get_if(
            {"IpAddress": {"aws:SourceIp": "0127.0.0.0/8"}})) == "ALLOW"

    def test_leading_zero_octet_not_octal(self, table):
        # 0177.0.0.0/8 read as decimal is 177.0.0.0/8, which does not contain the
        # loopback caller; octal 0177 would be 127 and would match. The service
        # reads decimal, so the gate does not grant.
        assert self._get(table, self._allow_get_if(
            {"IpAddress": {"aws:SourceIp": "0177.0.0.0/8"}})) == "DENY"


class TestTagCollisionDeterministic(_TaggedTableBase):
    """Two stored tag keys that fold to the same string (mass and ma\u00df)
    must resolve deterministically to the exact key, every time, not at random
    HashMap order."""

    SHARP = "\u00df"

    CHECKS = 20

    def _resolve_once(self, table, policy_key):
        """Create a FRESH user tagged {mass:A, ma\u00df:B}, allow GetItem when
        aws:PrincipalTag/<policy_key> == A, and return "ALLOW" or "DENY".

        A fresh user per check is the point: the principal's tag map is cached
        per user, so one user sees a single HashMap order and hides the
        nondeterminism. Each fresh user gets its own order, so resolution that
        is not pinned to the exact (then smallest-by-code-point) key shows up as
        a DENY on some fraction of users.
        """
        user = f"gk-coll-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        try:
            assert self.mgmt.tag_user(
                self.account_id, user, {"mass": "A", f"ma{self.SHARP}": "B"}
            ).status_code in (200, 201, 204)
            assert self.mgmt.put_user_policy(self.account_id, user, "p", {
                "Version": "2012-10-17", "Statement": [{
                    "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                    "Condition": {"StringEquals": {
                        f"aws:PrincipalTag/{policy_key}": "A"}}}]}).status_code == 204
            client = _make_client(self.endpoint, ak, sk, self.region)
            try:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
                return "ALLOW"
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDeniedException"
                return "DENY"
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_fold_colliding_exact_key_resolves_deterministically(self, table):
        # aws:PrincipalTag/mass must resolve the byte-exact stored key (mass=A),
        # so every one of CHECKS fresh users allows.
        results = [self._resolve_once(table, "mass") for _ in range(self.CHECKS)]
        assert set(results) == {"ALLOW"}, results

    def test_fold_colliding_no_exact_key_resolves_deterministically(self, table):
        # aws:PrincipalTag/MASS has no byte-exact key; it folds to "mass",
        # colliding with mass and ma\u00df. The rule picks the smallest
        # fold-matching key by code point (mass < ma\u00df), so A resolves and
        # every one of CHECKS fresh users allows.
        results = [self._resolve_once(table, "MASS") for _ in range(self.CHECKS)]
        assert set(results) == {"ALLOW"}, results

    def test_rejects_invalid_and_dup_tag_keys_at_write(self):
        # A combining-mark tag key and the reserved aws: prefix are rejected
        # at write (ValidationError on the service); two keys differing only
        # in ASCII case are a duplicate.
        user = f"gk-tv-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        try:
            assert self.mgmt.tag_user(self.account_id, user, {"Equipe\u0301": "x"}).status_code == 400
            assert self.mgmt.tag_user(self.account_id, user, {"aws:team": "x"}).status_code == 400
            assert self.mgmt.tag_user(self.account_id, user, {"Team": "1", "team": "2"}).status_code == 400
            # a valid tag is accepted
            assert self.mgmt.tag_user(self.account_id, user, {"team": "prod"}).status_code in (200, 201, 204)
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# aws:TokenIssueTime (role sessions) and aws:userid across recreate
# ---------------------------------------------------------------------------

# The service id derivation, reproduced here so a policy can be pinned to the
# exact id of a principal's first incarnation. It hashes the principal type,
# account, name and creation time, then base32-encodes the first 17 characters,
# with an AIDA (user) or AROA (role) prefix. The creation time is read verbatim
# from the management list response, the same RFC 3339 string the server hashes.
_B32_LEN = 17


def _id_body(parts: list[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return base64.b32encode(h.digest()).decode("ascii").rstrip("=")[:_B32_LEN]


def _derive_user_id(account_id: str, user_name: str, created_at: str) -> str:
    return "AIDA" + _id_body(["user", account_id, user_name, created_at])


def _derive_role_id(account_id: str, role_name: str, created_at: str) -> str:
    return "AROA" + _id_body(["role", account_id, role_name, created_at])


def _user_created_at(mgmt: ManagementClient, account_id: str, user_name: str) -> str:
    resp = mgmt.list_users(account_id)
    assert resp.status_code == 200, resp.text
    for e in resp.json():
        if e["user_name"] == user_name:
            return e["created_at"]
    raise AssertionError(f"user {user_name} not found in list_users")


def _role_created_at(mgmt: ManagementClient, account_id: str, role_name: str) -> str:
    resp = mgmt.list_roles(account_id)
    assert resp.status_code == 200, resp.text
    for e in resp.json():
        if e["role_name"] == role_name:
            return e["created_at"]
    raise AssertionError(f"role {role_name} not found in list_roles")


class TestTokenIssueTimeRoleSession(_TaggedTableBase):
    """aws:TokenIssueTime is the session issue time for a role session and
    is absent for an IAM user. Measured live against STS/DynamoDB."""

    def _role_session_get(self, table: str, condition: dict) -> str:
        """Create a user and a role whose identity policy allows GetItem under
        `condition`, assume the role, GetItem with the session credentials, and
        return the outcome."""
        user = f"tit-caller-{uuid.uuid4().hex[:8]}"
        role = f"tit-role-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201, resp.text
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": caller_arn},
            "Action": "sts:AssumeRole"}]}
        resp = self.mgmt.create_role(self.account_id, role, trust)
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.put_role_policy(
            self.account_id, role, "p",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                "Condition": condition}]})
        assert resp.status_code == 204, resp.text
        try:
            resp = self.mgmt.assume_role(
                self.account_id, role, caller_arn, "sess1")
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            client = boto3.client(
                "dynamodb", endpoint_url=self.endpoint,
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                aws_session_token=creds["session_token"],
                region_name=self.region,
                config=BotoConfig(retries={"max_attempts": 0}),
                verify=not self.endpoint.startswith("https://"))
            try:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
                return "ALLOW"
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
                return "DENY"
        finally:
            self.mgmt.delete_role(self.account_id, role)
            self.mgmt.delete_user(self.account_id, user)

    def test_role_session_token_issue_time_present_denies_null_true(self, table):
        # Present for a role session, so Null true is false and the Allow does
        # not grant.
        assert self._role_session_get(
            table, {"Null": {"aws:TokenIssueTime": "true"}}) == "DENY"

    def test_role_session_token_issue_time_after_recent_past_grants(self, table):
        # The session was issued just now, which is after a point an hour ago.
        past = "2000-01-01T00:00:00Z"
        assert self._role_session_get(
            table, {"DateGreaterThan": {"aws:TokenIssueTime": past}}) == "ALLOW"

    def test_role_session_token_issue_time_before_far_future_denies(self, table):
        # The session issue time is not after a point far in the future.
        future = "2999-01-01T00:00:00Z"
        assert self._role_session_get(
            table, {"DateGreaterThan": {"aws:TokenIssueTime": future}}) == "DENY"

    def test_user_token_issue_time_absent_null_true_grants(self, table):
        # An IAM user leaves aws:TokenIssueTime absent, so Null true grants.
        assert self._get(table, self._allow_get_if(
            {"Null": {"aws:TokenIssueTime": "true"}})) == "ALLOW"

    def test_user_token_issue_time_absent_date_operator_denies(self, table):
        # A positive Date operator on the absent key does not match, so the
        # Allow does not grant.
        assert self._get(table, self._allow_get_if(
            {"DateGreaterThan": {"aws:TokenIssueTime": "2000-01-01T00:00:00Z"}})) == "DENY"


class TestUserIdRecreate(_TaggedTableBase):
    """aws:userid changes when a principal is deleted and recreated with
    the same name, so a policy pinned to the first incarnation's id denies the
    second. Measured live against IAM/DynamoDB."""

    def test_user_id_changes_on_recreate(self, table):
        name = f"uidrc-{uuid.uuid4().hex[:8]}"
        # First incarnation: pin a policy to its id, confirm it grants.
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, name)
        created1 = _user_created_at(self.mgmt, self.account_id, name)
        uid1 = _derive_user_id(self.account_id, name, created1)
        pol = self._allow_get_if({"StringEquals": {"aws:userid": uid1}})
        assert self.mgmt.put_user_policy(self.account_id, name, "p", pol).status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        try:
            client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            first = "ALLOW"
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
            first = "DENY"
        assert first == "ALLOW", "policy pinned to the first incarnation id should grant"

        # Delete and recreate with the same name: a new id.
        self.mgmt.delete_user(self.account_id, name)
        ak2, sk2 = _create_user_with_key(self.mgmt, self.account_id, name)
        created2 = _user_created_at(self.mgmt, self.account_id, name)
        assert created2 != created1, "recreated user should have a new creation time"
        # Reattach the SAME policy (still pinned to uid1).
        assert self.mgmt.put_user_policy(self.account_id, name, "p", pol).status_code == 204
        client2 = _make_client(self.endpoint, ak2, sk2, self.region)
        try:
            client2.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            second = "ALLOW"
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
            second = "DENY"
        finally:
            self.mgmt.delete_user(self.account_id, name)
        assert second == "DENY", "recreated principal must not match the old id"

    def test_role_id_changes_on_recreate(self, table):
        caller = f"ridrc-caller-{uuid.uuid4().hex[:8]}"
        role = f"ridrc-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_user(self.account_id, caller, "TestPass123!").status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{caller}"
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": caller_arn},
            "Action": "sts:AssumeRole"}]}

        def assume_and_get(expected_userid: str) -> str:
            pol = {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
                "Condition": {"StringEquals": {"aws:userid": expected_userid}}}]}
            assert self.mgmt.put_role_policy(self.account_id, role, "p", pol).status_code == 204
            resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "sess1")
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            client = boto3.client(
                "dynamodb", endpoint_url=self.endpoint,
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                aws_session_token=creds["session_token"],
                region_name=self.region,
                config=BotoConfig(retries={"max_attempts": 0}),
                verify=not self.endpoint.startswith("https://"))
            try:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
                return "ALLOW"
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
                return "DENY"

        try:
            assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
            created1 = _role_created_at(self.mgmt, self.account_id, role)
            uid1 = _derive_role_id(self.account_id, role, created1) + ":sess1"
            assert assume_and_get(uid1) == "ALLOW", "role session should match the pinned id"

            # Recreate the role with the same name: a new role id.
            self.mgmt.delete_role(self.account_id, role)
            assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
            created2 = _role_created_at(self.mgmt, self.account_id, role)
            assert created2 != created1, "recreated role should have a new creation time"
            # The policy is still pinned to the first incarnation id.
            assert assume_and_get(uid1) == "DENY", "recreated role must not match the old id"
        finally:
            self.mgmt.delete_role(self.account_id, role)
            self.mgmt.delete_user(self.account_id, caller)


class TestMultiFactorAuthPresentRoleSession(_TaggedTableBase):
    """aws:MultiFactorAuthPresent is present and false for a role session
    assumed without MFA, and absent for an IAM user. Measured live against
    STS/DynamoDB (role session DENY, user ALLOW on the deny-without-MFA guard)."""

    def _role_session_get(self, table: str, policy: dict) -> str:
        user = f"mfa-caller-{uuid.uuid4().hex[:8]}"
        role = f"mfa-role-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        caller_arn = f"arn:aws:iam::{self.account_id}:user/{user}"
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": caller_arn},
            "Action": "sts:AssumeRole"}]}
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        assert self.mgmt.put_role_policy(self.account_id, role, "p", policy).status_code == 204
        try:
            resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "sess1")
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            client = boto3.client(
                "dynamodb", endpoint_url=self.endpoint,
                aws_access_key_id=creds["access_key_id"],
                aws_secret_access_key=creds["secret_access_key"],
                aws_session_token=creds["session_token"],
                region_name=self.region,
                config=BotoConfig(retries={"max_attempts": 0}),
                verify=not self.endpoint.startswith("https://"))
            try:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
                return "ALLOW"
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDeniedException", exc.response["Error"]
                return "DENY"
        finally:
            self.mgmt.delete_role(self.account_id, role)
            self.mgmt.delete_user(self.account_id, user)

    def test_role_session_deny_without_mfa_blocks(self, table):
        # aws:MultiFactorAuthPresent=false is present for a role session, so the
        # Deny fires. Live: DENY.
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"},
            {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"Bool": {"aws:MultiFactorAuthPresent": "false"}}}]}
        assert self._role_session_get(table, policy) == "DENY"

    def test_role_session_mfa_present_null_false_grants(self, table):
        # The key is present for a role session, so Null false grants.
        policy = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"Null": {"aws:MultiFactorAuthPresent": "false"}}}]}
        assert self._role_session_get(table, policy) == "ALLOW"

    def test_role_session_require_mfa_true_denies(self, table):
        # The session is non-MFA (false), so a require-true guard does not grant.
        policy = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"Bool": {"aws:MultiFactorAuthPresent": "true"}}}]}
        assert self._role_session_get(table, policy) == "DENY"

    def test_user_deny_without_mfa_is_inert(self, table):
        # An IAM user leaves aws:MultiFactorAuthPresent absent, so the Deny on
        # Bool=false is inert and the request is allowed. Live: ALLOW.
        assert self._get(table, self._allow_all_deny_get_if(
            {"Bool": {"aws:MultiFactorAuthPresent": "false"}})) == "ALLOW"

    def test_user_mfa_absent_null_true_grants(self, table):
        # The key is absent for an IAM user, so Null true grants.
        assert self._get(table, self._allow_get_if(
            {"Null": {"aws:MultiFactorAuthPresent": "true"}})) == "ALLOW"


class TestMalformedCidrRejection:
    """A malformed CIDR in an IpAddress/NotIpAddress condition is rejected
    at identity-policy and permissions-boundary write, matching the live service
    (MalformedPolicyDocument). The trust path accepts it (see
    test_auth_integration)."""

    MALFORMED = ["10.0.0.0/33", "999.0.0.0/8", "not-a-cidr", "10.0.0.0/"]

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.mgmt = mgmt
        self.account_id = account_id

    @staticmethod
    def _policy(cidr, op="IpAddress"):
        return {"Version": "2012-10-17", "Statement": [{
            "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {op: {"aws:SourceIp": cidr}}}]}

    def test_put_user_policy_rejects_malformed_cidr(self):
        user = f"cidr-u-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        try:
            for cidr in self.MALFORMED:
                resp = self.mgmt.put_user_policy(self.account_id, user, "p", self._policy(cidr))
                assert resp.status_code == 400, f"{cidr}: {resp.status_code} {resp.text}"
            # NotIpAddress too.
            resp = self.mgmt.put_user_policy(
                self.account_id, user, "p", self._policy("10.0.0.0/33", "NotIpAddress"))
            assert resp.status_code == 400, resp.text
            # A well-formed CIDR is accepted.
            resp = self.mgmt.put_user_policy(self.account_id, user, "p", self._policy("10.0.0.0/8"))
            assert resp.status_code == 204, resp.text
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_put_role_policy_rejects_malformed_cidr(self):
        role = f"cidr-r-{uuid.uuid4().hex[:8]}"
        trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{self.account_id}:root"},
            "Action": "sts:AssumeRole"}]}
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        try:
            resp = self.mgmt.put_role_policy(
                self.account_id, role, "p", self._policy("10.0.0.0/33"))
            assert resp.status_code == 400, resp.text
        finally:
            self.mgmt.delete_role(self.account_id, role)

    def test_set_user_boundary_rejects_malformed_cidr(self):
        user = f"cidr-b-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        try:
            resp = self.mgmt.set_user_boundary(
                self.account_id, user, self._policy("not-a-cidr"))
            assert resp.status_code == 400, resp.text
        finally:
            self.mgmt.delete_user(self.account_id, user)


# ---------------------------------------------------------------------------
# Backups carry no resource tags
# ---------------------------------------------------------------------------

class TestBackupResourceTags:
    """A caller allowed TagResource cannot satisfy a tag condition on a backup.

    DescribeBackup, DeleteBackup and RestoreTableFromBackup are authorized on
    the backup ARN. The service does not tag backups (TagResource takes table
    and stream ARNs; the backup resource has no ResourceTag condition key), so
    TagResource, UntagResource and ListTagsOfResource refuse a backup ARN, and
    an ``aws:ResourceTag`` condition never matches a backup.
    """

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _code(self, call) -> str:
        with pytest.raises(ClientError) as exc_info:
            call()
        return exc_info.value.response["Error"]["Code"]

    def test_tagging_a_backup_is_refused_and_grants_nothing(self):
        user = f"bkp-tag-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        admin = f"adm-{uuid.uuid4().hex[:8]}"
        admin_ak, admin_sk = _create_user_with_key(
            self.mgmt, self.account_id, admin
        )
        resp = self.mgmt.put_user_policy(
            self.account_id, admin, "full",
            {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Action": "dynamodb:*", "Resource": "*",
            }]},
        )
        assert resp.status_code == 204
        # A policy that grants DescribeBackup only on resources tagged
        # classification=dev, and the tagging actions on table/*.
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "bkp-tag-policy",
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["dynamodb:DescribeBackup",
                                   "dynamodb:DescribeTable"],
                        "Resource": "*",
                        "Condition": {"StringEquals": {
                            "aws:ResourceTag/classification": "dev",
                        }},
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["dynamodb:TagResource",
                                   "dynamodb:UntagResource",
                                   "dynamodb:ListTagsOfResource"],
                        "Resource": "arn:aws:dynamodb:*:*:table/*",
                    },
                ],
            },
        )
        assert resp.status_code == 204

        admin_client = _make_client(
            self.endpoint, admin_ak, admin_sk, self.region
        )
        user_client = _make_client(self.endpoint, ak, sk, self.region)
        table = f"abac-bkp-{uuid.uuid4().hex[:8]}"
        backup_arn = None
        dev = [{"Key": "classification", "Value": "dev"}]
        try:
            admin_client.create_table(
                TableName=table,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"}
                ],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(admin_client, table)
            table_arn = admin_client.describe_table(
                TableName=table)["Table"]["TableArn"]
            backup_arn = admin_client.create_backup(
                TableName=table, BackupName="abac-backup",
            )["BackupDetails"]["BackupArn"]

            def describe_backup():
                return user_client.describe_backup(BackupArn=backup_arn)

            assert self._code(describe_backup) == "AccessDeniedException"

            # The step that used to succeed and turn the next call into Allow.
            assert self._code(lambda: user_client.tag_resource(
                ResourceArn=backup_arn, Tags=dev,
            )) == "ValidationException"
            assert self._code(describe_backup) == "AccessDeniedException"

            # The other tagging operations refuse the backup ARN too, and so
            # does a caller allowed dynamodb:* (nothing is stored).
            assert self._code(lambda: user_client.untag_resource(
                ResourceArn=backup_arn, TagKeys=["classification"],
            )) == "ValidationException"
            assert self._code(lambda: user_client.list_tags_of_resource(
                ResourceArn=backup_arn,
            )) == "ValidationException"
            assert self._code(lambda: admin_client.tag_resource(
                ResourceArn=backup_arn, Tags=dev,
            )) == "ValidationException"
            assert self._code(describe_backup) == "AccessDeniedException"

            # Liveness: the same user and policy can tag the table, and the
            # tag condition then allows DescribeTable on it. A backup does not
            # take its table's tags.
            assert self._code(lambda: user_client.describe_table(
                TableName=table)) == "AccessDeniedException"
            user_client.tag_resource(ResourceArn=table_arn, Tags=dev)
            assert user_client.list_tags_of_resource(
                ResourceArn=table_arn)["Tags"] == dev
            user_client.describe_table(TableName=table)
            assert self._code(describe_backup) == "AccessDeniedException"
            # And the admin can still read the backup it made.
            assert admin_client.describe_backup(BackupArn=backup_arn)[
                "BackupDescription"]["BackupDetails"]["BackupArn"] == backup_arn
        finally:
            if backup_arn is not None:
                try:
                    admin_client.delete_backup(BackupArn=backup_arn)
                except Exception:
                    pass
            try:
                admin_client.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(admin_client, table)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, admin)
