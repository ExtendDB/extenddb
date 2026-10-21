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
    """Policies with dynamodb:ResourceTag/* conditions."""

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
                            "dynamodb:ResourceTag/Env": "dev",
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
                            "dynamodb:ResourceTag/Env": "dev",
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


class TestUnresolvableGlobalKey:
    """Identity policies gated on a global key the service populates but
    ExtendDB cannot resolve (for example aws:PrincipalOrgID). Such a statement
    fails closed: a Deny blocks and an Allow does not grant, whatever the
    operator, so a negated operator cannot spuriously grant. A key ExtendDB does
    resolve (aws:PrincipalArn) evaluates normally.
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

    def test_deny_on_unresolvable_orgid_blocks_fail_closed(self):
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
            with pytest.raises(ClientError) as exc:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
            # The Deny names only GetItem, so PutItem still succeeds.
            client.put_item(TableName=table, Item={"pk": {"S": "k2"}, "data": {"S": "v2"}})
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_negated_allow_on_unresolvable_orgid_does_not_grant(self):
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
            with pytest.raises(ClientError) as exc:
                client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
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

    def test_unknown_key_negated_allow_fails_closed(self, table):
        # An unknown aws: key is unresolvable, so the Allow does not grant even
        # under a negated operator. ExtendDB denies here while live DynamoDB
        # allows (treating the key as absent); this is the deliberate
        # fail-closed over-denial recorded in the report's open questions.
        assert self._get(table, self._allow_get_if(
            {"StringNotEquals": {"aws:NoSuchGlobalKey": "x"}})) == "DENY"

    def test_unknown_key_deny_fails_closed(self, table):
        # A Deny gated on an unknown aws: key fails closed and blocks. Live
        # DynamoDB also denies here (the negated-absent rule applies the Deny).
        assert self._get(table, self._allow_all_deny_get_if(
            {"StringNotEquals": {"aws:NoSuchGlobalKey": "x"}})) == "DENY"
