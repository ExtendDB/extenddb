# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Arn condition operators on a resolved ARN key (aws:PrincipalArn).

IAM evaluates ArnEquals as ArnLike and ArnNotEquals as ArnNotLike: `*` and
`?` match in every ARN field. Measured with `iam simulate-custom-policy` on
2026-10-23. aws:PrincipalArn is the ARN-valued key ExtendDB resolves on both
the DynamoDB request path (identity policies) and the AssumeRole trust path,
so a wildcarded ArnEquals on it must take effect on both.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

ALLOW = 201
DENY = 403


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: these tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD."
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
    resp = mgmt.create_account(acct_id, f"arncond-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


def _create_user_with_key(mgmt: ManagementClient, account_id: str,
                          user_name: str) -> tuple[str, str]:
    resp = mgmt.create_user(account_id, user_name, "TestPass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, user_name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return creds["access_key_id"], creds["secret_access_key"]


def _policy(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


ALLOW_ALL = {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}


class TestIdentityPolicyArnEqualsWildcard:
    """Identity policies conditioned on aws:PrincipalArn with wildcards."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _admin_table(self) -> tuple[Any, str]:
        admin = f"arn-adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, admin)
        resp = self.mgmt.put_user_policy(self.account_id, admin, "full", _policy(ALLOW_ALL))
        assert resp.status_code == 204
        client = _make_client(self.endpoint, ak, sk, self.region)
        table = f"arnc-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table)
        client.put_item(TableName=table, Item={"pk": {"S": "k1"}, "data": {"S": "v1"}})
        return client, table

    def _user(self, prefix: str, policy: dict) -> tuple[str, Any]:
        user = f"{prefix}-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        resp = self.mgmt.put_user_policy(self.account_id, user, "p", policy)
        assert resp.status_code == 204, resp.text
        return user, _make_client(self.endpoint, ak, sk, self.region)

    def _get_denied(self, client: Any, table: str) -> None:
        with pytest.raises(ClientError) as exc_info:
            client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
        assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"

    def _get_ok(self, client: Any, table: str) -> None:
        out = client.get_item(TableName=table, Key={"pk": {"S": "k1"}})
        assert out["Item"]["data"]["S"] == "v1"

    def test_deny_arn_equals_wildcard_blocks(self):
        admin_client, table = self._admin_table()
        user_prefix = f"arn-deny-{uuid.uuid4().hex[:6]}"
        pattern = f"arn:aws:iam::{self.account_id}:user/{user_prefix}*"
        user, client = self._user(user_prefix, _policy(
            ALLOW_ALL,
            {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
             "Condition": {"ArnEquals": {"aws:PrincipalArn": pattern}}}))
        try:
            # The caller's ARN matches the wildcard, so the Deny applies to
            # GetItem; PutItem is outside the Deny and still allowed.
            self._get_denied(client, table)
            client.put_item(TableName=table, Item={"pk": {"S": "k2"}})
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_deny_arn_equals_question_mark_blocks(self):
        admin_client, table = self._admin_table()
        user_prefix = f"arn-q-{uuid.uuid4().hex[:6]}"
        user, client = self._user(user_prefix, _policy(ALLOW_ALL, {
            "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnEquals": {"aws:PrincipalArn": "arn:aws:iam::" + "?" * 12 + ":user/*"}}}))
        try:
            self._get_denied(client, table)
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_deny_arn_equals_other_wildcard_is_inert(self):
        admin_client, table = self._admin_table()
        user, client = self._user("arn-inert", _policy(ALLOW_ALL, {
            "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/zzz-*"}}}))
        try:
            self._get_ok(client, table)
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_gated_on_arn_equals_wildcard_grants(self):
        admin_client, table = self._admin_table()
        user_prefix = f"arn-allow-{uuid.uuid4().hex[:6]}"
        user, client = self._user(user_prefix, _policy({
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/{user_prefix}*"}}}))
        try:
            self._get_ok(client, table)
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_gated_on_arn_equals_wildcard_case_mismatch_does_not_grant(self):
        admin_client, table = self._admin_table()
        user_prefix = f"arn-case-{uuid.uuid4().hex[:6]}"
        user, client = self._user(user_prefix, _policy({
            "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/{user_prefix.upper()}*"}}}))
        try:
            self._get_denied(client, table)
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)

    def test_deny_arn_not_equals_wildcard(self):
        # ArnNotEquals against a wildcard the caller matches is false, so the
        # Deny is inert; against a wildcard the caller does not match it holds.
        admin_client, table = self._admin_table()
        user_prefix = f"arn-ne-{uuid.uuid4().hex[:6]}"
        user, client = self._user(user_prefix, _policy(ALLOW_ALL, {
            "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnNotEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/{user_prefix}*"}}}))
        other, other_client = self._user("arn-ne-other", _policy(ALLOW_ALL, {
            "Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
            "Condition": {"ArnNotEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/{user_prefix}*"}}}))
        try:
            self._get_ok(client, table)
            self._get_denied(other_client, table)
        finally:
            admin_client.delete_table(TableName=table)
            self.mgmt.delete_user(self.account_id, user)
            self.mgmt.delete_user(self.account_id, other)


class TestTrustPolicyArnEqualsWildcard:
    """Trust policies conditioned on aws:PrincipalArn with wildcards."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.mgmt = mgmt
        self.account_id = account_id

    def _caller(self) -> tuple[str, str]:
        user = f"arn-tc-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, user, "TestPass123!")
        assert resp.status_code == 201, resp.text
        return user, f"arn:aws:iam::{self.account_id}:user/{user}"

    def _assume(self, trust: dict, caller_arn: str) -> int:
        role = f"arn-tr-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_role(self.account_id, role, trust)
        assert resp.status_code == 201, f"create_role: {resp.status_code} {resp.text}"
        try:
            return self.mgmt.assume_role(
                self.account_id, role, caller_arn, "arn-session").status_code
        finally:
            self.mgmt.delete_role(self.account_id, role)

    @staticmethod
    def _stmt(effect: str, principal, condition=None) -> dict:
        s: dict[str, Any] = {"Effect": effect, "Principal": principal, "Action": "sts:AssumeRole"}
        if condition is not None:
            s["Condition"] = condition
        return s

    def test_deny_arn_equals_wildcard_blocks_assume(self):
        user, arn = self._caller()
        try:
            trust = _policy(
                self._stmt("Allow", {"AWS": arn}),
                self._stmt("Deny", {"AWS": "*"}, {"ArnEquals": {
                    "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/arn-tc-*"}}),
            )
            assert self._assume(trust, arn) == DENY
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_gated_on_arn_equals_wildcard_grants_assume(self):
        user, arn = self._caller()
        try:
            trust = _policy(self._stmt("Allow", {"AWS": "*"}, {"ArnEquals": {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/arn-tc-*"}}))
            assert self._assume(trust, arn) == ALLOW
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_deny_arn_not_equals_wildcard_on_matching_caller_is_inert(self):
        user, arn = self._caller()
        try:
            trust = _policy(
                self._stmt("Allow", {"AWS": arn}),
                self._stmt("Deny", {"AWS": "*"}, {"ArnNotEquals": {
                    "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/arn-tc-*"}}),
            )
            assert self._assume(trust, arn) == ALLOW
        finally:
            self.mgmt.delete_user(self.account_id, user)

    def test_allow_gated_on_arn_not_equals_does_not_grant_non_arn_caller(self):
        """IAM: a context value that is not an ARN matches neither ArnNotEquals
        nor ArnNotLike (simulate-custom-policy on 2026-10-28, values
        ``notanarn`` and ``arn:aws:dynamodb`` against wildcard patterns such
        as ``table/*``: implicitDeny for all four Arn operators; the rows are
        in the description of PR #471). ``arn:aws:iam`` here has the same
        three-field shape as the measured ``arn:aws:dynamodb``. The AssumeRole
        caller ARN is a request field, so a non-ARN value reaches the trust
        evaluator here."""
        for op in ("ArnNotEquals", "ArnNotLike"):
            trust = _policy(self._stmt("Allow", {"AWS": "*"}, {op: {
                "aws:PrincipalArn": f"arn:aws:iam::{self.account_id}:user/zzz-*"}}))
            assert self._assume(trust, "notanarn") == DENY, op
            assert self._assume(trust, "arn:aws:iam") == DENY, op
