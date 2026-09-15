# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Fine-grained access control tests for the multivalued key dynamodb:Attributes.

Reproduces BR-7085 end-to-end. The multivalued condition key dynamodb:Attributes
is populated from a request's ProjectionExpression. Verified against real AWS IAM:

  * A BARE operator (no ForAllValues:/ForAnyValue: qualifier) on a multivalued key
    NEVER matches. So a Deny using bare StringEquals on dynamodb:Attributes is a
    no-op: every projection is allowed, including the "denied" attribute on its own.
    (Real AWS returns the attribute; extenddb must match this, not deny it.)

  * The CORRECT column-level denylist uses ForAnyValue:StringEquals, which fires
    whenever any requested attribute is in the denied set — alone or alongside
    another attribute, and whether named directly or via ExpressionAttributeNames.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_fgac_attributes.py -v

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
            "MISCONFIGURED: FGAC tests require EXTENDDB_TEST_ENDPOINT, "
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
    # Self-signed certs from ``extenddb init`` — disable SSL verification.
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
    resp = mgmt.create_account(acct_id, f"fgac-test-{acct_id}")
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


ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}


class TestAttributesFgac:
    """Column-level FGAC via dynamodb:Attributes (multivalued key)."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region

    def _make_table_with_item(self) -> tuple[str, Any]:
        """Create an owner (full access), a table, and one item. Returns
        (table_name, owner_client) so the caller can tear the table down."""
        owner = f"owner-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, owner)
        resp = self.mgmt.put_user_policy(
            self.account_id, owner, "full", ALLOW_ALL
        )
        assert resp.status_code == 204, resp.text
        owner_client = _make_client(self.endpoint, ak, sk, self.region)

        table = f"emp-{uuid.uuid4().hex[:8]}"
        owner_client.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(owner_client, table)
        owner_client.put_item(
            TableName=table,
            Item={
                "pk": {"S": "emp1"},
                "ssn": {"S": "123-45-6789"},
                "fullname": {"S": "Alice Smith"},
            },
        )
        return table, owner_client

    def _restricted_client(self, table: str, deny_policy: dict) -> Any:
        user = f"restricted-{uuid.uuid4().hex[:8]}"
        ak, sk = _create_user_with_key(self.mgmt, self.account_id, user)
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "AllowAll", ALLOW_ALL
        )
        assert resp.status_code == 204, resp.text
        resp = self.mgmt.put_user_policy(
            self.account_id, user, "DenySSN", deny_policy
        )
        assert resp.status_code == 204, resp.text
        return _make_client(self.endpoint, ak, sk, self.region)

    def _deny(self, table: str, qualifier: str) -> dict:
        arn = (f"arn:aws:dynamodb:{self.region}:{self.account_id}"
               f":table/{table}")
        op = f"{qualifier}:StringEquals" if qualifier else "StringEquals"
        return {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Action": "dynamodb:*",
                "Resource": arn,
                "Condition": {op: {"dynamodb:Attributes": ["ssn"]}},
            }],
        }

    def test_bare_string_equals_deny_is_a_noop(self):
        """A bare-StringEquals Deny on a multivalued key never fires (AWS parity).

        All three projections succeed, including "ssn" on its own — the bare Deny
        provides no protection, exactly as real AWS DynamoDB behaves.
        """
        table, owner_client = self._make_table_with_item()
        try:
            client = self._restricted_client(table, self._deny(table, ""))

            # ssn alone -> ALLOWED (the crux: extenddb must NOT deny this)
            resp = client.get_item(
                TableName=table, Key={"pk": {"S": "emp1"}},
                ProjectionExpression="ssn",
            )
            assert resp["Item"]["ssn"]["S"] == "123-45-6789"

            # ssn + fullname -> both returned
            resp = client.get_item(
                TableName=table, Key={"pk": {"S": "emp1"}},
                ProjectionExpression="ssn, fullname",
            )
            assert resp["Item"]["ssn"]["S"] == "123-45-6789"
            assert resp["Item"]["fullname"]["S"] == "Alice Smith"

            # fullname alone -> returned
            resp = client.get_item(
                TableName=table, Key={"pk": {"S": "emp1"}},
                ProjectionExpression="fullname",
            )
            assert resp["Item"]["fullname"]["S"] == "Alice Smith"
            assert "ssn" not in resp["Item"]
        finally:
            owner_client.delete_table(TableName=table)
            wait_for_deleted(owner_client, table)

    def test_for_any_value_deny_blocks_ssn(self):
        """The correct denylist (ForAnyValue:StringEquals) blocks ssn everywhere."""
        table, owner_client = self._make_table_with_item()
        try:
            client = self._restricted_client(
                table, self._deny(table, "ForAnyValue")
            )

            # ssn alone -> denied
            with pytest.raises(ClientError) as exc:
                client.get_item(
                    TableName=table, Key={"pk": {"S": "emp1"}},
                    ProjectionExpression="ssn",
                )
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

            # ssn + fullname -> denied (any requested attr is ssn)
            with pytest.raises(ClientError) as exc:
                client.get_item(
                    TableName=table, Key={"pk": {"S": "emp1"}},
                    ProjectionExpression="ssn, fullname",
                )
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

            # fullname alone -> allowed
            resp = client.get_item(
                TableName=table, Key={"pk": {"S": "emp1"}},
                ProjectionExpression="fullname",
            )
            assert resp["Item"]["fullname"]["S"] == "Alice Smith"
        finally:
            owner_client.delete_table(TableName=table)
            wait_for_deleted(owner_client, table)

    def test_for_any_value_deny_blocks_ssn_via_placeholder(self):
        """ExpressionAttributeNames placeholders resolve before evaluation, so the
        ForAnyValue denylist still blocks a placeholdered ssn projection."""
        table, owner_client = self._make_table_with_item()
        try:
            client = self._restricted_client(
                table, self._deny(table, "ForAnyValue")
            )
            with pytest.raises(ClientError) as exc:
                client.get_item(
                    TableName=table, Key={"pk": {"S": "emp1"}},
                    ProjectionExpression="#s, #f",
                    ExpressionAttributeNames={"#s": "ssn", "#f": "fullname"},
                )
            assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        finally:
            owner_client.delete_table(TableName=table)
            wait_for_deleted(owner_client, table)
