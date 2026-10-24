# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""An Allow gated on a bare negated operator over a multivalued key.

IAM matches a bare negated operator (`StringNotEquals`, `StringNotLike`,
`StringNotEqualsIgnoreCase`) on an absent multivalued key and never on a
present one (`wave-2026-10-23/probes/B19-2.out` group G, `H19-1.out` groups A,
B and I, `iam simulate-custom-policy`, 2026-10-24). ExtendDB applies the
absent-key answer to `aws:TagKeys` alone, which it populates on exactly the
operations the AWS service authorization reference lists it for. It leaves
`dynamodb:Attributes` absent on writes, on `AttributesToGet` reads and on batch
and transaction targets, where the service populates the key and answers
implicitDeny for a bare operator, so for that key an absent key does not match
and such an Allow grants nothing on those operations.

The same statement is checked as a user policy, a role policy, a permissions
boundary and a session policy, and as the only statement of a trust policy.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active
from test_tag_request_conditions import (
    ALLOW_ALL,
    _error_code,
    _make_client,
    _require_auth_env,
    _user_with_key,
)
from management_helpers import ManagementClient

PRINCIPAL_KINDS = ["user", "role_session", "permissions_boundary", "session_policy"]


def _allow_unless(cond: dict) -> dict:
    """A policy whose only grant is `dynamodb:*` gated on `cond`."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*", "Condition": cond}
        ],
    }


ALLOW_UNLESS_SSN = _allow_unless({"StringNotEquals": {"dynamodb:Attributes": "ssn"}})
ALLOW_UNLESS_SECRET_TAG = _allow_unless({"StringNotEquals": {"aws:TagKeys": "secret"}})


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def region() -> str:
    import os

    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"reqtag-allow-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


def _ssn_operations(table: str) -> dict[str, Callable[[Any], Any]]:
    """Each operation names the attribute `ssn`, on a key of its own."""
    key = {"pk": {"S": f"k-{uuid.uuid4().hex[:8]}"}}
    return {
        "put_item": lambda c: c.put_item(TableName=table, Item={**key, "ssn": {"N": "1"}}),
        "update_item": lambda c: c.update_item(
            TableName=table,
            Key=key,
            UpdateExpression="SET ssn = :v",
            ExpressionAttributeValues={":v": {"N": "1"}},
        ),
        "delete_item": lambda c: c.delete_item(TableName=table, Key=key),
        "get_item_attributes_to_get": lambda c: c.get_item(
            TableName=table, Key=key, AttributesToGet=["pk", "ssn"]
        ),
        "batch_write_item": lambda c: c.batch_write_item(
            RequestItems={table: [{"PutRequest": {"Item": {**key, "ssn": {"N": "1"}}}}]}
        ),
        "transact_write_items": lambda c: c.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": table,
                        "Key": key,
                        "UpdateExpression": "SET ssn = :v",
                        "ExpressionAttributeValues": {":v": {"N": "1"}},
                    }
                }
            ]
        ),
    }


class TestBareNegatedAllow:
    """An Allow gated on a bare negated operator, on four policy types."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self.admin_name = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, self.admin_name)
        resp = self.mgmt.put_user_policy(self.account_id, self.admin_name, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        self.admin_client = _make_client(self.endpoint, ak, sk, self.region)
        self.caller_arn = f"arn:aws:iam::{self.account_id}:user/{self.admin_name}"
        self.table = f"rta-{uuid.uuid4().hex[:8]}"
        self.admin_client.create_table(
            TableName=self.table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(self.admin_client, self.table)
        self.arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{self.table}"
        self.roles: list[str] = []
        yield
        for role in self.roles:
            self.mgmt.delete_role(self.account_id, role)
        try:
            self.admin_client.delete_table(TableName=self.table)
        except ClientError:
            pass
        self.mgmt.delete_user(self.account_id, self.admin_name)

    def _trust_for_caller(self) -> dict:
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self.caller_arn},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    def _role(self, trust: dict) -> str:
        role = f"r-{uuid.uuid4().hex[:8]}"
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        self.roles.append(role)
        return role

    def _session_client(self, role: str, **assume_kwargs) -> Any:
        resp = self.mgmt.assume_role(
            self.account_id, role, self.caller_arn, "sess", **assume_kwargs
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        return _make_client(
            self.endpoint,
            creds["access_key_id"],
            creds["secret_access_key"],
            self.region,
            session_token=creds["session_token"],
        )

    def _client_holding(self, kind: str, policy: dict) -> Any:
        """A client whose effective permissions are `policy`, carried as `kind`."""
        if kind == "user":
            name = f"u-{uuid.uuid4().hex[:8]}"
            ak, sk = _user_with_key(self.mgmt, self.account_id, name)
            assert self.mgmt.put_user_policy(self.account_id, name, "p", policy).status_code == 204
            return _make_client(self.endpoint, ak, sk, self.region)
        if kind == "permissions_boundary":
            name = f"ub-{uuid.uuid4().hex[:8]}"
            ak, sk = _user_with_key(self.mgmt, self.account_id, name)
            assert (
                self.mgmt.put_user_policy(self.account_id, name, "p", ALLOW_ALL).status_code
                == 204
            )
            assert self.mgmt.set_user_boundary(self.account_id, name, policy).status_code == 204
            return _make_client(self.endpoint, ak, sk, self.region)
        if kind == "role_session":
            role = self._role(self._trust_for_caller())
            assert self.mgmt.put_role_policy(self.account_id, role, "p", policy).status_code == 204
            return self._session_client(role)
        if kind == "session_policy":
            role = self._role(self._trust_for_caller())
            assert (
                self.mgmt.put_role_policy(self.account_id, role, "full", ALLOW_ALL).status_code
                == 204
            )
            return self._session_client(role, session_policy=policy)
        raise AssertionError(kind)

    def _assert_denied(self, call: Callable[[], Any]) -> None:
        with pytest.raises(ClientError) as exc:
            call()
        assert _error_code(exc.value) == "AccessDeniedException", exc.value.response

    # -- dynamodb:Attributes: the Allow grants none of the operations --------

    @pytest.mark.parametrize("kind", PRINCIPAL_KINDS)
    @pytest.mark.parametrize("operation", sorted(_ssn_operations("t")))
    def test_allow_unless_ssn_does_not_grant(self, kind: str, operation: str):
        # H19-1 a_allow_attr_putitem_ne_present_named,
        # a_allow_attr_updateitem_ne_present_named and
        # i_role_allow_attr_put_ne_present_named: implicitDeny. The service
        # populates dynamodb:Attributes on these operations, and a bare operator
        # on the present key does not match, so the Allow grants nothing.
        client = self._client_holding(kind, ALLOW_UNLESS_SSN)
        self._assert_denied(lambda: _ssn_operations(self.table)[operation](client))
        scan = self.admin_client.scan(TableName=self.table)
        assert scan["Count"] == 0, "a refused write stores nothing"

    # -- aws:TagKeys: the Allow grants an operation with no tag set ---------

    @pytest.mark.parametrize("kind", PRINCIPAL_KINDS)
    def test_allow_unless_secret_tag_grants_an_untagged_operation(self, kind: str):
        # B19-2 G-aws-TagKeys-bare-StringNotEquals-absent (the Deny fires) and
        # H19-1 a_allow_tagkeys_tagresource_ne_absent, i_role_allow_tagkeys_*
        # (the Allow grants): an absent aws:TagKeys matches a bare negated
        # operator. GetItem carries no tag set.
        client = self._client_holding(kind, ALLOW_UNLESS_SECRET_TAG)
        client.get_item(TableName=self.table, Key={"pk": {"S": "k"}})

    @pytest.mark.parametrize("kind", PRINCIPAL_KINDS)
    def test_allow_unless_secret_tag_does_not_grant_a_tagged_request(self, kind: str):
        # H19-1 a_allow_tagkeys_tagresource_ne_present_named and
        # _present_other: implicitDeny. A bare operator on a present multivalued
        # key does not match, whichever keys the request names.
        client = self._client_holding(kind, ALLOW_UNLESS_SECRET_TAG)
        for tags in (
            [{"Key": "secret", "Value": "x"}],
            [{"Key": "colour", "Value": "red"}],
        ):
            self._assert_denied(lambda: client.tag_resource(ResourceArn=self.arn, Tags=tags))
        resp = self.admin_client.list_tags_of_resource(ResourceArn=self.arn)
        assert resp.get("Tags", []) == [], "a refused TagResource stores nothing"

    # -- the trust path keeps its own rule ----------------------------------

    def test_trust_policy_bare_negated_tagkeys_refuses_an_untagged_assume_role(self):
        # The AssumeRole trust context does not take the absent-key answer for
        # aws:TagKeys: with no session tags the Allow does not match and the
        # call is refused, as before this change.
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self.caller_arn},
                    "Action": ["sts:AssumeRole", "sts:TagSession"],
                    "Condition": {"StringNotEquals": {"aws:TagKeys": "env"}},
                }
            ],
        }
        role = self._role(trust)
        resp = self.mgmt.assume_role(self.account_id, role, self.caller_arn, "sess")
        assert resp.status_code == 403, resp.text
        resp = self.mgmt.assume_role(
            self.account_id, role, self.caller_arn, "sess", session_tags={"env": "prod"}
        )
        assert resp.status_code == 403, resp.text
