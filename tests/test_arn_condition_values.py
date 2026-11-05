# SPDX-License-Identifier: Apache-2.0

"""Arn condition values at policy write, and short values on the request path.

Measured on live IAM on 2026-10-30; the rows are in the description of the
pull request for issue #665.

- Identity policies (CreatePolicy, PutRolePolicy) reject an Arn condition value
  whose first ``:`` field does not match ``arn``, whose partition or service
  field is empty, or whose service, region or account field holds ``/`` or
  ``${``, with MalformedPolicyDocument "The policy failed legacy parsing".
  They accepted the values in IDENTITY_ACCEPTED, including values shorter
  than an ARN.
- Role trust policies (CreateRole, UpdateAssumeRolePolicy) reject a value that
  does not split into six ``:`` fields, a JSON number or boolean included,
  with a message naming the value. They accepted every six-field value tried,
  an empty partition included.
- Requests from a role session showed how a value shorter than an ARN
  matches the role's ARN: ``arn:aws:iam`` and ``arn:aws:iam:*`` matched, and
  ``arn:aws:ia``, ``arn``, a bare ``*`` and ``arn:aws:dynamodb:*`` did not.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import contextlib
import json
import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from management_helpers import ManagementClient
from test_console_integration import ConsoleClient

IDENTITY_ERROR = "MalformedPolicyDocument: The policy failed legacy parsing"

OPERATORS = ["ArnEquals", "ArnNotEquals", "ArnLike", "ArnNotLike",
             "ForAnyValue:ArnLike", "ArnNotLikeIfExists"]

IDENTITY_REJECTED = [
    "",
    "foo:aws:iam::123456789012:user/alice",
    "ARN:aws:iam::123456789012:user/alice",
    "b*",
    "arn::dynamodb:us-east-1:123456789012:table/t",
    "arn:aws::us-east-1:123456789012:table/t",
    "arn:*:role/extenddb-test-665-e1",
    "arn:aws:iam:role/extenddb-test-665-e1",
    "arn:aws:iam:*:role/extenddb-test-665-e1",
    "arn:aws:iam::${aws:PrincipalAccount}:user/alice",
]

IDENTITY_ACCEPTED = [
    "*", "a*", "*:*", "arn", "arn:*", "arn:aws:iam", "arn:aws:iam:*",
    "arn:aws:dynamodb:*", "arn:aws:dynamodb:us-east-1:123456789012",
    "arn:aws:dynamodb:us-east-1:123456789012:", "*:*:*:*:*:*",
    "arn:aws:dynamodb:us-east-1:123456789012:table/a:b",
    "arn:aws:iam::123456789012:user/${aws:username}",
]

# A JSON number or boolean is read as its text, which never has six fields.
TRUST_REJECTED: list[Any] = ["*", "", "arn:aws:dynamodb:us-east-1:123456789012",
                             "arn:aws:iam:*", 123, True, 1.5]

TRUST_ACCEPTED = [
    "arn:aws:iam::123456789012:user/alice",
    "arn::dynamodb:us-east-1:123456789012:table/t",
    "*:*:*:*:*:*",
    "arn:aws:dynamodb:us-east-1:123456789012:",
]


def _trust_error(value: Any) -> str:
    # The service's text holds backslash escapes, not a newline and a quote,
    # and shows a number or boolean as its JSON text.
    text = value if isinstance(value, str) else json.dumps(value)
    return ("MalformedPolicyDocument: value used for ARN condition is incorrect "
            'type: STRING\\nstring: \\"' + text + '\\"\\n')


def _policy(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def _stmt(effect: str, op: str | None = None, value: str | None = None) -> dict:
    s: dict[str, Any] = {"Effect": effect, "Action": "dynamodb:ListTables", "Resource": "*"}
    if op is not None:
        s["Condition"] = {op: {"aws:PrincipalArn": value}}
    return s


def _trust(op: str, value: Any) -> dict:
    return _policy({"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole",
                    "Condition": {op: {"aws:PrincipalArn": value}}})


@pytest.fixture(scope="module")
def auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail("MISCONFIGURED: these tests require EXTENDDB_TEST_ENDPOINT, "
                    "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD.")
    return endpoint, admin_user, admin_pass


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    return ManagementClient(*auth_env)


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"arnval-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    resp = mgmt.delete_account(acct_id)
    assert resp.status_code == 204, resp.text


class TestArnValueWrites:
    @pytest.fixture(autouse=True)
    def setup(self, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id
        with contextlib.ExitStack() as cleanup:
            self.cleanup = cleanup
            yield

    def _name(self, prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _user(self) -> str:
        user = self._name("arnv-u")
        assert self.mgmt.create_user(self.account_id, user, "TestPass123!").status_code == 201
        self.cleanup.callback(self.mgmt.delete_user, self.account_id, user)
        return user

    def _role(self) -> str:
        role = self._name("arnv-r")
        trust = _policy({"Effect": "Allow", "Principal": {"AWS": "*"},
                         "Action": "sts:AssumeRole"})
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        self.cleanup.callback(self.mgmt.delete_role, self.account_id, role)
        return role

    @staticmethod
    def _rejected(resp, label: str) -> None:
        assert resp.status_code == 400, f"{label}: {resp.status_code} {resp.text}"
        assert IDENTITY_ERROR in resp.text, f"{label}: {resp.text}"

    def test_put_user_policy_matches_the_service(self):
        user = self._user()
        for value in IDENTITY_REJECTED:
            for op in OPERATORS:
                doc = _policy(_stmt("Allow", op, value))
                resp = self.mgmt.put_user_policy(self.account_id, user, "p", doc)
                self._rejected(resp, f"{op} {value!r}")
        # A list holding one rejected value is rejected too.
        doc = _policy(_stmt("Allow", "ArnNotEquals", None))
        doc["Statement"][0]["Condition"]["ArnNotEquals"]["aws:PrincipalArn"] = [
            "arn:aws:iam::123456789012:user/alice", ""]
        self._rejected(self.mgmt.put_user_policy(self.account_id, user, "p", doc), "list")
        for value in IDENTITY_ACCEPTED:
            doc = _policy(_stmt("Allow", "ArnNotEquals", value))
            resp = self.mgmt.put_user_policy(self.account_id, user, "p", doc)
            assert resp.status_code == 204, f"{value!r}: {resp.status_code} {resp.text}"

    def test_every_identity_write_path_rejects(self):
        bad = _policy(_stmt("Deny", "ArnEquals", "arn::dynamodb:us-east-1:123456789012:table/t"))
        user, role = self._user(), self._role()
        group = self._name("arnv-g")
        assert self.mgmt.create_group(self.account_id, group).status_code == 201
        self.cleanup.callback(self.mgmt.delete_group, self.account_id, group)
        self._rejected(self.mgmt.put_group_policy(self.account_id, group, "p", bad), "group")
        self._rejected(self.mgmt.put_role_policy(self.account_id, role, "p", bad), "role")
        self._rejected(self.mgmt.set_user_boundary(self.account_id, user, bad), "user boundary")
        self._rejected(self.mgmt.set_role_boundary(self.account_id, role, bad), "role boundary")

    def test_create_role_matches_the_service(self):
        for op in ("ArnEquals", "ArnNotEquals"):
            for value in TRUST_REJECTED:
                role = self._name("arnv-t")
                resp = self.mgmt.create_role(self.account_id, role, _trust(op, value))
                if resp.status_code == 201:
                    self.cleanup.callback(self.mgmt.delete_role, self.account_id, role)
                assert resp.status_code == 400, f"{op} {value!r}: {resp.status_code}"
                assert _trust_error(value) in resp.text, f"{op} {value!r}: {resp.text}"
        for value in TRUST_ACCEPTED:
            role = self._name("arnv-t")
            resp = self.mgmt.create_role(self.account_id, role, _trust("ArnEquals", value))
            assert resp.status_code == 201, f"{value!r}: {resp.status_code} {resp.text}"
            self.cleanup.callback(self.mgmt.delete_role, self.account_id, role)

    def test_console_writes_reject(self, auth_env):
        console = ConsoleClient(auth_env[0])
        assert console.login(auth_env[1], auth_env[2]).status_code == 303
        user = self._user()
        doc = json.dumps(_policy(_stmt("Allow", "ArnNotEquals", "")))
        resp = console.post(f"/accounts/{self.account_id}/users/{user}/policies/new",
                            {"policy_name": "p", "policy_document": doc})
        assert resp.status_code == 200 and IDENTITY_ERROR in resp.text, resp.text
        role = self._name("arnv-c")
        resp = console.post(f"/accounts/{self.account_id}/roles/new", {
            "role_name": role, "trust_policy": json.dumps(_trust("ArnEquals", "*"))})
        if resp.status_code == 303:
            self.cleanup.callback(self.mgmt.delete_role, self.account_id, role)
        assert resp.status_code == 200, resp.status_code
        assert "value used for ARN condition is incorrect type: STRING" in resp.text


class TestShortValuesOnTheRequestPath:
    """ListTables by an IAM user whose only policy is rewritten per case.

    Each value below was evaluated by the live service for a role session, and
    the user's ARN has the same fields up to the resource."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id):
        self.mgmt = mgmt
        self.account_id = account_id
        self.user = f"arnv-q-{uuid.uuid4().hex[:8]}"
        assert mgmt.create_user(account_id, self.user, "TestPass123!").status_code == 201
        creds = mgmt.create_access_key(account_id, self.user).json()
        kwargs: dict = dict(
            service_name="dynamodb", endpoint_url=auth_env[0],
            aws_access_key_id=creds["access_key_id"],
            aws_secret_access_key=creds["secret_access_key"],
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=BotoConfig(retries={"max_attempts": 0}))
        if auth_env[0].startswith("https://"):
            kwargs["verify"] = False
        self.client = boto3.client(**kwargs)
        yield
        mgmt.delete_user(account_id, self.user)

    def _allowed(self, policy: dict) -> bool:
        resp = self.mgmt.put_user_policy(self.account_id, self.user, "p", policy)
        assert resp.status_code == 204, resp.text
        try:
            self.client.list_tables()
            return True
        except ClientError as e:
            assert e.response["Error"]["Code"] == "AccessDeniedException", e.response
            return False

    def test_short_values_match_as_the_service_does(self):
        acct = self.account_id
        allow_all = _stmt("Allow")
        cases = [
            (_policy(_stmt("Allow", "ArnNotEquals", "arn:aws:iam:*")), False),
            (_policy(_stmt("Allow", "ArnNotLike", "arn:aws:iam:*")), False),
            (_policy(_stmt("Allow", "ArnNotEquals", "arn:aws:dynamodb:*")), True),
            (_policy(_stmt("Allow", "ArnEquals", "*")), False),
            (_policy(_stmt("Allow", "ArnNotEquals", "*")), True),
            (_policy(_stmt("Allow", "ArnEquals", "arn:aws:iam")), True),
            (_policy(_stmt("Allow", "ArnEquals", f"arn:aws:iam::{acct}")), True),
            (_policy(_stmt("Allow", "ArnEquals", "arn:aws:ia")), False),
            (_policy(_stmt("Allow", "ArnEquals", "arn")), False),
            (_policy(allow_all, _stmt("Deny", "ArnNotEquals", "*")), False),
            (_policy(allow_all, _stmt("Deny", "ArnEquals", "arn:aws:iam")), False),
            (_policy(allow_all, _stmt("Deny", "ArnEquals", "*")), True),
            (_policy(_stmt("Allow", "ArnEquals", f"arn:aws:iam::{acct}:user/arnv-q-*")), True),
        ]
        for policy, granted in cases:
            assert self._allowed(policy) == granted, json.dumps(policy)
