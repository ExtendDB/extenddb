# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A policy write refuses a `${...}` reference that IAM refuses (issue #834).

IAM refuses such a policy at write with `MalformedPolicyDocument`, measured on
2026-11-09 with `PutUserPolicy`, `PutGroupPolicy`, `PutRolePolicy`, `CreateRole`
and `UpdateAssumeRolePolicy`, two samples each:

- identity policy, `Version` `2012-10-17`: `Syntax errors in policy.` for a
  condition value under any operator and for a `Resource` or `NotResource`
  pattern;
- identity policy, `Version` `2008-10-17`: `Variable declaration is not
  well-formed.` for a condition value; for a pattern, `Resource <pattern> must
  be in ARN format or "*".` when no `}` follows the `${` at all, and the
  variable message otherwise;
- trust policy, `Version` `2012-10-17`: `Policy contains one or more invalid
  variable declarations`;
- trust policy, `Version` `2008-10-17` or none: a string value is stored, and
  an Arn operator value with no `}` after its `${` is refused with `value used
  for ARN condition is incorrect type`.

After `${` the next character that is not whitespace belongs to the reference
text, `}` included; then a `'` opens or closes a quoted span, and a `}` inside
it does not close the reference. So `a${}}` and `a${aws:username, '}x'}` are
accepted and `a${aws:username, '}'` is refused. A closed reference whose text is
not an escape, a key name, or a key name with one quoted default (`a${}`,
`a${aws user}`, `a${aws:username, 'x' 'y'}`, `a${a$b, 'x'}`) is refused with
the same messages. The tests write through the management API and the console,
which provision builtin IAM principals, so they run against ExtendDB only.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_policy_variable_syntax.py -v
"""

from __future__ import annotations

import json
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

SYNTAX = "MalformedPolicyDocument: Syntax errors in policy."
NOT_WELL_FORMED = "MalformedPolicyDocument: Variable declaration is not well-formed."
TRUST_2012 = "MalformedPolicyDocument: Policy contains one or more invalid variable declarations"
TRUST_ARN = "MalformedPolicyDocument: value used for ARN condition is incorrect type: STRING"

UNTERMINATED = ["a${", "a$${", "${aws:username}${", "a${aws:username, '}'"]
MALFORMED = [
    "a${}",
    "a${aws user}",
    "a${aws:username, 'x' 'y'}",
    "a${a$b, 'x'}",
    "a${aws:username,\u0001'x'\u0001}",
]
INVALID = UNTERMINATED + MALFORMED
TERMINATED = [
    "a${}}",
    "a${aws:username}",
    "a${${aws:username}}",
    "a}",
    "a${aws:username, '}x'}",
    "a${aws:username, 'x''y'}",
    "a${ * }",
]
TABLE = "arn:aws:dynamodb:us-east-1:123456789012:table/blue/"
TAG_KEY = "aws:ResourceTag/owner"


def _condition_document(version: str, value: str, op: str = "StringEquals") -> dict:
    return {
        "Version": version,
        "Statement": [{
            "Effect": "Deny",
            "Action": "dynamodb:GetItem",
            "Resource": "*",
            "Condition": {op: {TAG_KEY: value}},
        }],
    }


def _resource_document(version: str, pattern: str) -> dict:
    return {
        "Version": version,
        "Statement": [{"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": pattern}],
    }


def _shown(value: str) -> str:
    """`value` as the refusal detail quotes it (Rust `Debug` for a string)."""
    out = []
    for c in value:
        if c in "\\\"":
            out.append("\\" + c)
        elif c == "\t":
            out.append("\\t")
        elif c == "\n":
            out.append("\\n")
        elif c == "\r":
            out.append("\\r")
        elif ord(c) < 0x20 or ord(c) == 0x7F:
            out.append(f"\\u{{{ord(c):x}}}")
        else:
            out.append(c)
    return '"' + "".join(out) + '"'


def _condition_error(message: str, value: str) -> str:
    return f"{message} (condition on {TAG_KEY} holds {_shown(value)})"


def _trust(account_id: str, version: str | None, op: str, key: str, value: str) -> dict:
    doc = {
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
            "Action": "sts:AssumeRole",
            "Condition": {op: {key: value}},
        }],
    }
    if version is not None:
        doc = {"Version": version, **doc}
    return doc


def _name() -> str:
    return f"vs-{uuid.uuid4().hex[:8]}"


def _role_names(mgmt: ManagementClient, account_id: str) -> list[str]:
    resp = mgmt.list_roles(account_id)
    assert resp.status_code == 200, resp.text
    return [e["role_name"] for e in resp.json()]


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
    resp = mgmt.create_account(acct_id, f"varsyntax-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


@pytest.fixture(scope="module")
def user(mgmt, account_id):
    name = f"vs-{uuid.uuid4().hex[:8]}"
    resp = mgmt.create_user(account_id, name, "TestPass123!")
    assert resp.status_code == 201, resp.text
    yield name
    mgmt.delete_user(account_id, name)


@pytest.fixture(scope="module")
def group(mgmt, account_id):
    name = f"vs-{uuid.uuid4().hex[:8]}"
    resp = mgmt.create_group(account_id, name)
    assert resp.status_code == 201, resp.text
    yield name
    mgmt.delete_group(account_id, name)


@pytest.fixture(scope="module")
def role(mgmt, account_id):
    name = f"vs-{uuid.uuid4().hex[:8]}"
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
def console(auth_env) -> ConsoleClient:
    client = ConsoleClient(auth_env[0])
    resp = client.login(auth_env[1], auth_env[2])
    assert resp.status_code == 303, f"Login failed: {resp.status_code}"
    return client


class TestIdentityWrite:
    @pytest.mark.parametrize("value", INVALID)
    @pytest.mark.parametrize("op", ["StringEquals", "StringLike"])
    @pytest.mark.parametrize(
        "version, message", [("2012-10-17", SYNTAX), ("2008-10-17", NOT_WELL_FORMED)]
    )
    @pytest.mark.parametrize("principal_type", ["user", "group", "role"])
    def test_policy_refuses_condition_value(
        self, request, mgmt, account_id, principal_type, version, message, op, value
    ):
        principal = request.getfixturevalue(principal_type)
        put = getattr(mgmt, f"put_{principal_type}_policy")
        listing = getattr(mgmt, f"list_{principal_type}_policies")
        name = _name()
        resp = put(account_id, principal, name, _condition_document(version, value, op))
        assert resp.status_code == 400, resp.text
        assert resp.text == (
            f"Invalid policy document: policy parse error: {_condition_error(message, value)}"
        )
        assert name not in [p["policy_name"] for p in listing(account_id, principal).json()]

    @pytest.mark.parametrize("value", INVALID)
    @pytest.mark.parametrize("op", ["ArnLike", "ArnEquals"])
    @pytest.mark.parametrize(
        "version, message", [("2012-10-17", SYNTAX), ("2008-10-17", NOT_WELL_FORMED)]
    )
    def test_policy_refuses_arn_value(self, mgmt, account_id, user, version, message, op, value):
        doc = {
            "Version": version,
            "Statement": [{
                "Effect": "Deny",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": {op: {"aws:PrincipalArn": f"arn:aws:iam::123456789012:user/{value}"}},
            }],
        }
        resp = mgmt.put_user_policy(account_id, user, _name(), doc)
        assert resp.status_code == 400, resp.text
        assert resp.text.startswith(f"Invalid policy document: policy parse error: {message} ")

    @pytest.mark.parametrize("value", INVALID)
    @pytest.mark.parametrize("version", ["2012-10-17", "2008-10-17"])
    def test_policy_refuses_resource_pattern(self, mgmt, account_id, user, version, value):
        pattern = TABLE + value
        resp = mgmt.put_user_policy(account_id, user, _name(), _resource_document(version, pattern))
        assert resp.status_code == 400, resp.text
        if version == "2012-10-17":
            expected = f"{SYNTAX} (Resource holds {_shown(pattern)})"
        elif "'" in value or value in MALFORMED:
            # A closing `}` follows the `${`, quoted or not: the variable message.
            expected = f"{NOT_WELL_FORMED} (Resource holds {_shown(pattern)})"
        else:
            expected = f'MalformedPolicyDocument: Resource {pattern} must be in ARN format or "*".'
        assert resp.text == f"Invalid policy document: policy parse error: {expected}"

    @pytest.mark.parametrize("value", TERMINATED)
    @pytest.mark.parametrize("version", ["2012-10-17", "2008-10-17"])
    @pytest.mark.parametrize("principal_type", ["user", "group", "role"])
    def test_policy_accepts_closed_references(
        self, request, mgmt, account_id, principal_type, version, value
    ):
        principal = request.getfixturevalue(principal_type)
        put = getattr(mgmt, f"put_{principal_type}_policy")
        resp = put(account_id, principal, _name(), _condition_document(version, value))
        assert resp.status_code == 204, resp.text
        resp = put(account_id, principal, _name(), _resource_document(version, TABLE + value))
        assert resp.status_code == 204, resp.text

    @pytest.mark.parametrize("principal_type", ["user", "role"])
    def test_boundary_refuses(self, request, mgmt, account_id, principal_type):
        principal = request.getfixturevalue(principal_type)
        put = getattr(mgmt, f"set_{principal_type}_boundary")
        get = getattr(mgmt, f"get_{principal_type}_boundary")
        resp = put(account_id, principal, _condition_document("2012-10-17", "a${"))
        assert resp.status_code == 400, resp.text
        assert resp.text == (
            "Invalid permissions boundary document: policy parse error: "
            f"{_condition_error(SYNTAX, 'a${')}"
        )
        assert get(account_id, principal).status_code == 404
        resp = put(account_id, principal, _condition_document("2012-10-17", "a${aws:username}"))
        assert resp.status_code == 204, resp.text
        resp = put(account_id, principal, _condition_document("2012-10-17", "a${"))
        assert resp.status_code == 400, resp.text
        assert get(account_id, principal).json()["Statement"][0]["Condition"]["StringEquals"][
            TAG_KEY
        ] == "a${aws:username}"

    @pytest.mark.parametrize("principal_type", ["user", "group", "role"])
    def test_console_policy_refuses(self, request, console, account_id, principal_type):
        principal = request.getfixturevalue(principal_type)
        base = f"/accounts/{account_id}/{principal_type}s/{principal}"
        name = _name()
        resp = console.post(
            f"{base}/policies/new",
            {
                "policy_name": name,
                "policy_document": json.dumps(_condition_document("2012-10-17", "a${")),
            },
        )
        assert resp.status_code == 200
        assert SYNTAX in resp.text
        assert name not in console.get(base).text


class TestTrustWrite:
    @pytest.mark.parametrize("value", INVALID)
    @pytest.mark.parametrize(
        "op, key", [("StringEquals", "sts:ExternalId"), ("StringLike", "sts:ExternalId")]
    )
    def test_2012_refuses(self, mgmt, account_id, op, key, value):
        name = f"vs-{uuid.uuid4().hex[:8]}"
        resp = mgmt.create_role(account_id, name, _trust(account_id, "2012-10-17", op, key, value))
        assert resp.status_code == 400, resp.text
        assert resp.text == (
            f"Invalid trust policy: {TRUST_2012} (condition on {key} holds {_shown(value)})"
        )
        assert name not in _role_names(mgmt, account_id)

    @pytest.mark.parametrize("value", UNTERMINATED)
    @pytest.mark.parametrize("version", ["2008-10-17", None])
    def test_older_version_stores_string_value(self, mgmt, account_id, version, value):
        name = f"vs-{uuid.uuid4().hex[:8]}"
        doc = _trust(account_id, version, "StringEquals", "sts:ExternalId", value)
        resp = mgmt.create_role(account_id, name, doc)
        assert resp.status_code == 201, resp.text
        mgmt.delete_role(account_id, name)

    @pytest.mark.parametrize("version", ["2008-10-17", None])
    def test_older_version_refuses_arn_value(self, mgmt, account_id, version):
        name = f"vs-{uuid.uuid4().hex[:8]}"
        doc = _trust(
            account_id, version, "ArnLike", "aws:PrincipalArn", "arn:aws:iam::123456789012:user/a${"
        )
        resp = mgmt.create_role(account_id, name, doc)
        assert resp.status_code == 400, resp.text
        assert resp.text == (
            f"Invalid trust policy: {TRUST_ARN}"
            + r"\nstring: \"arn:aws:iam::123456789012:user\/a\${\"\n"
        )

    @pytest.mark.parametrize("value", TERMINATED)
    def test_2012_accepts_closed_references(self, mgmt, account_id, value):
        name = f"vs-{uuid.uuid4().hex[:8]}"
        doc = _trust(account_id, "2012-10-17", "StringEquals", "sts:ExternalId", value)
        resp = mgmt.create_role(account_id, name, doc)
        assert resp.status_code == 201, resp.text
        mgmt.delete_role(account_id, name)

    def test_console_refuses(self, console, account_id):
        role = f"vs-{uuid.uuid4().hex[:8]}"
        doc = _trust(account_id, "2012-10-17", "StringEquals", "sts:ExternalId", "a${")
        resp = console.post(
            f"/accounts/{account_id}/roles/new",
            {"role_name": role, "trust_policy": json.dumps(doc)},
        )
        assert resp.status_code == 200
        assert TRUST_2012 in resp.text
        assert role not in console.get(f"/accounts/{account_id}").text


class TestOrdering:
    def test_2008_legacy_value_wins_anywhere(self, mgmt, account_id, user):
        doc = {
            "Version": "2008-10-17",
            "Statement": [
                {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
                 "Condition": {"StringEquals": {TAG_KEY: "a${"}}},
                {"Effect": "Deny", "Action": "dynamodb:GetItem", "Resource": "*",
                 "Condition": {"NumericEquals": {"aws:MultiFactorAuthAge": "1${"}}},
            ],
        }
        resp = mgmt.put_user_policy(account_id, user, _name(), doc)
        assert resp.status_code == 400, resp.text
        assert resp.text.startswith(
            "Invalid policy document: policy parse error: "
            "MalformedPolicyDocument: The policy failed legacy parsing"
        )

    def test_2012_trust_structural_fault_first(self, mgmt, account_id):
        doc = _trust(account_id, "2012-10-17", "StringEquals", "sts:ExternalId", "a${")
        doc["Statement"][0]["Resource"] = "*"
        resp = mgmt.create_role(account_id, _name(), doc)
        assert resp.status_code == 400, resp.text
        assert resp.text == "Invalid trust policy: Resource is not allowed in a trust policy"

    def test_2012_trust_variable_before_sid(self, mgmt, account_id):
        doc = _trust(account_id, "2012-10-17", "StringEquals", "sts:ExternalId", "a${")
        doc["Statement"][0]["Sid"] = "bad-sid"
        resp = mgmt.create_role(account_id, _name(), doc)
        assert resp.status_code == 400, resp.text
        assert resp.text.startswith(f"Invalid trust policy: {TRUST_2012}")


class TestQuotedDefaultEvaluation:
    """A quoted default is read as IAM reads it when a request is authorized.

    The policy simulator rows (2026-11-09, two samples each) used
    `StringEquals` on `aws:ResourceTag/owner`: `a${aws:username, '}x'}` matches
    `a}x`, `a${aws:username, 'x''y'}` matches `ax'y`, and `a${*, 'x'}` matches
    `a*` and not `ax`. A DynamoDB tag value cannot hold `}` or `'`, so here the
    same values gate `dynamodb:LeadingKeys`, which a request sets to any
    string. The caller has no `nope` tag, so the default is used.
    """

    @pytest.fixture(scope="class")
    def table(self, auth_env, mgmt, account_id):
        region = "us-east-1"
        name = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(mgmt, account_id, name)
        assert mgmt.put_user_policy(account_id, name, "full", ALLOW_ALL).status_code == 204
        admin = _make_client(auth_env[0], ak, sk, region)
        table = f"qd-{uuid.uuid4().hex[:8]}"
        admin.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(admin, table)
        yield table
        try:
            admin.delete_table(TableName=table)
        except ClientError:
            pass
        mgmt.delete_user(account_id, name)

    @pytest.mark.parametrize(
        "value, allowed, denied",
        [
            ("${aws:PrincipalTag/nope, '}x'}", "}x", "x"),
            ("${aws:PrincipalTag/nope, 'x''y'}", "x'y", "x''y"),
            ("${*, 'x'}", "*", "x"),
        ],
    )
    def test_leading_key_default(self, table, auth_env, mgmt, account_id, value, allowed, denied):
        name = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(mgmt, account_id, name)
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": "*",
                "Condition": {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": [value]}},
            }],
        }
        assert mgmt.put_user_policy(account_id, name, "lk", policy).status_code == 204
        client = _make_client(auth_env[0], ak, sk, "us-east-1")
        try:
            resp = client.get_item(TableName=table, Key={"pk": {"S": allowed}})
            assert "Item" not in resp
            with pytest.raises(ClientError) as exc:
                client.get_item(TableName=table, Key={"pk": {"S": denied}})
            assert _error_code(exc.value) == "AccessDeniedException", exc.value.response
        finally:
            mgmt.delete_user(account_id, name)
