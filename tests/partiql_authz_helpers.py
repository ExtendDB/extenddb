# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Principals, policies and checks shared by the PartiQL authorization tests.

The tests give an IAM user the permissions the measured session had, in its
identity policy, so a refusal names the user and an identity-based policy.
A test class derives from :class:`PartiqlAuthzCase` and imports the module
fixtures it uses (``auth_env``, ``mgmt``, ``region``, ``account_id``).
"""

from __future__ import annotations

import os
import time
import uuid
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

PARTIQL = [
    "dynamodb:PartiQLSelect",
    "dynamodb:PartiQLInsert",
    "dynamodb:PartiQLUpdate",
    "dynamodb:PartiQLDelete",
]
CLASSIC = [
    "dynamodb:GetItem",
    "dynamodb:Query",
    "dynamodb:Scan",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:BatchGetItem",
    "dynamodb:BatchWriteItem",
    "dynamodb:ConditionCheckItem",
]
ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}
AX = "pk = 'a' AND sk = 'x'"
BX = "pk = 'b' AND sk = 'x'"
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons "


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: PartiQL authorization tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


def _make_client(endpoint_url: str, access_key: str, secret_key: str, region: str,
                 session_token: str | None = None, validate: bool = True) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}, parameter_validation=validate),
    )
    if session_token is not None:
        kwargs["aws_session_token"] = session_token
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def select(table: str, where: str | None = None, what: str = "*",
           index: str | None = None) -> str:
    target = f'"{table}"' if index is None else f'"{table}"."{index}"'
    return f"SELECT {what} FROM {target}" + (f" WHERE {where}" if where else "")


def keys_of(items: list[dict]) -> list[str]:
    return sorted("/".join(i[n]["S"] for n in ("pk", "sk") if n in i) for i in items)


def statement_error(client: Any, statement: str, **kwargs) -> dict:
    with pytest.raises(ClientError) as exc:
        client.execute_statement(Statement=statement, **kwargs)
    return exc.value.response["Error"]


def batch(client: Any, statements: list[str]) -> list[dict]:
    resp = client.batch_execute_statement(
        Statements=[{"Statement": s} for s in statements])
    return resp["Responses"]


def transaction(client: Any, statements: list[str]) -> dict:
    return client.execute_transaction(
        TransactStatements=[{"Statement": s} for s in statements])


def cancelled(client: Any, statements: list[str]) -> dict:
    with pytest.raises(ClientError) as exc:
        transaction(client, statements)
    return exc.value.response


def transaction_error(client: Any, statements: list[str]) -> dict:
    return cancelled(client, statements)["Error"]


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
    resp = mgmt.create_account(acct_id, f"pq-authz-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class PartiqlAuthzCase:
    """Principals and policies in an account of their own, and the checks of
    the answers a refused statement gets. A table fixture sets ``self._run``,
    the run id in the names of the tables."""

    _run: str

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self._users: list[str] = []
        self._roles: list[str] = []
        yield
        for u in self._users:
            try:
                self.mgmt.delete_user(self.account_id, u)
            except Exception:
                pass
        for r in self._roles:
            try:
                self.mgmt.delete_role(self.account_id, r)
            except Exception:
                pass

    # -- principals and policies ---------------------------------------------

    def _user(self, policy: dict, validate: bool = True) -> tuple[Any, str]:
        """A user with `policy` as its identity policy: its client and ARN."""
        name = f"pqz-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, name, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(name)
        resp = self.mgmt.create_access_key(self.account_id, name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
        assert resp.status_code == 204, resp.text
        client = _make_client(self.endpoint, creds["access_key_id"],
                              creds["secret_access_key"], self.region, validate=validate)
        return client, f"arn:aws:iam::{self.account_id}:user/{name}"

    def _client(self, *statements: dict, validate: bool = True) -> tuple[Any, str]:
        return self._user({"Version": "2012-10-17", "Statement": list(statements)},
                          validate=validate)

    def _session(self, session_policy: dict) -> Any:
        """A role session: the role is allowed dynamodb:*, the session is
        narrowed by `session_policy`."""
        _, caller_arn = self._client(
            {"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": "*"})
        role = f"pqr-{uuid.uuid4().hex[:8]}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": caller_arn},
                           "Action": "sts:AssumeRole"}],
        }
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        self._roles.append(role)
        assert self.mgmt.put_role_policy(
            self.account_id, role, "full", ALLOW_ALL).status_code == 204
        resp = self.mgmt.assume_role(self.account_id, role, caller_arn, "pq-authz",
                                     session_policy=session_policy)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        return _make_client(self.endpoint, creds["access_key_id"],
                            creds["secret_access_key"], self.region,
                            session_token=creds["session_token"])

    def _arn(self, table: str, index: str | None = None) -> str:
        arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{table}"
        return arn if index is None else f"{arn}/index/{index}"

    def _tables_pattern(self) -> list[str]:
        """The tables named pqa-* and their indexes, as the measured identity
        policy names them."""
        base = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/pqa-*"
        return [base, base + "/index/*"]

    def _allow(self, actions: list[str], condition: dict | None = None,
               resources: list[str] | None = None) -> dict:
        st: dict = {"Effect": "Allow", "Action": actions,
                    "Resource": resources or self._tables_pattern()}
        if condition:
            st["Condition"] = condition
        return st

    def _deny(self, actions: list[str], condition: dict | None = None,
              resources: list[str] | None = None) -> dict:
        st = self._allow(actions, condition, resources)
        st["Effect"] = "Deny"
        return st

    def _identity(self, validate: bool = True) -> tuple[Any, str]:
        """The measured identity policy: dynamodb:* on pqa-* and their
        indexes, PartiQLDelete denied on pqa-deny-*."""
        return self._client(
            self._allow(["dynamodb:*"]),
            self._deny(["dynamodb:PartiQLDelete"],
                       resources=[self._arn(f"pqa-deny-{self._run}")]),
            validate=validate,
        )

    def _fenced(self, operator: str, values: list[str] | str) -> tuple[Any, str]:
        return self._client(self._allow(
            PARTIQL, {operator: {"dynamodb:LeadingKeys": values}}))

    def _with(self, actions: list[str], condition: dict | None = None,
              resources: list[str] | None = None) -> tuple[Any, str]:
        """A user allowed `actions` on the tables named pqa-* and their
        indexes, under `condition`."""
        return self._client(self._allow(actions, condition, resources))

    # -- tables ----------------------------------------------------------------

    @pytest.fixture()
    def r(self):
        """The tables of the second measurement: C (pk, sk, GSI gix on g)
        holding a/x (g ga), a/y, b/x (g gb) and c/x, each with a = '1'; K
        (pk, sk, KEYS_ONLY GSI kix on g) holding a/x (g ga); N keyed on a
        number pk holding 10; L (pk, sk, KEYS_ONLY LSI lix on l) holding a/x
        with l and an attribute u the index does not hold; B outside the
        pqa-* pattern, keyed on pk with GSI gix on g, holding a (g ga)."""
        self._run = uuid.uuid4().hex[:6]
        owner, _ = self._user(ALLOW_ALL)
        names = SimpleNamespace(
            c=f"pqa-{self._run}-c", k=f"pqa-{self._run}-k", n=f"pqa-{self._run}-n",
            l=f"pqa-{self._run}-l", b=f"pqb-{self._run}",
            missing_a=f"pqa-{self._run}-missing", missing_b=f"pqb-{self._run}-missing",
            owner=owner)

        def create(name: str, attrs: list[str | tuple[str, str]],
                   keys: list[tuple[str, str]], **extra: Any) -> None:
            typed = [a if isinstance(a, tuple) else (a, "S") for a in attrs]
            owner.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": a, "AttributeType": t}
                                      for a, t in typed],
                KeySchema=[{"AttributeName": a, "KeyType": k} for a, k in keys],
                BillingMode="PAY_PER_REQUEST", **extra)
            wait_for_active(owner, name)

        def on_g(index: str, projection: str) -> dict:
            return {"GlobalSecondaryIndexes": [{
                "IndexName": index,
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": projection}}]}

        def put(name: str, **attrs: str) -> None:
            owner.put_item(TableName=name, Item={k: {"S": v} for k, v in attrs.items()})

        pk_sk = [("pk", "HASH"), ("sk", "RANGE")]
        create(names.c, ["pk", "sk", "g"], pk_sk, **on_g("gix", "ALL"))
        create(names.k, ["pk", "sk", "g"], pk_sk, **on_g("kix", "KEYS_ONLY"))
        create(names.b, ["pk", "g"], [("pk", "HASH")], **on_g("gix", "ALL"))
        create(names.n, [("pk", "N")], [("pk", "HASH")])
        create(names.l, ["pk", "sk", "l"], pk_sk, LocalSecondaryIndexes=[{
            "IndexName": "lix",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"},
                          {"AttributeName": "l", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"}}])
        owner.put_item(TableName=names.n, Item={"pk": {"N": "10"}, "a": {"S": "1"}})
        put(names.l, pk="a", sk="x", l="la", u="hidden")
        put(names.c, pk="a", sk="x", g="ga", a="1")
        put(names.c, pk="a", sk="y", a="1")
        put(names.c, pk="b", sk="x", g="gb", a="1")
        put(names.c, pk="c", sk="x", a="1")
        put(names.k, pk="a", sk="x", g="ga", a="1")
        put(names.b, pk="a", g="ga")
        for name, index, count in ((names.c, "gix", 2), (names.k, "kix", 1),
                                   (names.b, "gix", 1)):
            deadline = time.monotonic() + 15
            while owner.scan(TableName=name, IndexName=index,
                             Select="COUNT")["Count"] < count:
                assert time.monotonic() < deadline, f"index {index} of {name} not filled"
                time.sleep(0.05)
        yield names
        for name in (names.c, names.k, names.n, names.l, names.b):
            try:
                owner.delete_table(TableName=name)
            except Exception:
                pass

    # -- the answers to a refused statement ------------------------------------

    @staticmethod
    def _refusal(principal: str, action: str, resource: str,
                 explicit: bool = False) -> str:
        head = (f"User: {principal} is not authorized to perform: dynamodb:{action} "
                f"on resource: {resource}")
        if explicit:
            return head + " with an explicit deny in an identity-based policy"
        return head + f" because no identity-based policy allows the dynamodb:{action} action"

    def _denied(self, error: dict, principal: str, action: str, resource: str,
                explicit: bool = False) -> None:
        assert error["Code"] == "AccessDeniedException", error
        assert error["Message"] == self._refusal(principal, action, resource, explicit)

    def _denied_entry(self, entry: dict, principal: str, action: str, table: str,
                      explicit: bool = False) -> None:
        assert entry == {
            "Error": {"Code": "AccessDenied",
                      "Message": self._refusal(principal, action, self._arn(table),
                                               explicit)},
            "TableName": table,
        }

    def _cancelled_by(self, resp: dict, reasons: list[tuple[str, str] | None],
                      principal: str) -> None:
        """A transaction cancelled with AccessDenied, as (action, table), for
        each refused member and None for the others."""
        codes = ["None" if r is None else "AccessDenied" for r in reasons]
        assert resp["Error"]["Code"] == "TransactionCanceledException", resp
        assert resp["Error"]["Message"] == CANCELLED + f"[{', '.join(codes)}]"
        want = []
        for r in reasons:
            if r is None:
                want.append({"Code": "None"})
            else:
                action, table, *explicit = r
                want.append({"Code": "AccessDenied", "Message": self._refusal(
                    principal, action, self._arn(table), bool(explicit))})
        assert resp["CancellationReasons"] == want
