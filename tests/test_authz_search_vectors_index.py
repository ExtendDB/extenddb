# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors is authorized against the vector index it searches.

The service documents ``arn:aws:dynamodb:<region>:<account>:table/<T>/index/<I>``
as the resource for ``dynamodb:SearchVectors``. ExtendDB authorized every
SearchVectors request against the table ARN, so a Deny on one vector index
never matched and the search ran. Each case below gives a principal
``dynamodb:*`` on ``*`` plus one Deny, and checks the refusal and its error
type against a running server.

An IndexName outside ``[a-zA-Z0-9_.-]+`` (``vidx/extra``) gets ValidationException
for every caller, before the policy decision, as Query and Scan do.

The principals are builtin-IAM users provisioned through the management API.
The module skips when the backend does not support vector indexes, unless
``EXTENDDB_EXPECT_VECTORS=1``, which makes that a failure.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from management_helpers import ManagementClient
from test_search_vectors_condition_limit import _expect_vectors

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
VECTOR = [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]

ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}


def _allow_all_deny(resource: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "dynamodb:SearchVectors", "Resource": resource},
        ],
    }


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: SearchVectors authz tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


class Principal:
    """A builtin-IAM user that posts SigV4-signed DynamoDB JSON requests."""

    def __init__(self, endpoint: str, ak: str, sk: str) -> None:
        self.endpoint = endpoint
        self.creds = Credentials(ak, sk)

    def post(self, operation: str, body: dict) -> requests.Response:
        data = json.dumps(body).encode("utf-8")
        req = AWSRequest(
            method="POST",
            url=self.endpoint,
            data=data,
            headers={
                "X-Amz-Target": f"DynamoDB_20120810.{operation}",
                "Content-Type": "application/x-amz-json-1.0",
            },
        )
        SigV4Auth(self.creds, "dynamodb", REGION).add_auth(req)
        return requests.post(
            self.endpoint,
            data=data,
            headers=dict(req.headers),
            verify=not self.endpoint.startswith("https://"),
            timeout=30,
        )


def _error_type(resp: requests.Response) -> str:
    return resp.json().get("__type", "").rsplit("#", 1)[-1]


@pytest.fixture(scope="module")
def mgmt() -> ManagementClient:
    endpoint, admin_user, admin_pass = _require_auth_env()
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def account_id(mgmt):
    acct = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct, f"svauthz-{acct}")
    assert resp.status_code == 201, resp.text
    yield acct
    mgmt.delete_account(acct)


@pytest.fixture(scope="module")
def principal(mgmt, account_id):
    """Return a factory: policy -> Principal holding a fresh user's keys."""
    endpoint = _require_auth_env()[0]

    def make(policy: dict) -> Principal:
        user = f"u{uuid.uuid4().hex[:10]}"
        assert mgmt.create_user(account_id, user, "TestPass123!").status_code == 201
        assert mgmt.put_user_policy(account_id, user, "p", policy).status_code == 204
        resp = mgmt.create_access_key(account_id, user)
        assert resp.status_code == 201, resp.text
        keys = resp.json()
        return Principal(endpoint, keys["access_key_id"], keys["secret_access_key"])

    return make


@pytest.fixture(scope="module")
def vector_table(principal):
    """A table with one item and an ACTIVE vector index ``vidx``."""
    expected = _expect_vectors()
    admin = principal(ALLOW_ALL)
    name = f"sv{uuid.uuid4().hex[:10]}"
    resp = admin.post(
        "CreateTable",
        {
            "TableName": name,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "vidx",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
    )
    body = resp.json() if resp.status_code == 400 else {}
    if "not supported" in body.get("message", body.get("Message", "")):
        if expected is True:
            pytest.fail("EXTENDDB_EXPECT_VECTORS=1 but the backend refuses vector indexes")
        pytest.skip("target backend does not support vector indexes")
    assert resp.status_code == 200, resp.text
    try:
        deadline = time.monotonic() + 60
        while True:
            table = admin.post("DescribeTable", {"TableName": name}).json()["Table"]
            if table["TableStatus"] == "ACTIVE" and all(
                vi.get("IndexStatus") == "ACTIVE" for vi in table.get("VectorIndexes", [])
            ) and table.get("VectorIndexes"):
                break
            assert time.monotonic() < deadline, f"{name} did not become ACTIVE: {table}"
            time.sleep(0.05)
        item = {"pk": {"S": "p1"}, "emb": {"L": VECTOR}}
        put = admin.post("PutItem", {"TableName": name, "Item": item})
        assert put.status_code == 200, put.text
        yield {"name": name, "table_arn": table["TableArn"]}
    finally:
        admin.post("DeleteTable", {"TableName": name})


def _search(p: Principal, table: str, index: str = "vidx") -> requests.Response:
    return p.post(
        "SearchVectors",
        {"TableName": table, "IndexName": index, "SearchVector": VECTOR, "TopK": 5},
    )


def _policy(effect: str, resource: str) -> dict:
    """Allow: SearchVectors on ``resource`` only. Deny: allow-all plus a Deny on it."""
    if effect == "Allow":
        return {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:SearchVectors", "Resource": resource}
            ],
        }
    return _allow_all_deny(resource)


# (effect, resource suffix on the table ARN, status, error type)
POLICY_CASES = [
    ("Deny", "/index/vidx", 400, "AccessDeniedException"),
    ("Deny", "/index/*", 400, "AccessDeniedException"),
    ("Deny", "/index/other", 200, None),
    ("Deny", "", 200, None),  # a table-only Deny does not cover SearchVectors
    ("Allow", "", 400, "AccessDeniedException"),  # a table-only Allow loses it
    ("Allow", "/index/*", 200, None),
]


@pytest.mark.parametrize(
    ("effect", "suffix", "status", "error"),
    POLICY_CASES,
    ids=[f"{e}-table{s or ''}" for e, s, _, _ in POLICY_CASES],
)
def test_search_is_authorized_against_the_index_arn(
    principal, vector_table, effect, suffix, status, error
):
    p = principal(_policy(effect, vector_table["table_arn"] + suffix))
    resp = _search(p, vector_table["name"])
    assert resp.status_code == status, resp.text
    if error:
        assert _error_type(resp) == error, resp.text
    else:
        assert len(resp.json()["SearchResults"]) == 1, resp.text


def test_allowed_principal_searches_the_index(principal, vector_table):
    resp = _search(principal(ALLOW_ALL), vector_table["name"])
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["SearchResults"]) == 1, resp.text


def test_deny_on_the_vector_index_also_applies_to_the_table_arn_name(
    principal, vector_table
):
    p = principal(_allow_all_deny(vector_table["table_arn"] + "/index/vidx"))
    resp = _search(p, vector_table["table_arn"])
    assert resp.status_code == 400, resp.text
    assert _error_type(resp) == "AccessDeniedException", resp.text


@pytest.mark.parametrize(
    "policy_resource",
    [None, "/index/*"],
    ids=["allow-all", "deny-index-wildcard"],
)
def test_index_name_outside_the_pattern_is_refused_before_authorization(
    principal, vector_table, policy_resource
):
    # Query and Scan refuse such a name with ValidationException before the
    # policy decision; SearchVectors does the same for every caller.
    policy = (
        ALLOW_ALL
        if policy_resource is None
        else _allow_all_deny(vector_table["table_arn"] + policy_resource)
    )
    resp = _search(principal(policy), vector_table["name"], "vidx/extra")
    assert resp.status_code == 400, resp.text
    assert _error_type(resp) == "ValidationException", resp.text
