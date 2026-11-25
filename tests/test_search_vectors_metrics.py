# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The latency segments of a SearchVectors that fails its checks.

The server makes the checks of a SearchVectors request that need no table
before the IAM decision. A request that fails them is recorded in the
request metrics as a request that fails in the engine is: the error, the
request count and one entry in the latency segments of ``/metrics``. The
test sends three searches with ``TopK`` 0, which fail those checks, and
three searches of a table that does not exist, which pass them on a backend
with vector search, as a new account, and reads the account's SearchVectors
segment count after each group.

``/metrics`` is an ExtendDB endpoint, so the module runs only against
ExtendDB.

Prerequisites:
  - extenddb running on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from management_helpers import ManagementClient

VECTOR = [{"N": "1"}, {"N": "0"}, {"N": "0"}, {"N": "0"}]


def _require_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.skip(
            "The /metrics endpoint is ExtendDB's: the module needs "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD, which devtools/run-tests sets."
        )
    return endpoint, admin_user, admin_pass


def _segment_count(env: tuple[str, str, str], account_id: str) -> int:
    endpoint, admin_user, admin_pass = env
    resp = requests.get(
        f"{endpoint}/metrics",
        params={"account_id": account_id},
        auth=(admin_user, admin_pass),
        timeout=10,
        verify=False,
    )
    assert resp.status_code == 200, f"/metrics: {resp.status_code} {resp.text[:200]}"
    return sum(
        s["count"] for s in resp.json()["segments"] if s["operation"] == "SearchVectors"
    )


@pytest.fixture(scope="module")
def account():
    """A new account with a user that may call every DynamoDB action."""
    env = _require_env()
    mgmt = ManagementClient(*env)
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    user = "search-metrics"
    resp = mgmt.create_account(account_id, f"search-metrics-{account_id}")
    assert resp.status_code == 201, resp.text
    try:
        resp = mgmt.create_user(account_id, user)
        assert resp.status_code == 201, resp.text
        resp = mgmt.create_access_key(account_id, user)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        }
        resp = mgmt.put_user_policy(account_id, user, "full", policy)
        assert resp.status_code == 204, resp.text
        signer = SigV4Auth(
            Credentials(creds["access_key_id"], creds["secret_access_key"]),
            "dynamodb",
            os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        yield env, account_id, signer
    finally:
        mgmt.delete_user(account_id, user)
        resp = mgmt.delete_account(account_id)
        assert resp.status_code in (200, 204), resp.text


def _search(env: tuple[str, str, str], signer: SigV4Auth, table: str, top_k: int) -> str:
    """The error type of a SearchVectors of ``table`` with ``top_k``."""
    body = json.dumps(
        {"TableName": table, "IndexName": "vix", "SearchVector": VECTOR, "TopK": top_k}
    ).encode()
    headers = {
        "X-Amz-Target": "DynamoDB_20120810.SearchVectors",
        "Content-Type": "application/x-amz-json-1.0",
    }
    req = AWSRequest(method="POST", url=env[0], data=body, headers=headers)
    signer.add_auth(req)
    resp = requests.post(
        env[0], data=body, headers=dict(req.headers), verify=False, timeout=30
    )
    assert resp.status_code == 400, resp.text
    return resp.json()["__type"].rsplit("#", 1)[-1]


def test_a_search_that_fails_its_checks_adds_latency_segments(account):
    env, account_id, signer = account
    assert _segment_count(env, account_id) == 0
    codes = {_search(env, signer, "search-metrics-table", 0) for _ in range(3)}
    assert codes == {"ValidationException"}, codes
    assert _segment_count(env, account_id) == 3
    table = f"search-metrics-missing-{uuid.uuid4().hex[:8]}"
    for _ in range(3):
        _search(env, signer, table, 5)
    assert _segment_count(env, account_id) == 6
