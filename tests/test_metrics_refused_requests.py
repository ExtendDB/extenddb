# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A request refused in the authorization step counts in the metrics.

The server refuses some requests in its authorization step, before the IAM
decision (a ``TableName`` that breaks the naming rules among them) or by it
(``AccessDeniedException``). Each is an HTTP 400, which the DynamoDB metrics
documentation counts in ``UserErrors``, and each used to record no metric,
where a request the engine refuses records ``UserErrors`` and
``RequestCount``. Now all three record the same two metrics.

The test creates one account with a table, a user that may call every
DynamoDB action and a user with no policy. It sends REPEATS requests of each
row of ROWS, one operation per row so the rows have their own metrics rows:
a GetItem with the TableName ``bad name!`` (refused by the table name gate),
a Query by the user with no policy (refused by the IAM decision) and a
PutItem to a table that does not exist (refused by the engine, the control).
Last it sends a DescribeTable that succeeds, waits for that request's row
(see ``flushed``), and reads ``UserErrors`` and ``RequestCount`` for each
row's operation.

Prerequisites:
  - extenddb running on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig

from conftest import _is_real_dynamodb, wait_for_active, wait_for_deleted
from management_helpers import ManagementClient

# The flush worker persists a data point once it is older than the worker's
# 60-second interval, at the next tick, so a point can take up to two
# intervals to reach the metrics table.
FLUSH_DEADLINE_SECONDS = 180
POLL_SECONDS = 5
# After the awaited row, the fixture reads the metrics every SETTLE_SECONDS
# until two reads in a row are the same; see ``flushed``.
SETTLE_SECONDS = 5

# How many requests of each row are sent. Each counts once.
REPEATS = 5

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(_is_real_dynamodb(), reason="/metrics is ExtendDB's own endpoint"),
]

KEY = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "BillingMode": "PAY_PER_REQUEST",
}

# (operation, the user that sends it, the refusal's __type). The request of
# each is built by ``_request``.
ROWS = [
    ("GetItem", "owner", "ValidationException"),
    ("Query", "nobody", "AccessDeniedException"),
    ("PutItem", "owner", "ResourceNotFoundException"),
]


def _request(operation: str, table: str) -> dict[str, Any]:
    """The request of a row of ROWS, against the test table ``table``."""
    if operation == "GetItem":
        # Refused by the table name gate, before the IAM decision.
        return {"TableName": "bad name!", "Key": {"pk": {"S": "1"}}}
    if operation == "Query":
        # Refused by the IAM decision: the sender has no policy.
        return {
            "TableName": table,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "1"}},
        }
    # Refused by the engine, which already recorded the two metrics: the
    # control row.
    return {"TableName": f"{table}-missing", "Item": {"pk": {"S": "1"}}}


def _require_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: the refused-request metrics test needs "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD, which devtools/run-tests sets."
        )
    return endpoint, admin_user, admin_pass


def _admin_metrics(env: tuple[str, str, str]) -> dict[str, Any]:
    endpoint, admin_user, admin_pass = env
    resp = requests.get(
        f"{endpoint}/metrics",
        params={"window": "LastHour"},
        auth=(admin_user, admin_pass),
        timeout=10,
        verify=False,
    )
    assert resp.status_code == 200, f"/metrics: {resp.status_code} {resp.text[:200]}"
    return resp.json()


def _dimension(metric: dict[str, Any], key: str) -> str | None:
    for d in metric["dimensions"]:
        if isinstance(d, dict) and key in d:
            return d[key]
    return None


def _count(
    data: dict[str, Any], account_id: str, metric: str, operation: str, table: str | None
) -> int:
    """The data points of ``metric`` for ``operation`` in ``account_id``.

    Only rows whose TableName is ``table`` count; None asks for a row without
    that dimension. 0 when there is no such row.
    """
    return sum(
        m["count"]
        for m in data["metrics"]
        if m["metric"] == metric
        and _dimension(m, "AccountId") == account_id
        and _dimension(m, "Operation") == operation
        and _dimension(m, "TableName") == table
    )


def _canonical(data: dict[str, Any]) -> list[str]:
    return sorted(json.dumps(m, sort_keys=True) for m in data["metrics"])


def _client(endpoint: str, creds: dict[str, str]):
    """A DynamoDB client of one user, with no retries and no parameter checks."""
    kwargs: dict[str, Any] = {
        "service_name": "dynamodb",
        "endpoint_url": endpoint,
        "aws_access_key_id": creds["access_key_id"],
        "aws_secret_access_key": creds["secret_access_key"],
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": BotoConfig(retries={"max_attempts": 0}, parameter_validation=False),
    }
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _user(mgmt: ManagementClient, endpoint: str, account_id: str, name: str, policy: bool):
    """A user of ``account_id`` with an access key, and a full policy if asked."""
    resp = mgmt.create_user(account_id, name)
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    if policy:
        document = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        }
        resp = mgmt.put_user_policy(account_id, name, "full", document)
        assert resp.status_code == 204, resp.text
    return _client(endpoint, creds)


def _send(client, operation: str, request: dict[str, Any], expected_type: str) -> None:
    """One request through the client's own API method; it must be refused."""
    method = getattr(client, _snake(operation))
    try:
        method(**request)
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        assert code == expected_type, f"{operation}: {code} {e.response['Error']}"
        return
    pytest.fail(f"{operation} {request} was served; expected {expected_type}")


def _snake(operation: str) -> str:
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in operation).lstrip("_")


@pytest.fixture(scope="module")
def flushed():
    """The test's requests, persisted by the flush.

    Yields the account id and the metrics an admin reads once the last
    request's row is in them.
    """
    env = _require_env()
    endpoint = env[0]
    mgmt = ManagementClient(*env)
    suffix = uuid.uuid4().hex[:8]
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    table = f"metrics-refused-{suffix}"
    resp = mgmt.create_account(account_id, f"metrics-refused-{account_id}")
    assert resp.status_code == 201, resp.text
    users: dict[str, Any] = {}
    try:
        users["owner"] = _user(mgmt, endpoint, account_id, "owner", policy=True)
        users["nobody"] = _user(mgmt, endpoint, account_id, "nobody", policy=False)
        owner = users["owner"]
        owner.create_table(TableName=table, **KEY)
        try:
            wait_for_active(owner, table)
            for operation, user, expected_type in ROWS:
                for _ in range(REPEATS):
                    _send(users[user], operation, _request(operation, table), expected_type)
            # Last, so its row reaching the metrics means the flush tick that
            # writes this minute's rows has started.
            owner.describe_table(TableName=table)

            def sentinel_flushed(data: dict[str, Any]) -> bool:
                return _count(data, account_id, "RequestCount", "DescribeTable", None) >= 1

            deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
            while True:
                data = _admin_metrics(env)
                if sentinel_flushed(data) or time.monotonic() > deadline:
                    break
                time.sleep(POLL_SECONDS)
            assert sentinel_flushed(data), (
                f"within {FLUSH_DEADLINE_SECONDS}s the metrics had no RequestCount "
                f"row for the DescribeTable of account {account_id}"
            )
            # One flush tick writes every pending row of the minute, one
            # statement per row, and a read can land partway through it. So
            # read until two reads SETTLE_SECONDS apart are the same: nothing
            # was written between them, so the tick that wrote the awaited row
            # has finished. The server lists the rows in no fixed order, so
            # compare them sorted.
            settle = time.monotonic() + FLUSH_DEADLINE_SECONDS
            while True:
                time.sleep(SETTLE_SECONDS)
                again = _admin_metrics(env)
                if _canonical(again) == _canonical(data):
                    break
                assert time.monotonic() < settle, (
                    f"the metrics were still changing {FLUSH_DEADLINE_SECONDS}s "
                    f"after the awaited row arrived"
                )
                data = again
            yield account_id, data
        finally:
            owner.delete_table(TableName=table)
            wait_for_deleted(owner, table)
    finally:
        for name in users:
            resp = mgmt.delete_user(account_id, name)
            assert resp.status_code in (200, 204), resp.text
        resp = mgmt.delete_account(account_id)
        assert resp.status_code in (200, 204), resp.text


@pytest.mark.parametrize("operation, user, expected_type", ROWS, ids=[row[0] for row in ROWS])
def test_a_refused_request_counts_once_in_user_errors_and_request_count(
    flushed, operation, user, expected_type
):
    account_id, data = flushed
    counts = {
        metric: _count(data, account_id, metric, operation, None)
        for metric in ("UserErrors", "RequestCount")
    }
    assert counts == {"UserErrors": REPEATS, "RequestCount": REPEATS}, (
        f"{operation} by {user}, refused with {expected_type}: {counts}"
    )
