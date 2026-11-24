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

import time
import uuid
from typing import Any

import pytest

from conftest import _is_real_dynamodb, wait_for_active, wait_for_deleted
from management_helpers import ManagementClient
from metrics_helpers import (
    admin_metrics,
    canonical,
    new_account,
    new_user,
    per_account,
    require_env,
)

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
    env = require_env()
    endpoint = env[0]
    mgmt = ManagementClient(*env)
    table = f"metrics-refused-{uuid.uuid4().hex[:8]}"
    # The owner may call every DynamoDB action, nobody has no policy. Neither
    # client checks parameters, so the TableName "bad name!" reaches the server.
    account_id, owner, owner_name = new_account(mgmt, endpoint, "refused", validate=False)
    users: dict[str, Any] = {"owner": owner}
    names = [owner_name]
    try:
        users["nobody"] = new_user(
            mgmt, endpoint, account_id, "nobody", policy=False, validate=False
        )
        names.append("nobody")
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
                counted = per_account(data, "RequestCount", "DescribeTable", None, "count")
                return counted.get(account_id, 0) >= 1

            deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
            while True:
                data = admin_metrics(env)
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
                again = admin_metrics(env)
                if canonical(again) == canonical(data):
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
        for name in names:
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
        metric: per_account(data, metric, operation, None, "count").get(account_id, 0)
        for metric in ("UserErrors", "RequestCount")
    }
    assert counts == {"UserErrors": REPEATS, "RequestCount": REPEATS}, (
        f"{operation} by {user}, refused with {expected_type}: {counts}"
    )
