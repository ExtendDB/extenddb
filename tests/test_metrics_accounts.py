# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Metrics are kept per account (issue #728).

The metrics used to have no account dimension, so two accounts that each
had a table of the same name wrote to one row, and the counts an admin read
for that name were the two tables' added together. Each metric of a request
and each TTL metric of a table now carries the account it was recorded for,
as an ``AccountId`` dimension, and ``/metrics`` takes an ``account_id``
filter, which also applies to the latency segments.

The test creates two accounts, each with a user that may call every
DynamoDB action, and a table of the same name with TTL in each. Each
account sends one PutItem (one write capacity unit for the first account,
three for the second, so a sum of the two is visible), a GetItem, a Query,
a DeleteItem whose condition fails, a GetItem on a table that does not
exist, and PutItems to a table provisioned with one write capacity unit
until one is throttled; the second account also sends a Scan. Each table holds one expired
item for the TTL sweep. The fixture waits until the flush has persisted
every row the tests read, with the PutItem sums at their final values,
then reads until two reads in a row are the same (see ``flushed``).

Prerequisites:
  - extenddb running on EXTENDDB_TEST_ENDPOINT, with the throttling_enabled
    setting on, as devtools/run-tests sets it
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient
from metrics_helpers import (
    admin_metrics,
    canonical,
    dimension,
    new_account,
    per_account,
    require_env,
)

# A TTL sweep runs once a minute, a new TTL attribute can take one more
# sweep to be ready, and the flush persists a data point at most two
# minutes after it was recorded.
FLUSH_DEADLINE_SECONDS = 420
POLL_SECONDS = 5
# After the awaited rows, the fixture reads the metrics every SETTLE_SECONDS
# until two reads in a row are the same, for at most SETTLE_DEADLINE_SECONDS:
# one flush writes its rows one by one, and a read can land partway through.
SETTLE_SECONDS = 5
SETTLE_DEADLINE_SECONDS = 180

# The write capacity each account's one PutItem consumes: a small item is
# one unit, an item of about 2.5 KB three.
WRITE_UNITS = (1.0, 3.0)
PADDING = (0, 2500)

pytestmark = pytest.mark.slow

KEY = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "BillingMode": "PAY_PER_REQUEST",
}

# A table that throttles writes: one write capacity unit.
SLOW_KEY = {
    "AttributeDefinitions": KEY["AttributeDefinitions"],
    "KeySchema": KEY["KeySchema"],
    "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
}
# How long PutItems go to that table before one must be throttled: the
# server reads the throttling_enabled setting every 30 seconds.
THROTTLE_DEADLINE_SECONDS = 60


def _ours(sums: dict[str | None, float], ids: list[str]) -> dict[str | None, float]:
    return {account: value for account, value in sums.items() if account in ids}


def _throttle(client, table: str) -> None:
    """PutItems to ``table`` until one is throttled."""
    deadline = time.monotonic() + THROTTLE_DEADLINE_SECONDS
    i = 0
    while time.monotonic() < deadline:
        try:
            client.put_item(TableName=table, Item={"pk": {"S": f"w{i}"}})
        except ClientError as e:
            code = e.response["Error"]["Code"]
            assert code == "ProvisionedThroughputExceededException", code
            return
        i += 1
        time.sleep(0.02)
    pytest.fail(
        f"PutItems to a table of one write capacity unit were not throttled "
        f"within {THROTTLE_DEADLINE_SECONDS}s; is the throttling_enabled setting on?"
    )


def _send_requests(client, table: str, padding: int, scan: bool) -> None:
    """The requests whose metrics the tests read, as one account."""
    # The expired item goes in by BatchWriteItem, whose metrics carry no
    # table name, so it adds nothing to the PutItem rows.
    client.batch_write_item(
        RequestItems={
            table: [
                {
                    "PutRequest": {
                        "Item": {
                            "pk": {"S": "expired"},
                            "expires": {"N": str(int(time.time()) - 600)},
                        }
                    }
                }
            ]
        }
    )
    client.put_item(
        TableName=table, Item={"pk": {"S": "1"}, "pad": {"S": "x" * padding or "x"}}
    )
    client.get_item(TableName=table, Key={"pk": {"S": "1"}})
    client.query(
        TableName=table,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "1"}},
    )
    if scan:
        client.scan(TableName=table)
    with pytest.raises(ClientError) as failed:
        client.delete_item(
            TableName=table,
            Key={"pk": {"S": "absent"}},
            ConditionExpression="attribute_exists(pk)",
        )
    assert failed.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    with pytest.raises(ClientError) as missing:
        client.get_item(TableName=f"{table}-missing", Key={"pk": {"S": "1"}})
    assert missing.value.response["Error"]["Code"] == "ResourceNotFoundException"
    _throttle(client, f"{table}-slow")


@pytest.fixture(scope="module")
def flushed():
    """Both accounts' requests, persisted by the flush.

    Yields the admin environment, the table name, the two account ids in
    the order of WRITE_UNITS, and the metrics an admin reads once every row
    the tests read is in them.
    """
    env = require_env()
    mgmt = ManagementClient(*env)
    table = f"metrics-accounts-{uuid.uuid4().hex[:8]}"
    accounts = []
    try:
        for label in ("first", "second"):
            accounts.append(new_account(mgmt, env[0], label))
        for _, client, _ in accounts:
            client.create_table(TableName=table, **KEY)
            client.create_table(TableName=f"{table}-slow", **SLOW_KEY)
        for _, client, _ in accounts:
            wait_for_active(client, table)
            wait_for_active(client, f"{table}-slow")
            client.update_time_to_live(
                TableName=table,
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires"},
            )
        for i, ((_, client, _), padding) in enumerate(zip(accounts, PADDING)):
            _send_requests(client, table, padding, scan=i == 1)

        ids = [account_id for account_id, _, _ in accounts]
        every = set(ids)

        def all_flushed(data: dict[str, Any]) -> bool:
            writes = _ours(
                per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table), ids
            )
            present = [
                per_account(data, "ConsumedReadCapacityUnits", "GetItem", table),
                per_account(data, "ReturnedItemCount", "Query", table),
                per_account(data, "ConditionalCheckFailedRequests", "DeleteItem", table),
                per_account(data, "UserErrors", "GetItem", None),
                per_account(data, "TimeToLiveDeletedItemCount", None, table),
                per_account(data, "WriteThrottleEvents", "PutItem", f"{table}-slow"),
                per_account(data, "ThrottledRequests", "PutItem", f"{table}-slow"),
            ]
            return writes == dict(zip(ids, WRITE_UNITS)) and all(
                every <= set(p) for p in present
            )

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        while True:
            data = admin_metrics(env)
            if all_flushed(data) or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert all_flushed(data), (
            f"within {FLUSH_DEADLINE_SECONDS}s the metrics lacked a row the "
            f"tests read for one of the two accounts under {table}; PutItem "
            f"write capacity per account: "
            f"{per_account(data, 'ConsumedWriteCapacityUnits', 'PutItem', table)}"
        )
        # The server lists the rows in no fixed order, so compare them sorted.
        settle = time.monotonic() + SETTLE_DEADLINE_SECONDS
        while True:
            time.sleep(SETTLE_SECONDS)
            again = admin_metrics(env)
            if canonical(again) == canonical(data):
                break
            assert time.monotonic() < settle, (
                f"the metrics were still changing {SETTLE_DEADLINE_SECONDS}s "
                f"after the awaited rows arrived"
            )
            data = again
        yield env, table, ids, data
    finally:
        for account_id, client, user in accounts:
            try:
                for name in (table, f"{table}-slow"):
                    try:
                        client.delete_table(TableName=name)
                        wait_for_deleted(client, name)
                    except client.exceptions.ResourceNotFoundException:
                        pass
            finally:
                resp = mgmt.delete_user(account_id, user)
                assert resp.status_code in (200, 204), resp.text
                resp = mgmt.delete_account(account_id)
                assert resp.status_code in (200, 204), resp.text


def test_two_accounts_with_the_same_table_name_get_separate_rows(flushed):
    _, table, ids, data = flushed
    writes = per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table)
    assert _ours(writes, ids) == dict(zip(ids, WRITE_UNITS)), writes


@pytest.mark.parametrize(
    "metric, operation, on_table",
    [
        ("ConsumedReadCapacityUnits", "GetItem", True),
        ("ReturnedItemCount", "Query", True),
        ("ReturnedBytes", "Query", True),
        ("ConditionalCheckFailedRequests", "DeleteItem", True),
        ("UserErrors", "GetItem", False),
        ("RequestCount", "PutItem", False),
    ],
)
def test_each_request_metric_names_its_account(flushed, metric, operation, on_table):
    _, table, ids, data = flushed
    rows = per_account(data, metric, operation, table if on_table else None, "count")
    assert set(ids) <= set(rows), f"{metric} {operation}: {rows}"


@pytest.mark.parametrize("metric", ["WriteThrottleEvents", "ThrottledRequests"])
def test_each_throttle_metric_names_its_account(flushed, metric):
    _, table, ids, data = flushed
    rows = per_account(data, metric, "PutItem", f"{table}-slow", "count")
    assert set(ids) <= set(rows), f"{metric}: {rows}"


def test_latency_counts_each_accounts_requests_apart(flushed):
    _, table, ids, data = flushed
    counts = per_account(data, "SuccessfulRequestLatency", "PutItem", table, "count")
    assert _ours(counts, ids) == {ids[0]: 1, ids[1]: 1}, counts


def test_ttl_deletions_name_their_account(flushed):
    _, table, ids, data = flushed
    deleted = per_account(data, "TimeToLiveDeletedItemCount", None, table)
    assert _ours(deleted, ids) == {ids[0]: 1.0, ids[1]: 1.0}, deleted


def test_the_account_filter_returns_that_accounts_rows_only(flushed):
    env, table, ids, _ = flushed
    data = admin_metrics(env, account_id=ids[1])
    accounts = {dimension(m, "AccountId") for m in data["metrics"]}
    assert accounts == {ids[1]}, accounts
    writes = per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table)
    assert writes == {ids[1]: WRITE_UNITS[1]}, writes


def test_the_account_filter_applies_to_the_latency_segments(flushed):
    env, _, ids, _ = flushed
    # Only the second account sent a Scan.
    first = {s["operation"] for s in admin_metrics(env, account_id=ids[0])["segments"]}
    second = {s["operation"] for s in admin_metrics(env, account_id=ids[1])["segments"]}
    assert "PutItem" in first and "Scan" not in first, first
    assert {"PutItem", "Scan"} <= second, second
