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

import json
import os
import time
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient

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


def _require_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: the per-account metrics test needs "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD, which devtools/run-tests sets."
        )
    return endpoint, admin_user, admin_pass


def _admin_metrics(env: tuple[str, str, str], **params: str) -> dict[str, Any]:
    endpoint, admin_user, admin_pass = env
    resp = requests.get(
        f"{endpoint}/metrics",
        params={"window": "LastHour", **params},
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


def _per_account(
    data: dict[str, Any],
    metric: str,
    operation: str | None,
    table: str | None,
    field: str = "sum",
) -> dict[str | None, float]:
    """``field`` of the rows of ``metric``, summed per AccountId.

    Only rows whose Operation is ``operation`` and whose TableName is
    ``table`` count; None asks for a row without that dimension.
    """
    sums: dict[str | None, float] = {}
    for m in data["metrics"]:
        if (
            m["metric"] == metric
            and _dimension(m, "Operation") == operation
            and _dimension(m, "TableName") == table
        ):
            account = _dimension(m, "AccountId")
            sums[account] = sums.get(account, 0.0) + m[field]
    return sums


def _ours(sums: dict[str | None, float], ids: list[str]) -> dict[str | None, float]:
    return {account: value for account, value in sums.items() if account in ids}


def _canonical(data: dict[str, Any]) -> list[str]:
    return sorted(json.dumps(m, sort_keys=True) for m in data["metrics"])


def _account(mgmt: ManagementClient, endpoint: str, label: str):
    """A new account with a user that may call every DynamoDB action.

    Returns the account id, the user's DynamoDB client and the user's name.
    """
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(account_id, f"metrics-{label}-{account_id}")
    assert resp.status_code == 201, resp.text
    user = f"metrics-{label}"
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
    kwargs: dict[str, Any] = dict(
        service_name="dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return account_id, boto3.client(**kwargs), user


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
    env = _require_env()
    mgmt = ManagementClient(*env)
    table = f"metrics-accounts-{uuid.uuid4().hex[:8]}"
    accounts = []
    try:
        for label in ("first", "second"):
            accounts.append(_account(mgmt, env[0], label))
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
                _per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table), ids
            )
            present = [
                _per_account(data, "ConsumedReadCapacityUnits", "GetItem", table),
                _per_account(data, "ReturnedItemCount", "Query", table),
                _per_account(data, "ConditionalCheckFailedRequests", "DeleteItem", table),
                _per_account(data, "UserErrors", "GetItem", None),
                _per_account(data, "TimeToLiveDeletedItemCount", None, table),
                _per_account(data, "WriteThrottleEvents", "PutItem", f"{table}-slow"),
                _per_account(data, "ThrottledRequests", "PutItem", f"{table}-slow"),
            ]
            return writes == dict(zip(ids, WRITE_UNITS)) and all(
                every <= set(p) for p in present
            )

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        while True:
            data = _admin_metrics(env)
            if all_flushed(data) or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert all_flushed(data), (
            f"within {FLUSH_DEADLINE_SECONDS}s the metrics lacked a row the "
            f"tests read for one of the two accounts under {table}; PutItem "
            f"write capacity per account: "
            f"{_per_account(data, 'ConsumedWriteCapacityUnits', 'PutItem', table)}"
        )
        # The server lists the rows in no fixed order, so compare them sorted.
        settle = time.monotonic() + SETTLE_DEADLINE_SECONDS
        while True:
            time.sleep(SETTLE_SECONDS)
            again = _admin_metrics(env)
            if _canonical(again) == _canonical(data):
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
    writes = _per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table)
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
    rows = _per_account(data, metric, operation, table if on_table else None, "count")
    assert set(ids) <= set(rows), f"{metric} {operation}: {rows}"


@pytest.mark.parametrize("metric", ["WriteThrottleEvents", "ThrottledRequests"])
def test_each_throttle_metric_names_its_account(flushed, metric):
    _, table, ids, data = flushed
    rows = _per_account(data, metric, "PutItem", f"{table}-slow", "count")
    assert set(ids) <= set(rows), f"{metric}: {rows}"


def test_latency_counts_each_accounts_requests_apart(flushed):
    _, table, ids, data = flushed
    counts = _per_account(data, "SuccessfulRequestLatency", "PutItem", table, "count")
    assert _ours(counts, ids) == {ids[0]: 1, ids[1]: 1}, counts


def test_ttl_deletions_name_their_account(flushed):
    _, table, ids, data = flushed
    deleted = _per_account(data, "TimeToLiveDeletedItemCount", None, table)
    assert _ours(deleted, ids) == {ids[0]: 1.0, ids[1]: 1.0}, deleted


def test_the_account_filter_returns_that_accounts_rows_only(flushed):
    env, table, ids, _ = flushed
    data = _admin_metrics(env, account_id=ids[1])
    accounts = {_dimension(m, "AccountId") for m in data["metrics"]}
    assert accounts == {ids[1]}, accounts
    writes = _per_account(data, "ConsumedWriteCapacityUnits", "PutItem", table)
    assert writes == {ids[1]: WRITE_UNITS[1]}, writes


def test_the_account_filter_applies_to_the_latency_segments(flushed):
    env, _, ids, _ = flushed
    # Only the second account sent a Scan.
    first = {s["operation"] for s in _admin_metrics(env, account_id=ids[0])["segments"]}
    second = {s["operation"] for s in _admin_metrics(env, account_id=ids[1])["segments"]}
    assert "PutItem" in first and "Scan" not in first, first
    assert {"PutItem", "Scan"} <= second, second
