# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Metrics are kept per account (issue #728).

The metrics used to have no account dimension, so two accounts that each
had a table of the same name wrote to one row, and the counts an admin read
for that name were the two tables' added together. Each metric of a request
now carries the account the request was made for, as an ``AccountId``
dimension, and ``/metrics`` takes an ``account_id`` filter.

The test creates two accounts, each with a user that may call every
DynamoDB action, and a table of the same name in each. It writes one item
to the first account's table and three to the second's, waits until the
flush has persisted both accounts' ``ConsumedWriteCapacityUnits`` rows, then
reads until two reads in a row are the same (see ``flushed``): the two
accounts' rows are two rows with their own sums, and the account filter
returns one account's rows only.

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

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient

# The flush worker persists a data point once it is older than the worker's
# 60-second interval, at the next tick, so a point can take up to two
# intervals to reach the metrics table.
FLUSH_DEADLINE_SECONDS = 180
POLL_SECONDS = 5
# After the awaited rows, the fixture reads the metrics every SETTLE_SECONDS
# until two reads in a row are the same: one flush writes its rows one by
# one, and a read can land partway through it.
SETTLE_SECONDS = 5

# Items each account writes to its table; a PutItem of a small item on an
# on-demand table consumes one write capacity unit.
WRITES = (1, 3)

pytestmark = pytest.mark.slow

KEY = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "BillingMode": "PAY_PER_REQUEST",
}


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
        params=params,
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


def _write_capacity(data: dict[str, Any], table: str) -> dict[str | None, float]:
    """The PutItem write capacity under ``table``, summed per AccountId."""
    sums: dict[str | None, float] = {}
    for m in data["metrics"]:
        if (
            m["metric"] == "ConsumedWriteCapacityUnits"
            and _dimension(m, "Operation") == "PutItem"
            and _dimension(m, "TableName") == table
        ):
            account = _dimension(m, "AccountId")
            sums[account] = sums.get(account, 0.0) + m["sum"]
    return sums


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


@pytest.fixture(scope="module")
def flushed():
    """Both accounts' writes, persisted by the flush.

    Yields the admin environment, the table name, the two account ids in
    the order of WRITES, and the metrics an admin reads once both accounts'
    rows are in them.
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
        for (_, client, _), writes in zip(accounts, WRITES):
            wait_for_active(client, table)
            for i in range(writes):
                client.put_item(TableName=table, Item={"pk": {"S": str(i)}})

        ids = [account_id for account_id, _, _ in accounts]

        def both_flushed(data: dict[str, Any]) -> bool:
            return set(ids) <= set(_write_capacity(data, table))

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        while True:
            data = _admin_metrics(env, window="LastHour")
            if both_flushed(data) or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert both_flushed(data), (
            f"within {FLUSH_DEADLINE_SECONDS}s the metrics lacked a PutItem "
            f"write capacity row under {table} for each account: "
            f"{_write_capacity(data, table)}"
        )
        # The server lists the rows in no fixed order, so compare them sorted.
        while True:
            time.sleep(SETTLE_SECONDS)
            again = _admin_metrics(env, window="LastHour")
            if _canonical(again) == _canonical(data):
                break
            assert time.monotonic() < deadline, (
                f"the metrics were still changing {FLUSH_DEADLINE_SECONDS}s "
                f"after the test's requests"
            )
            data = again
        yield env, table, ids, data
    finally:
        for account_id, client, user in accounts:
            try:
                client.delete_table(TableName=table)
                wait_for_deleted(client, table)
            except client.exceptions.ResourceNotFoundException:
                pass
            finally:
                mgmt.delete_user(account_id, user)
                mgmt.delete_account(account_id)


def test_two_accounts_with_the_same_table_name_get_separate_rows(flushed):
    _, table, ids, data = flushed
    sums = _write_capacity(data, table)
    assert sums == {ids[0]: float(WRITES[0]), ids[1]: float(WRITES[1])}, sums


def test_the_account_filter_returns_that_accounts_rows_only(flushed):
    env, table, ids, _ = flushed
    data = _admin_metrics(env, window="LastHour", account_id=ids[1])
    accounts = {_dimension(m, "AccountId") for m in data["metrics"]}
    assert accounts == {ids[1]}, accounts
    assert _write_capacity(data, table) == {ids[1]: float(WRITES[1])}
