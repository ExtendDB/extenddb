# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Metrics record only valid table names (issue #728).

A request that fails validation can carry any string as its ``TableName``.
The metrics used to record that string as a dimension value, so after
rejected CreateTable calls the metrics table held names such as
``x";DROP TABLE tables;--`` and ``a`` U+0000 ``b``. The metrics now carry a
table name only when it is valid, ``UserErrors`` carries none (CloudWatch
reports it per account and Region), and ``SuccessfulRequestLatency`` counts
successful requests only.

The test sends the rejected requests, then a successful PutItem on a table of
its own. Once the metrics flush has written that table's rows, the earlier
requests have been flushed too, and none of their names may appear.

Prerequisites:
  - extenddb running on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest
import requests

from conftest import wait_for_active, wait_for_deleted

# The flush worker persists a data point once it is older than the worker's
# 60-second interval, at the next tick, so a point can take up to two
# intervals to reach the metrics table.
FLUSH_DEADLINE_SECONDS = 180
POLL_SECONDS = 5

pytestmark = pytest.mark.slow

# Table names that break the table name rules; each CreateTable is refused.
INVALID_NAMES = (
    'x";DROP TABLE tables;--',
    "a\u0000b",
    "\u00e9\u00e9\u00e9",
    "a" * 256,
    "ab",
)

KEY = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "BillingMode": "PAY_PER_REQUEST",
}


def _admin_metrics(endpoint: str, **params: str) -> dict[str, Any]:
    auth = (
        os.environ.get("EXTENDDB_ADMIN_USER", "admin"),
        os.environ.get("EXTENDDB_ADMIN_PASSWORD", ""),
    )
    resp = requests.get(
        f"{endpoint}/metrics", params=params, auth=auth, timeout=10, verify=False
    )
    assert resp.status_code == 200, f"/metrics: {resp.status_code} {resp.text[:200]}"
    return resp.json()


def _table_names(data: dict[str, Any]) -> set[str]:
    return {
        d["TableName"]
        for m in data["metrics"]
        for d in m["dimensions"]
        if isinstance(d, dict) and "TableName" in d
    }


@pytest.fixture(scope="module")
def flushed(endpoint_url: str, dynamodb_client, dynamodb_client_no_validation):
    """Rejected requests, then a successful PutItem, persisted by the flush.

    Yields the names the rejected requests carried and the metrics an admin
    reads once the PutItem's table is in them.
    """
    suffix = uuid.uuid4().hex[:8]
    sentinel = f"metrics-names-ok-{suffix}"
    # A valid name on a CreateTable that fails for another reason: before the
    # fix it gave a UserErrors and a SuccessfulRequestLatency row under it.
    refused_valid = f"metrics-names-refused-{suffix}"

    for name in INVALID_NAMES:
        with pytest.raises(dynamodb_client_no_validation.exceptions.ClientError):
            dynamodb_client_no_validation.create_table(TableName=name, **KEY)
    with pytest.raises(dynamodb_client_no_validation.exceptions.ClientError):
        dynamodb_client_no_validation.create_table(
            TableName=refused_valid,
            AttributeDefinitions=KEY["AttributeDefinitions"],
            KeySchema=[],
            BillingMode="PAY_PER_REQUEST",
        )

    dynamodb_client.create_table(TableName=sentinel, **KEY)
    try:
        wait_for_active(dynamodb_client, sentinel)
        dynamodb_client.put_item(TableName=sentinel, Item={"pk": {"S": "1"}})

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        while True:
            data = _admin_metrics(endpoint_url, window="LastHour")
            if sentinel in _table_names(data) or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert sentinel in _table_names(data), (
            f"the metrics flush did not persist {sentinel} within "
            f"{FLUSH_DEADLINE_SECONDS}s"
        )
        yield (*INVALID_NAMES, refused_valid), data
    finally:
        dynamodb_client.delete_table(TableName=sentinel)
        wait_for_deleted(dynamodb_client, sentinel)


def test_rejected_requests_leave_no_table_name_in_the_metrics(flushed) -> None:
    sent, data = flushed
    leaked = _table_names(data) & set(sent)
    assert not leaked, f"metrics recorded table names of rejected requests: {leaked!r}"


def test_user_errors_carry_no_table_name(flushed) -> None:
    _, data = flushed
    user_errors = [m for m in data["metrics"] if m["metric"] == "UserErrors"]
    assert user_errors, "the rejected requests recorded no UserErrors"
    with_table = [
        m["dimensions"]
        for m in user_errors
        if any(isinstance(d, dict) and "TableName" in d for d in m["dimensions"])
    ]
    assert not with_table, f"UserErrors with a TableName: {with_table}"
