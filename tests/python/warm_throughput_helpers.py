# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared constants, waits and the `tables` fixture for the WarmThroughput
wire tests (`test_warm_throughput.py`, `test_warm_throughput_updates.py`).
Every sentence here was measured against Amazon DynamoDB on 2026-10-23."""

from __future__ import annotations

import time

import pytest

from helpers import unique_name, wait_for_active, wait_for_deleted

ON_DEMAND_DEFAULTS = {"ReadUnitsPerSecond": 12000, "WriteUnitsPerSecond": 4000}
PREFIX = "One or more parameter values were invalid: Requested "
ON_DEMAND_MIN = (
    " is lower than initial throughput for OnDemand. See: "
    "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/"
    "on-demand-capacity-mode.html#on-demand-capacity-mode-initial"
)
DECREASE = " is lower than current WarmThroughput, decreasing WarmThroughput is not supported"
ONLY_OPERATION = (
    "One or more parameter values were invalid: WarmThroughput must be the only operation "
    "in the request"
)
TABLE_CLASS_ONLY = (
    "One or more parameter values were invalid: TableClass modification must be the only "
    "operation in the request"
)
DELETION_PROTECTION_ONLY = (
    "One or more parameter values were invalid: DeletionProtection modification must be the "
    "only operation in the request"
)
ON_DEMAND_NEEDS_BILLING = (
    "One or more parameter values were invalid: OnDemandThroughput can only be combined with "
    "BillingMode in an UpdateTable request"
)


def _sentence(member: str, subject: str, suffix: str) -> str:
    """`Requested <member> for WarmThroughput for <table | index name><suffix>`."""
    return f"{PREFIX}{member} for WarmThroughput for {subject}{suffix}"

PK = [{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}]
ATTRS = [
    {"AttributeName": "PK", "AttributeType": "S"},
    {"AttributeName": "SK", "AttributeType": "S"},
    {"AttributeName": "G", "AttributeType": "S"},
]
GSI = {
    "IndexName": "gsi",
    "KeySchema": [{"AttributeName": "G", "KeyType": "HASH"}],
    "Projection": {"ProjectionType": "ALL"},
}
LSI = {
    "IndexName": "lsi",
    "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "G", "KeyType": "RANGE"}],
    "Projection": {"ProjectionType": "KEYS_ONLY"},
}


def _wait_all_active(client, name: str, timeout: float = 900.0) -> dict:
    """The description once the table and every GSI are ACTIVE."""
    wait_for_active(client, name, timeout)
    deadline = time.monotonic() + timeout
    while True:
        t = client.describe_table(TableName=name)["Table"]
        if all(g["IndexStatus"] == "ACTIVE" for g in t.get("GlobalSecondaryIndexes", [])):
            return t
        if time.monotonic() > deadline:
            raise TimeoutError(f"indexes of {name} did not become ACTIVE")
        time.sleep(1)


def _wait_warm_active(client, name: str, timeout: float = 30.0) -> dict:
    """The description once the table's and every GSI's WarmThroughput is ACTIVE."""
    deadline = time.monotonic() + timeout
    while True:
        t = client.describe_table(TableName=name)["Table"]
        statuses = [t["WarmThroughput"]["Status"]] + [
            g["WarmThroughput"]["Status"] for g in t.get("GlobalSecondaryIndexes", [])
        ]
        if all(s == "ACTIVE" for s in statuses):
            return t
        if time.monotonic() > deadline:
            raise TimeoutError(f"WarmThroughput of {name} stayed {statuses}")
        time.sleep(0.2)


def _validation(exc_info, expected: str) -> None:
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == expected, err["Message"]


@pytest.fixture()
def tables(dynamodb_client):
    """Register created table names for deletion after the test."""
    names: list[str] = []
    yield names
    for name in names:
        try:
            dynamodb_client.update_table(TableName=name, DeletionProtectionEnabled=False)
        except Exception:
            pass
        try:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name, 600)
        except Exception:
            pass


def _create(client, tables, **kwargs) -> tuple[str, dict]:
    name = unique_name("wt")
    tables.append(name)
    # The G attribute is defined only when an index keys on it: an unused
    # definition is refused by the service and by ExtendDB.
    indexed = "GlobalSecondaryIndexes" in kwargs or "LocalSecondaryIndexes" in kwargs
    attrs = ATTRS if indexed else ATTRS[:2]
    args = {"TableName": name, "KeySchema": PK, "AttributeDefinitions": attrs}
    args.update(kwargs)
    return name, client.create_table(**args)["TableDescription"]
