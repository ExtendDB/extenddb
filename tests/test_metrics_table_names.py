# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Metrics record only valid table names (issue #728).

A request that fails validation can carry any string as its ``TableName``.
The metrics used to record that string as a dimension value, so after
rejected CreateTable calls the metrics table held names such as
``x";DROP TABLE tables;--`` and ``a`` U+0000 ``b``. The metrics now carry a
table name only when the operation acts on the table its ``TableName`` names
and the name is valid, a table ARN is reduced to its name, ``UserErrors``
carries none (CloudWatch reports it per account and Region), and
``SuccessfulRequestLatency`` counts successful requests only.

The test sends the rejected requests, a successful ListBackups filtered by a
table that does not exist, a successful CreateBackup that names a table of its
own by its ARN, a successful BatchWriteItem with an extra top-level
``TableName``, and last a successful PutItem on that table. It waits for the
PutItem's and the CreateBackup's rows, then reads until two reads in a row
are the same, so the flush that wrote them has finished (see ``flushed``).

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

from conftest import wait_for_active, wait_for_deleted
from raw_signed_request import signed_post

# The flush worker persists a data point once it is older than the worker's
# 60-second interval, at the next tick, so a point can take up to two
# intervals to reach the metrics table.
FLUSH_DEADLINE_SECONDS = 180
POLL_SECONDS = 5
# After the awaited rows, the fixture reads the metrics every SETTLE_SECONDS
# until two reads in a row are the same; see `flushed`.
SETTLE_SECONDS = 5
# How long the teardown waits for the test's backup to leave CREATING.
BACKUP_DEADLINE_SECONDS = 60

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


def _dimension(metric: dict[str, Any], key: str) -> str | None:
    for d in metric["dimensions"]:
        if isinstance(d, dict) and key in d:
            return d[key]
    return None


def _rows(data: dict[str, Any], metric: str, operation: str) -> list[dict[str, Any]]:
    return [
        m
        for m in data["metrics"]
        if m["metric"] == metric and _dimension(m, "Operation") == operation
    ]


def _table_dimensions(data: dict[str, Any], metric: str, operation: str) -> list[Any]:
    """The TableName of each row of ``metric`` for ``operation``, None if absent."""
    return sorted(
        (_dimension(m, "TableName") for m in _rows(data, metric, operation)),
        key=lambda n: (n is not None, n or ""),
    )


def _canonical(data: dict[str, Any]) -> list[str]:
    return sorted(json.dumps(m, sort_keys=True) for m in data["metrics"])


def _settled_metrics(
    endpoint: str, data: dict[str, Any], deadline: float
) -> dict[str, Any]:
    """Read the metrics until two reads SETTLE_SECONDS apart are the same."""
    while True:
        time.sleep(SETTLE_SECONDS)
        again = _admin_metrics(endpoint, window="LastHour")
        # The server lists the rows in no fixed order, so compare them sorted.
        if _canonical(again) == _canonical(data):
            return again
        assert time.monotonic() < deadline, (
            f"the metrics were still changing {FLUSH_DEADLINE_SECONDS}s after "
            f"the test's requests"
        )
        data = again


def _delete_backup_when_created(client, backup_arn: str) -> None:
    """Delete the backup once it has left CREATING, which DeleteBackup refuses."""
    deadline = time.monotonic() + BACKUP_DEADLINE_SECONDS
    while True:
        status = client.describe_backup(BackupArn=backup_arn)[
            "BackupDescription"
        ]["BackupDetails"]["BackupStatus"]
        if status != "CREATING" or time.monotonic() > deadline:
            break
        time.sleep(1)
    client.delete_backup(BackupArn=backup_arn)


def _raw_post(endpoint: str, operation: str, body: dict[str, Any]) -> requests.Response:
    """A signed request boto3 would not send, as the fixture's client's caller."""
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    return signed_post(
        endpoint,
        creds.access_key,
        creds.secret_key,
        os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        operation,
        body,
        session_token=creds.token,
    )


@pytest.fixture(scope="module")
def flushed(endpoint_url: str, dynamodb_client, dynamodb_client_no_validation):
    """The test's requests, persisted by the flush.

    Yields the names that must not appear, the test table's name, and the
    metrics an admin reads once the last request's row is in them.
    """
    suffix = uuid.uuid4().hex[:8]
    sentinel = f"metrics-names-ok-{suffix}"
    # A valid name on a CreateTable that fails for another reason: before the
    # fix it gave a UserErrors and a SuccessfulRequestLatency row under it.
    refused_valid = f"metrics-names-refused-{suffix}"
    # A valid name sent as an extra top-level TableName on a BatchWriteItem,
    # which has no such member and writes to the test table.
    decoy = f"metrics-names-decoy-{suffix}"
    # A valid name of no table, as the filter of a ListBackups, which succeeds
    # with an empty list.
    missing = f"metrics-names-missing-{suffix}"

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
    listed = dynamodb_client.list_backups(TableName=missing)
    assert listed["BackupSummaries"] == [], listed

    dynamodb_client.create_table(TableName=sentinel, **KEY)
    backup_arn = None
    try:
        wait_for_active(dynamodb_client, sentinel)
        arn = dynamodb_client.describe_table(TableName=sentinel)["Table"]["TableArn"]
        backup_arn = dynamodb_client.create_backup(
            TableName=arn, BackupName=f"metrics-names-{suffix}"
        )["BackupDetails"]["BackupArn"]
        resp = _raw_post(
            endpoint_url,
            "BatchWriteItem",
            {
                "RequestItems": {
                    sentinel: [{"PutRequest": {"Item": {"pk": {"S": "batch"}}}}]
                },
                "TableName": decoy,
            },
        )
        assert resp.status_code == 200, f"BatchWriteItem: {resp.status_code} {resp.text}"
        # Last, so its row reaching the metrics means the earlier requests'
        # minute buckets have been flushed too.
        dynamodb_client.put_item(TableName=sentinel, Item={"pk": {"S": "1"}})

        def sentinel_rows_flushed(data: dict[str, Any]) -> bool:
            put_item = any(
                _dimension(m, "TableName") == sentinel
                for m in _rows(data, "ConsumedWriteCapacityUnits", "PutItem")
            )
            # Under the bare name or, if the ARN were not reduced, the ARN, so
            # that a regression fails the ARN test rather than this wait.
            create_backup = any(
                _dimension(m, "TableName") in (sentinel, arn)
                for m in _rows(data, "SuccessfulRequestLatency", "CreateBackup")
            )
            return put_item and create_backup

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        while True:
            data = _admin_metrics(endpoint_url, window="LastHour")
            if sentinel_rows_flushed(data) or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert sentinel_rows_flushed(data), (
            f"within {FLUSH_DEADLINE_SECONDS}s the metrics lacked the PutItem row "
            f"under {sentinel} or the CreateBackup latency row under it or its "
            f"ARN. Either the flush did not persist it, or it was recorded under "
            f"another table name or none. PutItem ConsumedWriteCapacityUnits rows: "
            f"{_table_dimensions(data, 'ConsumedWriteCapacityUnits', 'PutItem')}; "
            f"CreateBackup SuccessfulRequestLatency rows: "
            f"{_table_dimensions(data, 'SuccessfulRequestLatency', 'CreateBackup')}"
        )
        # One flush tick writes every pending row of the minute, one statement
        # per row, and on a shared server that includes the rows of every
        # other test running against it, so a read can land partway through
        # it. The rows with no table name (the BatchWriteItem's, the
        # ListBackups', the rejected CreateTables') cannot be waited for by
        # name. So read until two reads SETTLE_SECONDS apart are the same:
        # nothing was written between them, so the flush that wrote the
        # awaited rows has finished.
        data = _settled_metrics(
            endpoint_url, data, time.monotonic() + FLUSH_DEADLINE_SECONDS
        )
        yield (*INVALID_NAMES, refused_valid, decoy, missing), sentinel, data
    finally:
        try:
            if backup_arn is not None:
                _delete_backup_when_created(dynamodb_client, backup_arn)
        finally:
            dynamodb_client.delete_table(TableName=sentinel)
            wait_for_deleted(dynamodb_client, sentinel)


def test_rejected_requests_leave_no_table_name_in_the_metrics(flushed) -> None:
    sent, _, data = flushed
    leaked = _table_names(data) & set(sent)
    assert not leaked, f"metrics recorded table names they must not: {leaked!r}"


def test_user_errors_carry_no_table_name(flushed) -> None:
    _, _, data = flushed
    user_errors = [m for m in data["metrics"] if m["metric"] == "UserErrors"]
    assert user_errors, "the rejected requests recorded no UserErrors"
    with_table = [
        m["dimensions"]
        for m in user_errors
        if any(isinstance(d, dict) and "TableName" in d for d in m["dimensions"])
    ]
    assert not with_table, f"UserErrors with a TableName: {with_table}"


def test_a_table_named_by_its_arn_is_reported_under_its_name(flushed) -> None:
    _, sentinel, data = flushed
    backed_up = _rows(data, "SuccessfulRequestLatency", "CreateBackup")
    assert sentinel in {_dimension(m, "TableName") for m in backed_up}, (
        f"no SuccessfulRequestLatency for CreateBackup under {sentinel}: {backed_up}"
    )
    arns = sorted(n for n in _table_names(data) if n.startswith("arn:"))
    assert not arns, f"metrics recorded table ARNs as table names: {arns}"
