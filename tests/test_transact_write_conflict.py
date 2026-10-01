# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Concurrent TransactWriteItems that contend for the same items.

Amazon DynamoDB answers a write transaction that loses a conflict with
TransactionCanceledException (HTTP 400). The contended items carry a
TransactionConflict reason and the other items carry None. Under this load
Amazon DynamoDB also answers a few transactions (about 1 in 1000) with
InternalServerError, so a small fixed number of 5xx is tolerated. Every
committed transaction must apply in full.

ExtendDB on PostgreSQL queues contending transactions instead of canceling
them, so there the cancellation shape checks have nothing to check and these
tests prove only that contention never surfaces as a 5xx. The mapping of a
database-detected deadlock to TransactionConflict is pinned by the storage
tests in crates/storage-postgres/tests/twi_conflict.rs.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

WORKERS = 4
TXNS_PER_WORKER = 25
# Bounds the run on a server that stalls on each conflict.
TIME_BUDGET_S = 20.0
# Amazon DynamoDB returned at most 3 InternalServerError in one run of 100
# contended transactions; they come in bursts.
MAX_SERVER_ERRORS = 5
CONFLICT_MESSAGE = "Transaction is ongoing for the item"
CANCEL_PREFIX = (
    "Transaction cancelled, please refer cancellation reasons for specific reasons ["
)


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every 5xx and every cancellation is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=WORKERS * 2,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _add_one(table: str, pk: str) -> dict:
    return {
        "Update": {
            "TableName": table,
            "Key": {"pk": {"S": pk}},
            "UpdateExpression": "ADD n :one",
            "ExpressionAttributeValues": {":one": {"N": "1"}},
        }
    }


def _run(client, builders) -> tuple[int, int, list[dict]]:
    """Run each builder on WORKERS / len(builders) threads at once.

    Returns the attempts, the committed transactions, and one record per failure.
    """
    lock = threading.Lock()
    attempts = 0
    committed = 0
    failures: list[dict] = []
    start = threading.Barrier(WORKERS)
    deadline = time.monotonic() + TIME_BUDGET_S

    def worker(which: int):
        nonlocal attempts, committed
        start.wait()
        for _ in range(TXNS_PER_WORKER):
            if time.monotonic() > deadline:
                return
            with lock:
                attempts += 1
            try:
                client.transact_write_items(TransactItems=builders[which]())
                with lock:
                    committed += 1
            except ClientError as e:
                r = e.response
                with lock:
                    failures.append(
                        {
                            "builder": which,
                            "status": r["ResponseMetadata"]["HTTPStatusCode"],
                            "code": r["Error"]["Code"],
                            "message": r["Error"]["Message"],
                            "reasons": r.get("CancellationReasons"),
                        }
                    )

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [
            pool.submit(worker, i % len(builders)) for i in range(WORKERS)
        ]
        for f in futures:
            f.result()
    return attempts, committed, failures


def _assert_conflict_shape(failures: list[dict], n_items: int) -> tuple[list[list[str]], int]:
    """Assert the failures are conflict cancellations.

    Returns the reason codes of each cancellation and the number of 5xx.
    """
    server_errors = [f for f in failures if f["status"] >= 500]
    cancellations = [f for f in failures if f["status"] < 500]
    for f in server_errors:
        assert (f["status"], f["code"]) == (500, "InternalServerError"), f
    assert len(server_errors) <= MAX_SERVER_ERRORS, (
        f"{len(server_errors)} 5xx in {len(failures)} failures, first: {server_errors[0]}"
    )
    shapes = []
    for f in cancellations:
        assert f["status"] == 400, f
        assert f["code"] == "TransactionCanceledException", f
        reasons = f["reasons"]
        assert reasons is not None and len(reasons) == n_items, f
        codes = [r["Code"] for r in reasons]
        assert set(codes) <= {"None", "TransactionConflict"}, f
        assert "TransactionConflict" in codes, f
        for r in reasons:
            if r["Code"] == "TransactionConflict":
                assert r.get("Message") == CONFLICT_MESSAGE, f
            else:
                assert "Message" not in r, f
        assert f["message"] == CANCEL_PREFIX + ", ".join(codes) + "]", f
        shapes.append(codes)
    return shapes, len(server_errors)


def _counter(client, table: str, pk: str) -> int:
    item = client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True)
    return int(item["Item"]["n"]["N"])


def test_opposite_order_transactions_cancel_instead_of_failing(
    dynamodb_client, raw_client, table
):
    """Two items updated in opposite orders by many clients at once."""
    a, b = f"hot-a-{uuid.uuid4().hex[:8]}", f"hot-b-{uuid.uuid4().hex[:8]}"
    for pk in (a, b):
        dynamodb_client.put_item(TableName=table, Item={"pk": {"S": pk}, "n": {"N": "0"}})

    attempts, committed, failures = _run(
        raw_client,
        [
            lambda: [_add_one(table, a), _add_one(table, b)],
            lambda: [_add_one(table, b), _add_one(table, a)],
        ],
    )

    _, n_5xx = _assert_conflict_shape(failures, 2)
    assert committed + len(failures) == attempts
    # Every committed transaction applied both updates and no canceled one
    # applied any. A 5xx leaves the outcome unknown, so it may count either way.
    n_a, n_b = _counter(dynamodb_client, table, a), _counter(dynamodb_client, table, b)
    assert n_a == n_b, (n_a, n_b)
    assert committed <= n_a <= committed + n_5xx, (n_a, committed, n_5xx)


def test_conflict_reason_names_only_the_contended_item(
    dynamodb_client, raw_client, table
):
    """One shared item plus one private item per transaction, in either position."""
    hot = f"hot-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": hot}, "n": {"N": "0"}})

    def private() -> str:
        return f"own-{uuid.uuid4().hex}"

    attempts, committed, failures = _run(
        raw_client,
        [
            lambda: [_add_one(table, hot), _add_one(table, private())],
            lambda: [_add_one(table, private()), _add_one(table, hot)],
        ],
    )

    shapes, n_5xx = _assert_conflict_shape(failures, 2)
    cancellations = [f for f in failures if f["status"] < 500]
    for codes, f in zip(shapes, cancellations):
        # The private item never conflicts, so only the shared item is named.
        expected = ["TransactionConflict", "None"]
        assert codes == (expected if f["builder"] == 0 else expected[::-1]), f
    assert committed + len(failures) == attempts
    assert committed <= _counter(dynamodb_client, table, hot) <= committed + n_5xx

