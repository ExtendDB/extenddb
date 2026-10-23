# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Many unconditional PutItem calls on one key of a table with a GSI.

Amazon DynamoDB applies the puts one after another and answers 200 to each
one; the last writer wins, and the index holds the final item's entry only.
On the MongoDB backend a put on a table with an index writes the item and its
index entries in one transaction, so concurrent puts on one key abort each
other and are retried. These tests fail on a server whose retry budget runs
out under this contention and reports the lost race to the client.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

WRITERS = 32
PUTS_PER_WRITER = 63


@pytest.fixture(scope="module")
def raw_client(endpoint_url):
    """A client that never retries, so every failure is seen."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=WRITERS * 2,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def gsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gk-index",
                "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


def _hammer(client, table: str, pk: str) -> list[str]:
    """WRITERS threads each put PUTS_PER_WRITER items on `pk`; return the failures."""
    start = threading.Barrier(WRITERS, timeout=30)
    failures: list[str] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def run(i: int):
        try:
            start.wait()
            for n in range(PUTS_PER_WRITER):
                item = {"pk": {"S": pk}, "gk": {"S": f"g-{pk}-{i}-{n}"}, "w": {"S": f"w{i}-{n}"}}
                try:
                    client.put_item(TableName=table, Item=item)
                except ClientError as e:
                    code = f"{e.response['ResponseMetadata']['HTTPStatusCode']} {e.response['Error']['Code']}"
                    with lock:
                        failures.append(code)
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(WRITERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return failures


def _gsi_entries(client, table: str, pk: str) -> list[str]:
    out: list[str] = []
    kwargs = {"TableName": table, "IndexName": "gk-index"}
    while True:
        resp = client.scan(**kwargs)
        out.extend(i["gk"]["S"] for i in resp["Items"] if i["pk"]["S"] == pk)
        if "LastEvaluatedKey" not in resp:
            return sorted(out)
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def test_unconditional_puts_on_one_key_of_a_gsi_table_all_succeed(
    dynamodb_client, raw_client, gsi_table
):
    pk = f"hot-{uuid.uuid4().hex[:8]}"
    failures = _hammer(raw_client, gsi_table, pk)
    total = WRITERS * PUTS_PER_WRITER
    assert not failures, f"{len(failures)} of {total} puts failed: {sorted(set(failures))}"
    final = dynamodb_client.get_item(
        TableName=gsi_table, Key={"pk": {"S": pk}}, ConsistentRead=True
    )["Item"]
    # The GSI is eventually consistent on Amazon DynamoDB, so poll.
    want = [final["gk"]["S"]]
    deadline = time.monotonic() + 30
    while (got := _gsi_entries(dynamodb_client, gsi_table, pk)) != want and time.monotonic() < deadline:
        time.sleep(0.5)
    assert got == want, f"GSI entries for {pk}: want {want}, got {got}"
