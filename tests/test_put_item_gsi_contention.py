# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Many unconditional PutItem calls on one key of a table with a GSI.

Amazon DynamoDB applies the puts one after another and answers 200 to each
one; the last writer wins, and the index holds the final item's entry only.
That is what the service answered to every call of this shape, up to 64
writers on one key.

On the MongoDB backend a put on a table with an index writes the item and its
index entries in one transaction, so concurrent puts on one key abort each
other and are retried. The retry loop stops after ten seconds, so under load
ExtendDB may answer 503 ServiceUnavailable where the service answers 200, and
a client with the SDK default retry mode retries it. What this test holds the
backend to is the contract the retry loop gives: every call answers 200 or 503
ServiceUnavailable, never 400 TransactionConflictException and never 500; the
stored item is one writer's item, whole; and the index holds exactly one entry
for it. These tests fail on a server that reports a lost race to the client as
a 400 or a 500, or that sheds more than a tenth of the puts.
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

from conftest import call_with_handshake_retry, configured_backend, scoped_table

WRITERS = 32
PUTS_PER_WRITER = 63
# The only failure a contended put may answer, as status, code and message.
# The server sheds a put that has been losing to other writers for
# CONTENTION_DEADLINE (10 s).
SHED = "503 ServiceUnavailable Service is temporarily unavailable"


@pytest.fixture(scope="module")
def raw_clients(endpoint_url):
    """One client per writer thread, none of which retries, so every failure is seen.

    A botocore client shared by several threads can fail a request with a TLS
    error under concurrent use. With retries off that error would fail the
    test, so each writer gets a client of its own.
    """
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "config": Config(retries={"total_max_attempts": 1, "mode": "standard"}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return [boto3.client(**kwargs) for _ in range(WRITERS)]


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


def _hammer(clients: list, table: str, pk: str) -> list[str]:
    """WRITERS threads each put PUTS_PER_WRITER items on `pk`, each with its own client.

    Returns the failures, each as status, code and message. A TLS handshake
    that fails before a put is sent is retried by `call_with_handshake_retry`,
    since the clients retry nothing by themselves.
    """
    start = threading.Barrier(WRITERS, timeout=30)
    failures: list[str] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def run(i: int):
        client = clients[i]
        try:
            start.wait()
            for n in range(PUTS_PER_WRITER):
                item = {"pk": {"S": pk}, "gk": {"S": f"g-{pk}-{i}-{n}"}, "w": {"S": f"w{i}-{n}"}}
                try:
                    call_with_handshake_retry(client.put_item, TableName=table, Item=item)
                except ClientError as e:
                    r = e.response
                    code = (
                        f"{r['ResponseMetadata']['HTTPStatusCode']} "
                        f"{r['Error']['Code']} {r['Error']['Message']}"
                    )
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


@pytest.mark.skipif(
    configured_backend() == "cassandra",
    reason="Cassandra answers some of these puts with TransactionConflictException (#802)",
)
def test_unconditional_puts_on_one_key_of_a_gsi_table_commit_or_are_shed(
    dynamodb_client, raw_clients, gsi_table
):
    pk = f"hot-{uuid.uuid4().hex[:8]}"
    failures = _hammer(raw_clients, gsi_table, pk)
    total = WRITERS * PUTS_PER_WRITER
    committed = total - len(failures)
    # How many of the contended puts are shed depends on the load on the
    # server's host, so the count is reported and bounded: a shed put is the
    # only failure a caller can get, and at least nine in ten of the puts
    # (1,814 of 2,016) must commit, so at most 202 are shed. The floor does
    # not hold the attempt budget: a server with a budget of 50 instead of
    # 1,000 can pass it. Four of the five live tests in
    # crates/storage-mongodb/src/data_engine_live_tests_contention.rs hold the
    # budget, because each of them fails with a budget of 50:
    #   live_concurrent_unconditional_puts_on_one_key_of_a_gsi_table_all_commit
    #   live_a_single_item_write_that_reaches_the_deadline_answers_a_transient_error
    #   live_a_transaction_that_reaches_the_deadline_answers_a_canceled_transaction
    #   live_unconditional_put_that_spends_its_attempt_budget_gives_up_with_a_transient_error
    floor = total * 9 // 10
    print(f"{len(failures)} of {total} puts were shed, {committed} committed")
    assert set(failures) <= {SHED}, (
        f"{len(failures)} of {total} puts failed with something other than a shed put: "
        f"{sorted(set(failures))}"
    )
    assert committed >= floor, (
        f"only {committed} of {total} puts committed, under the floor of {floor}: more "
        "than a tenth were shed. The live tests in "
        "crates/storage-mongodb/src/data_engine_live_tests_contention.rs hold the "
        "attempt budget"
    )
    final = dynamodb_client.get_item(
        TableName=gsi_table, Key={"pk": {"S": pk}}, ConsistentRead=True
    )["Item"]
    # The stored item is one writer's item, whole: its `gk` is the one that
    # writer sent with that `w`, not another put's.
    writer, _, nth = final["w"]["S"][1:].partition("-")
    assert 0 <= int(writer) < WRITERS, final
    assert 0 <= int(nth) < PUTS_PER_WRITER, final
    assert final["gk"]["S"] == f"g-{pk}-{writer}-{nth}", final
    # The GSI is eventually consistent on Amazon DynamoDB, so poll.
    want = [final["gk"]["S"]]
    deadline = time.monotonic() + 30
    while (got := _gsi_entries(dynamodb_client, gsi_table, pk)) != want and time.monotonic() < deadline:
        time.sleep(0.5)
    assert got == want, f"GSI entries for {pk}: want {want}, got {got}"
