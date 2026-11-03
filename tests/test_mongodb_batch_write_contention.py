# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""BatchWriteItem on the MongoDB backend when one of its items stays contended.

A mongosh session holds an item inside a MongoDB transaction it does not
commit, so every attempt of a write to that item conflicts until the session
aborts. The server stops retrying a contended write after CONTENTION_DEADLINE
(10 s), measured once for the whole batch. A request the server gives up on is
returned in UnprocessedItems and the batch answers 200, which is how Amazon
DynamoDB answers a batch with a request it cannot apply: measured with
TransactWriteItems calls holding one item, the held item's request came back
in UnprocessedItems and the other request of the batch was applied. The
requests already applied are not applied again.

The holds run through mongosh in the container the MongoDB test runner names
in EXTENDDB_TEST_MONGODB_CONTAINER, so these tests run only under
devtools/run-mongodb-tests.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid

import boto3
import pytest
from botocore.config import Config

from conftest import scoped_table

# The server's contention deadline, and the time past it within which a batch
# that reached it answers. A hold lasts longer than both together, so the
# deadline, not the end of a hold, is what ends a contended write.
DEADLINE = 10
SLACK = 3
HOLD = DEADLINE + SLACK + 2


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    return container


def _client(endpoint_url, retries: bool):
    """A client of its own; with `retries` it keeps the SDK's default retry mode."""
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if not retries:
        kwargs["config"] = Config(retries={"total_max_attempts": 1, "mode": "standard"})
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def gsi_table(dynamodb_client):
    """A table with a GSI, so every write to it runs in a MongoDB transaction."""
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


def _item(pk: str, w: str) -> dict:
    return {"pk": {"S": pk}, "gk": {"S": f"g-{pk}"}, "w": {"S": w}}


def _w(client, table: str, pk: str) -> str | None:
    item = client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True).get(
        "Item"
    )
    return item["w"]["S"] if item else None


class Hold:
    """Hold the item at `pk` inside a MongoDB transaction for `seconds`, then abort it."""

    def __init__(self, container: str, table: str, pk: str, seconds: float):
        script = f"""
const catalog = db.getSiblingDB("extenddb_catalog");
const table = catalog.tables.findOne({{"_id.table_name": {json.dumps(table)}}});
const session = db.getMongo().startSession();
session.startTransaction({{readConcern: {{level: "snapshot"}}, writeConcern: {{w: "majority"}}}});
const coll = session.getDatabase("extenddb_data").getCollection("_ddb_" + table.table_id);
const res = coll.updateOne({{pk: {json.dumps(pk)}}}, {{$set: {{"item_data.held": {{S: "yes"}}}}}});
print(JSON.stringify({{held: res.modifiedCount}}));
sleep({int(seconds * 1000)});
session.abortTransaction();
print(JSON.stringify({{released: true}}));
"""
        self.proc = subprocess.Popen(
            ["docker", "exec", container, "mongosh", "--quiet", "--eval", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        line = self.proc.stdout.readline()
        if json.loads(line or "{}").get("held") != 1:
            self.proc.kill()
            pytest.fail(f"could not hold {pk}: {line!r} {self.proc.stderr.read()!r}")

    def wait(self) -> None:
        out, err = self.proc.communicate(timeout=HOLD + 30)
        assert self.proc.returncode == 0, err
        assert '"released":true' in out, out


def test_a_contended_request_is_returned_unprocessed_and_the_rest_is_not_resent(
    dynamodb_client, endpoint_url, mongodb_container, gsi_table
):
    """The shape of a batch whose resend lost another client's write.

    The batch deletes A and puts B, which is held past the deadline. Five
    seconds in, another client puts A again. The batch client keeps the SDK's
    default retry mode, which resends a whole batch that answered 503. The
    batch must answer 200 with only the put of B unprocessed, so the delete of
    A is not sent again and the other client's A stays.
    """
    a, b = f"a-{uuid.uuid4().hex[:8]}", f"b-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=gsi_table, Item=_item(a, "v0"))
    dynamodb_client.put_item(TableName=gsi_table, Item=_item(b, "v0"))
    batch_client = _client(endpoint_url, retries=True)
    other_client = _client(endpoint_url, retries=True)

    hold = Hold(mongodb_container, gsi_table, b, HOLD)
    other = threading.Timer(
        5, lambda: other_client.put_item(TableName=gsi_table, Item=_item(a, "v2"))
    )
    other.start()
    started = time.monotonic()
    resp = batch_client.batch_write_item(
        RequestItems={
            gsi_table: [
                {"DeleteRequest": {"Key": {"pk": {"S": a}}}},
                {"PutRequest": {"Item": _item(b, "v1")}},
            ]
        }
    )
    elapsed = time.monotonic() - started
    other.join()
    hold.wait()

    print(f"the batch answered in {elapsed:.2f} s: {resp.get('UnprocessedItems')}")
    assert resp["UnprocessedItems"] == {gsi_table: [{"PutRequest": {"Item": _item(b, "v1")}}]}
    assert elapsed < DEADLINE + SLACK, f"the batch answered in {elapsed:.2f} s"
    assert _w(dynamodb_client, gsi_table, a) == "v2", "the other client's put of A was lost"
    assert _w(dynamodb_client, gsi_table, b) == "v0", "the unprocessed put of B was applied"


def test_the_requests_of_a_batch_share_one_contention_deadline(
    dynamodb_client, endpoint_url, mongodb_container, gsi_table
):
    """A request that reaches the deadline late in a batch does not get a deadline of its own.

    The batch puts B1, held for 6 s, and then B2, held past the deadline. B1
    commits once its hold ends. B2 is tried after that and gives up when the
    deadline the batch started with passes, so the batch answers within that
    deadline, with B2 unprocessed, rather than about 6 s plus a deadline later.
    """
    b1, b2 = f"b1-{uuid.uuid4().hex[:8]}", f"b2-{uuid.uuid4().hex[:8]}"
    for pk in (b1, b2):
        dynamodb_client.put_item(TableName=gsi_table, Item=_item(pk, "v0"))
    client = _client(endpoint_url, retries=False)

    hold_b2 = Hold(mongodb_container, gsi_table, b2, HOLD)
    hold_b1 = Hold(mongodb_container, gsi_table, b1, 6)
    started = time.monotonic()
    resp = client.batch_write_item(
        RequestItems={
            gsi_table: [
                {"PutRequest": {"Item": _item(b1, "v1")}},
                {"PutRequest": {"Item": _item(b2, "v1")}},
            ]
        }
    )
    elapsed = time.monotonic() - started
    hold_b1.wait()
    hold_b2.wait()

    print(f"the batch answered in {elapsed:.2f} s: {resp.get('UnprocessedItems')}")
    assert resp["UnprocessedItems"] == {gsi_table: [{"PutRequest": {"Item": _item(b2, "v1")}}]}
    assert elapsed < DEADLINE + SLACK, f"the batch answered in {elapsed:.2f} s"
    assert _w(dynamodb_client, gsi_table, b1) == "v1"
    assert _w(dynamodb_client, gsi_table, b2) == "v0"
