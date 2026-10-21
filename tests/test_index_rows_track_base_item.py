# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Secondary-index rows follow the base item through every write path.

On Amazon DynamoDB an index never reports an item the base table does not
hold, and never misses one it does, once the index has caught up. The tests
here pin that for each way an item can change: a write that keeps the index
key, a write that moves it, a transactional Put, Update and Delete, a
concurrent delete and put of one key, and ordinary writes on an item that a
transaction created. The LSI is read with ConsistentRead; the GSI is polled
because it is eventually consistent on Amazon DynamoDB.
"""

from __future__ import annotations

import os
import threading
import time

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import scoped_table

RACE_ROUNDS = 25
WRITERS = 2


def _s(name: str) -> dict:
    return {"AttributeName": name, "AttributeType": "S"}


def _k(name: str, kind: str) -> dict:
    return {"AttributeName": name, "KeyType": kind}


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
def indexed_table(dynamodb_client):
    """pk/sk base table with one LSI on lk and one GSI on gk, both ALL."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[_s("pk"), _s("sk"), _s("lk"), _s("gk")],
        key_schema=[_k("pk", "HASH"), _k("sk", "RANGE")],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lk-index",
                "KeySchema": [_k("pk", "HASH"), _k("lk", "RANGE")],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gk-index",
                "KeySchema": [_k("gk", "HASH")],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _item(pk: str, sk: str, lk: str, gk: str, data: str) -> dict:
    return {
        "pk": {"S": pk},
        "sk": {"S": sk},
        "lk": {"S": lk},
        "gk": {"S": gk},
        "data": {"S": data},
    }


def _lsi(client, table: str, pk: str) -> list[tuple[str, str]]:
    """(lk, data) for every LSI entry under pk, strongly consistent."""
    items = client.query(
        TableName=table,
        IndexName="lk-index",
        ConsistentRead=True,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": pk}},
    )["Items"]
    return sorted((i["lk"]["S"], i["data"]["S"]) for i in items)


def _gsi_now(client, table: str, gk: str) -> list[tuple[str, str]]:
    items = client.query(
        TableName=table,
        IndexName="gk-index",
        KeyConditionExpression="gk = :gk",
        ExpressionAttributeValues={":gk": {"S": gk}},
    )["Items"]
    return sorted((i["sk"]["S"], i["data"]["S"]) for i in items)


def _gsi(client, table: str, gk: str, want: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Poll the GSI until it reports `want` or 30 seconds pass; return what it reports."""
    deadline = time.monotonic() + 30
    while (got := _gsi_now(client, table, gk)) != want and time.monotonic() < deadline:
        time.sleep(0.25)
    return got


def _base(client, table: str, pk: str, sk: str) -> dict | None:
    return client.get_item(
        TableName=table, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True
    ).get("Item")


class TestWritesThatKeepTheIndexKey:
    """A write that leaves the index key alone keeps the item in the index,
    with the new projection."""

    def test_update_of_a_non_key_attribute(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("keep-u", "1", "a", "g-keep-u", "v1"))
        dynamodb_client.update_item(
            TableName=indexed_table,
            Key={"pk": {"S": "keep-u"}, "sk": {"S": "1"}},
            UpdateExpression="SET #d = :v",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":v": {"S": "v2"}},
        )
        assert _lsi(dynamodb_client, indexed_table, "keep-u") == [("a", "v2")]
        assert _gsi(dynamodb_client, indexed_table, "g-keep-u", [("1", "v2")]) == [("1", "v2")]

    def test_put_over_the_item_with_the_same_keys(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("keep-p", "1", "a", "g-keep-p", "v1"))
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("keep-p", "1", "a", "g-keep-p", "v2"))
        assert _lsi(dynamodb_client, indexed_table, "keep-p") == [("a", "v2")]
        assert _gsi(dynamodb_client, indexed_table, "g-keep-p", [("1", "v2")]) == [("1", "v2")]

    def test_transactional_update_of_a_non_key_attribute(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("keep-t", "1", "a", "g-keep-t", "v1"))
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": indexed_table,
                        "Key": {"pk": {"S": "keep-t"}, "sk": {"S": "1"}},
                        "UpdateExpression": "SET #d = :v",
                        "ExpressionAttributeNames": {"#d": "data"},
                        "ExpressionAttributeValues": {":v": {"S": "v2"}},
                    }
                }
            ]
        )
        assert _lsi(dynamodb_client, indexed_table, "keep-t") == [("a", "v2")]
        assert _gsi(dynamodb_client, indexed_table, "g-keep-t", [("1", "v2")]) == [("1", "v2")]


class TestTransactionalWritesThatMoveOrRemoveTheItem:
    """Transactional Put, Update and Delete replace or remove the index rows
    of the image they overwrite."""

    def test_put_over_an_item_whose_index_keys_change(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("txp", "1", "old", "g-txp-old", "v1"))
        dynamodb_client.transact_write_items(
            TransactItems=[{"Put": {"TableName": indexed_table, "Item": _item("txp", "1", "new", "g-txp-new", "v2")}}]
        )
        assert _lsi(dynamodb_client, indexed_table, "txp") == [("new", "v2")]
        assert _gsi(dynamodb_client, indexed_table, "g-txp-new", [("1", "v2")]) == [("1", "v2")]
        assert _gsi(dynamodb_client, indexed_table, "g-txp-old", []) == []

    def test_update_that_changes_the_index_keys(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("txu", "1", "old", "g-txu-old", "v1"))
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": indexed_table,
                        "Key": {"pk": {"S": "txu"}, "sk": {"S": "1"}},
                        "UpdateExpression": "SET lk = :l, gk = :g",
                        "ExpressionAttributeValues": {":l": {"S": "new"}, ":g": {"S": "g-txu-new"}},
                    }
                }
            ]
        )
        assert _lsi(dynamodb_client, indexed_table, "txu") == [("new", "v1")]
        assert _gsi(dynamodb_client, indexed_table, "g-txu-new", [("1", "v1")]) == [("1", "v1")]
        assert _gsi(dynamodb_client, indexed_table, "g-txu-old", []) == []

    def test_delete_removes_the_index_rows(self, dynamodb_client, indexed_table):
        dynamodb_client.put_item(TableName=indexed_table, Item=_item("txd", "1", "a", "g-txd", "v1"))
        assert _lsi(dynamodb_client, indexed_table, "txd") == [("a", "v1")]
        dynamodb_client.transact_write_items(
            TransactItems=[{"Delete": {"TableName": indexed_table, "Key": {"pk": {"S": "txd"}, "sk": {"S": "1"}}}}]
        )
        assert _base(dynamodb_client, indexed_table, "txd", "1") is None
        assert _lsi(dynamodb_client, indexed_table, "txd") == []
        assert _gsi(dynamodb_client, indexed_table, "g-txd", []) == []


class TestOrdinaryWritesOnATransactionCreatedItem:
    """An item a transaction created takes ordinary writes like any other."""

    @pytest.fixture(scope="class")
    def created(self, dynamodb_client, hash_table):
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": name}, "v": {"N": "1"}}}}
                for name in ("tc-update", "tc-put", "tc-delete", "tc-cond-delete")
            ]
        )

    def test_update_item(self, dynamodb_client, hash_table, created):
        dynamodb_client.update_item(
            TableName=hash_table,
            Key={"pk": {"S": "tc-update"}},
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"N": "2"}},
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "tc-update"}}, ConsistentRead=True)["Item"]
        assert got == {"pk": {"S": "tc-update"}, "v": {"N": "2"}}

    def test_put_item(self, dynamodb_client, hash_table, created):
        dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "tc-put"}, "v": {"N": "2"}})
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "tc-put"}}, ConsistentRead=True)["Item"]
        assert got == {"pk": {"S": "tc-put"}, "v": {"N": "2"}}

    def test_delete_item(self, dynamodb_client, hash_table, created):
        dynamodb_client.delete_item(TableName=hash_table, Key={"pk": {"S": "tc-delete"}})
        assert "Item" not in dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "tc-delete"}}, ConsistentRead=True
        )

    def test_conditional_delete_item(self, dynamodb_client, hash_table, created):
        dynamodb_client.delete_item(
            TableName=hash_table,
            Key={"pk": {"S": "tc-cond-delete"}},
            ConditionExpression="v = :v",
            ExpressionAttributeValues={":v": {"N": "1"}},
        )
        assert "Item" not in dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "tc-cond-delete"}}, ConsistentRead=True
        )


def test_a_delete_racing_a_put_leaves_the_index_agreeing_with_the_base_item(
    dynamodb_client, raw_client, indexed_table
):
    """Whichever of a DeleteItem and a PutItem on one key lands last, the LSI
    reports the item exactly when the base table holds it, with its lk."""
    disagreements = []
    for n in range(RACE_ROUNDS):
        pk = f"race-{n}"
        dynamodb_client.put_item(TableName=indexed_table, Item=_item(pk, "1", "old", f"g-{pk}", "v1"))
        errors: list[ClientError] = []

        def put():
            try:
                raw_client.put_item(TableName=indexed_table, Item=_item(pk, "1", "new", f"g-{pk}", "v2"))
            except ClientError as e:  # pragma: no cover - reported below
                errors.append(e)

        def delete():
            try:
                raw_client.delete_item(TableName=indexed_table, Key={"pk": {"S": pk}, "sk": {"S": "1"}})
            except ClientError as e:  # pragma: no cover - reported below
                errors.append(e)

        threads = [threading.Thread(target=put), threading.Thread(target=delete)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, [e.response["Error"] for e in errors]

        base = _base(dynamodb_client, indexed_table, pk, "1")
        want = [(base["lk"]["S"], base["data"]["S"])] if base else []
        got = _lsi(dynamodb_client, indexed_table, pk)
        if got != want:
            disagreements.append((pk, want, got))
    assert not disagreements, f"LSI disagrees with the base table (pk, base, lsi): {disagreements}"
