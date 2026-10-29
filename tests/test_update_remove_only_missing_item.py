# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateItem whose actions only remove data, on a key that holds no item.

Amazon DynamoDB writes nothing for such a request: no item is created, no
stream record is emitted, and the response carries no Attributes under any
ReturnValues. SET and ADD, alone or mixed with a removal, and an update with
no actions at all, still create the item. On a table with a GSI and an LSI no
index entry appears, ItemCollectionMetrics is reported from the key, and
ReturnConsumedCapacity INDEXES charges the table only. Measured 2026-10-24 in
us-east-1 on a hash table, a hash and range table, and a hash and range table
with a GSI and an LSI (issue #478).
"""

from __future__ import annotations

import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import _poll_interval, scoped_table, wait_for_active, wait_for_deleted

ALL_RETURN_VALUES = ["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"]


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def hash_range_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        yield name


@pytest.fixture(params=["hash", "hash_range"])
def table(request, hash_table, hash_range_table):
    """(table name, key factory) for both key shapes."""
    if request.param == "hash":
        return hash_table, lambda: {"pk": {"S": f"r-{uuid.uuid4().hex[:12]}"}}
    return hash_range_table, lambda: {
        "pk": {"S": f"r-{uuid.uuid4().hex[:12]}"},
        "sk": {"S": "s1"},
    }


def _get(client, name, key):
    return client.get_item(TableName=name, Key=key, ConsistentRead=True)


def _assert_nothing_written(client, name, key, resp):
    assert "Attributes" not in resp, resp
    assert "Item" not in _get(client, name, key), "the item was created"


class TestRemoveOnlyOnMissingKey:
    @pytest.mark.parametrize("return_values", ALL_RETURN_VALUES)
    def test_remove_creates_no_item(self, dynamodb_client, table, return_values):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a",
            ReturnValues=return_values,
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    @pytest.mark.parametrize("return_values", ["NONE", "ALL_NEW", "UPDATED_NEW"])
    def test_remove_two_attributes(self, dynamodb_client, table, return_values):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a, b",
            ReturnValues=return_values,
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    def test_remove_nested_path(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name, Key=key, UpdateExpression="REMOVE a.b[0]"
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    @pytest.mark.parametrize("return_values", ["NONE", "ALL_NEW"])
    def test_delete_from_set(self, dynamodb_client, table, return_values):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="DELETE s :v",
            ExpressionAttributeValues={":v": {"SS": ["x"]}},
            ReturnValues=return_values,
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    def test_remove_and_delete_together(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a DELETE s :v",
            ExpressionAttributeValues={":v": {"SS": ["x"]}},
            ReturnValues="ALL_NEW",
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    def test_remove_with_attribute_not_exists_condition(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a",
            ConditionExpression="attribute_not_exists(pk)",
            ReturnValues="ALL_NEW",
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    def test_remove_with_attribute_exists_condition_fails(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        with pytest.raises(ClientError) as ei:
            dynamodb_client.update_item(
                TableName=name,
                Key=key,
                UpdateExpression="REMOVE a",
                ConditionExpression="attribute_exists(pk)",
            )
        assert ei.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
        assert "Item" not in _get(dynamodb_client, name, key)

    def test_consumed_capacity_is_still_charged(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a",
            ReturnConsumedCapacity="TOTAL",
        )
        assert resp["ConsumedCapacity"] == {"CapacityUnits": 1.0, "TableName": name}
        _assert_nothing_written(dynamodb_client, name, key, resp)


class TestLegacyDeleteOnMissingKey:
    @pytest.mark.parametrize("return_values", ["NONE", "ALL_NEW"])
    def test_delete_without_value(self, dynamodb_client, table, return_values):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            AttributeUpdates={"a": {"Action": "DELETE"}},
            ReturnValues=return_values,
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    def test_two_deletes_without_value(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            AttributeUpdates={"a": {"Action": "DELETE"}, "b": {"Action": "DELETE"}},
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)

    @pytest.mark.parametrize("return_values", ["NONE", "ALL_NEW"])
    def test_delete_with_set_value(self, dynamodb_client, table, return_values):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            AttributeUpdates={"s": {"Action": "DELETE", "Value": {"SS": ["x"]}}},
            ReturnValues=return_values,
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)


class TestUpdatesThatStillCreate:
    """Controls: every other update shape creates the item as before."""

    def test_set_only(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="SET a = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "a": {"S": "x"}}

    def test_add_only(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="ADD n :v",
            ExpressionAttributeValues={":v": {"N": "1"}},
        )
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "n": {"N": "1"}}

    def test_remove_plus_set(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a SET b = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"] == {**key, "b": {"S": "x"}}
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "b": {"S": "x"}}

    def test_remove_plus_add(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a ADD n :v",
            ExpressionAttributeValues={":v": {"N": "1"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"] == {**key, "n": {"N": "1"}}
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "n": {"N": "1"}}

    def test_delete_from_set_plus_add(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="DELETE s :s ADD n :n",
            ExpressionAttributeValues={":s": {"SS": ["x"]}, ":n": {"N": "1"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"] == {**key, "n": {"N": "1"}}
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "n": {"N": "1"}}

    def test_legacy_delete_plus_put(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            AttributeUpdates={
                "a": {"Action": "DELETE"},
                "b": {"Action": "PUT", "Value": {"S": "x"}},
            },
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"] == {**key, "b": {"S": "x"}}
        assert _get(dynamodb_client, name, key)["Item"] == {**key, "b": {"S": "x"}}

    def test_empty_attribute_updates_creates_key_only_item(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        resp = dynamodb_client.update_item(TableName=name, Key=key, AttributeUpdates={})
        assert "Attributes" not in resp
        assert _get(dynamodb_client, name, key)["Item"] == key

    def test_remove_on_existing_item_still_removes(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.put_item(
            TableName=name, Item={**key, "a": {"S": "1"}, "b": {"S": "2"}}
        )
        resp = dynamodb_client.update_item(
            TableName=name, Key=key, UpdateExpression="REMOVE a", ReturnValues="ALL_NEW"
        )
        assert resp["Attributes"] == {**key, "b": {"S": "2"}}
        resp = dynamodb_client.update_item(
            TableName=name, Key=key, UpdateExpression="REMOVE b", ReturnValues="ALL_NEW"
        )
        assert resp["Attributes"] == key
        assert _get(dynamodb_client, name, key)["Item"] == key


class TestAfterAWriteToTheTable:
    """The same requests on a table that has already taken a write.

    A backend may serve updates on a table it has written to through a
    different path from the first one, so each test writes a control item
    first, then sends the removal-only update on a missing key."""

    def _write_control(self, client, name, new_key):
        control = new_key()
        client.update_item(
            TableName=name,
            Key=control,
            UpdateExpression="SET a = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
        return control

    def _scan_keys(self, client, name, keys):
        pks = [k["pk"]["S"] for k in keys]
        resp = client.scan(
            TableName=name,
            FilterExpression="pk IN (:k0, :k1)",
            ExpressionAttributeValues={":k0": {"S": pks[0]}, ":k1": {"S": pks[1]}},
            ConsistentRead=True,
        )
        return sorted(item["pk"]["S"] for item in resp["Items"])

    def test_remove_after_a_write(self, dynamodb_client, table):
        name, new_key = table
        control = self._write_control(dynamodb_client, name, new_key)
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name, Key=key, UpdateExpression="REMOVE a", ReturnValues="ALL_NEW"
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)
        assert self._scan_keys(dynamodb_client, name, [control, key]) == [control["pk"]["S"]]

    def test_conditional_remove_after_a_write(self, dynamodb_client, table):
        name, new_key = table
        control = self._write_control(dynamodb_client, name, new_key)
        key = new_key()
        resp = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="REMOVE a",
            ConditionExpression="attribute_not_exists(pk)",
            ReturnValues="ALL_NEW",
        )
        _assert_nothing_written(dynamodb_client, name, key, resp)
        assert self._scan_keys(dynamodb_client, name, [control, key]) == [control["pk"]["S"]]


# ---------------------------------------------------------------------------
# A table with a GSI and an LSI: no index entry, and the capacity and item
# collection metrics the service returns for a removal-only update.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def index_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g1pk", "AttributeType": "S"},
            {"AttributeName": "l1sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "g1pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l1sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "present"},
                "sk": {"S": "s1"},
                "g1pk": {"S": "g-present"},
                "l1sk": {"S": "l-present"},
            },
        )
        yield name


def _index_key():
    return {"pk": {"S": f"r-{uuid.uuid4().hex[:12]}"}, "sk": {"S": "s1"}}


class TestTableWithSecondaryIndexes:
    def test_removing_index_keys_creates_no_item_or_index_entry(
        self, dynamodb_client, index_table
    ):
        key = _index_key()
        resp = dynamodb_client.update_item(
            TableName=index_table,
            Key=key,
            UpdateExpression="REMOVE g1pk, l1sk",
            ReturnValues="ALL_NEW",
        )
        _assert_nothing_written(dynamodb_client, index_table, key, resp)
        lsi = dynamodb_client.query(
            TableName=index_table,
            IndexName="lsi1",
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": key["pk"]},
        )
        assert lsi["Count"] == 0
        gsi = dynamodb_client.scan(
            TableName=index_table,
            IndexName="gsi1",
            FilterExpression="pk = :p",
            ExpressionAttributeValues={":p": key["pk"]},
        )
        assert gsi["Count"] == 0

    def test_item_collection_metrics_come_from_the_key(self, dynamodb_client, index_table):
        key = _index_key()
        resp = dynamodb_client.update_item(
            TableName=index_table,
            Key=key,
            UpdateExpression="REMOVE a",
            ReturnItemCollectionMetrics="SIZE",
        )
        assert resp["ItemCollectionMetrics"] == {
            "ItemCollectionKey": {"pk": key["pk"]},
            "SizeEstimateRangeGB": [0.0, 1.0],
        }
        _assert_nothing_written(dynamodb_client, index_table, key, resp)

    def test_consumed_capacity_indexes_charges_the_table_only(
        self, dynamodb_client, index_table
    ):
        key = _index_key()
        resp = dynamodb_client.update_item(
            TableName=index_table,
            Key=key,
            UpdateExpression="REMOVE g1pk",
            ReturnConsumedCapacity="INDEXES",
        )
        assert resp["ConsumedCapacity"] == {
            "CapacityUnits": 1.0,
            "Table": {"CapacityUnits": 1.0},
            "TableName": index_table,
        }
        _assert_nothing_written(dynamodb_client, index_table, key, resp)


# ---------------------------------------------------------------------------
# Streams: a REMOVE-only update on a missing key emits no record.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def streams_client(endpoint_url):
    kwargs: dict = {
        "service_name": "dynamodbstreams",
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)


@pytest.fixture(scope="module")
def stream_table(dynamodb_client):
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    wait_for_active(dynamodb_client, name)
    arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
    yield name, arn
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


def _records_for(streams_client, arn, pks: set[str]) -> list[dict]:
    desc = streams_client.describe_stream(StreamArn=arn)["StreamDescription"]
    found = []
    for shard in desc["Shards"]:
        it = streams_client.get_shard_iterator(
            StreamArn=arn, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        for _ in range(20):
            if not it:
                break
            page = streams_client.get_records(ShardIterator=it, Limit=100)
            found.extend(
                r for r in page.get("Records", []) if r["dynamodb"]["Keys"]["pk"]["S"] in pks
            )
            it = page.get("NextShardIterator")
            if not page.get("Records"):
                break
    return found


def test_remove_only_on_missing_key_emits_no_stream_record(
    dynamodb_client, streams_client, stream_table
):
    name, arn = stream_table
    removed = f"r-{uuid.uuid4().hex[:12]}"
    control = f"r-{uuid.uuid4().hex[:12]}"
    dynamodb_client.update_item(
        TableName=name, Key={"pk": {"S": removed}}, UpdateExpression="REMOVE a"
    )
    dynamodb_client.update_item(
        TableName=name,
        Key={"pk": {"S": control}},
        UpdateExpression="SET a = :v",
        ExpressionAttributeValues={":v": {"S": "x"}},
    )
    # The control record proves the stream is delivering before the absence
    # of a record for the REMOVE-only key means anything.
    deadline = time.monotonic() + 10.0
    records: list[dict] = []
    while time.monotonic() < deadline:
        records = _records_for(streams_client, arn, {removed, control})
        if any(r["dynamodb"]["Keys"]["pk"]["S"] == control for r in records):
            break
        time.sleep(_poll_interval())
    events = [(r["dynamodb"]["Keys"]["pk"]["S"], r["eventName"]) for r in records]
    assert events == [(control, "INSERT")], events
    assert "Item" not in _get(dynamodb_client, name, {"pk": {"S": removed}})
