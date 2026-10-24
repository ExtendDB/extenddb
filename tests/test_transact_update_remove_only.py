# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TransactWriteItems Update whose actions only remove, on a key that holds no
item.

Amazon DynamoDB writes nothing for such an update: no item is created, no index
entry appears and no stream record is emitted, while the transaction's other
items are written as before and ConsumedCapacity reports the same 2.0 capacity
units as a SET. The update's own condition still runs, so a condition that fails
cancels the transaction. SET and ADD, alone or mixed with a removal, still
create the item. Measured 2026-10-24 in us-east-1 on a hash table and a hash
and range table (issue #510).
"""

from __future__ import annotations

import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import _poll_interval, scoped_table, wait_for_active, wait_for_deleted


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
        return hash_table, lambda: {"pk": {"S": f"t-{uuid.uuid4().hex[:12]}"}}
    return hash_range_table, lambda: {
        "pk": {"S": f"t-{uuid.uuid4().hex[:12]}"},
        "sk": {"S": "s1"},
    }


def _get(client, name, key):
    return client.get_item(TableName=name, Key=key, ConsistentRead=True)


def _assert_absent(client, name, key):
    assert "Item" not in _get(client, name, key), "the item was created"


def _update(name, key, expr, **kwargs):
    op = {"TableName": name, "Key": key, "UpdateExpression": expr}
    op.update(kwargs)
    return {"Update": op}


REMOVAL_ONLY = [
    pytest.param("REMOVE a", {}, id="remove-one"),
    pytest.param("REMOVE a, b", {}, id="remove-two"),
    pytest.param("REMOVE a.b[0]", {}, id="remove-nested-path"),
    pytest.param(
        "DELETE s :v", {"ExpressionAttributeValues": {":v": {"SS": ["x"]}}}, id="delete-set"
    ),
    pytest.param(
        "REMOVE a DELETE s :v",
        {"ExpressionAttributeValues": {":v": {"SS": ["x"]}}},
        id="remove-and-delete",
    ),
]


class TestRemovalOnlyUpdateOnMissingKey:
    @pytest.mark.parametrize("expr,extra", REMOVAL_ONLY)
    def test_nothing_is_written(self, dynamodb_client, table, expr, extra):
        name, new_key = table
        key = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, key, expr, **extra)]
        )
        _assert_absent(dynamodb_client, name, key)

    def test_a_condition_that_passes_still_writes_nothing(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[
                _update(
                    name,
                    key,
                    "REMOVE a",
                    ConditionExpression="attribute_not_exists(pk)",
                )
            ]
        )
        _assert_absent(dynamodb_client, name, key)

    def test_a_condition_that_fails_cancels_the_transaction(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        other = new_key()
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _update(
                        name,
                        key,
                        "REMOVE a",
                        ConditionExpression="attribute_exists(pk)",
                    ),
                    _update(
                        name,
                        other,
                        "SET a = :v",
                        ExpressionAttributeValues={":v": {"S": "x"}},
                    ),
                ]
            )
        err = excinfo.value.response
        assert err["Error"]["Code"] == "TransactionCanceledException"
        codes = [r["Code"] for r in err["CancellationReasons"]]
        assert codes == ["ConditionalCheckFailed", "None"], codes
        _assert_absent(dynamodb_client, name, key)
        # The transaction is all or nothing, so the SET item is not there either.
        _assert_absent(dynamodb_client, name, other)

    def test_consumed_capacity_matches_a_set(self, dynamodb_client, table):
        name, new_key = table
        removal = dynamodb_client.transact_write_items(
            TransactItems=[_update(name, new_key(), "REMOVE a")],
            ReturnConsumedCapacity="TOTAL",
        )
        control = dynamodb_client.transact_write_items(
            TransactItems=[
                _update(
                    name,
                    new_key(),
                    "SET a = :v",
                    ExpressionAttributeValues={":v": {"S": "x"}},
                )
            ],
            ReturnConsumedCapacity="TOTAL",
        )
        assert removal["ConsumedCapacity"] == [
            {"TableName": name, "CapacityUnits": 2.0, "WriteCapacityUnits": 2.0}
        ]
        assert removal["ConsumedCapacity"] == control["ConsumedCapacity"]

    def test_the_other_items_of_the_transaction_are_written(self, dynamodb_client, table):
        name, new_key = table
        removed = new_key()
        written = new_key()
        put = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[
                _update(name, removed, "REMOVE a"),
                _update(
                    name,
                    written,
                    "SET a = :v",
                    ExpressionAttributeValues={":v": {"S": "x"}},
                ),
                {"Put": {"TableName": name, "Item": {**put, "a": {"S": "p"}}}},
            ]
        )
        _assert_absent(dynamodb_client, name, removed)
        assert _get(dynamodb_client, name, written)["Item"]["a"] == {"S": "x"}
        assert _get(dynamodb_client, name, put)["Item"]["a"] == {"S": "p"}

    def test_a_removal_only_update_beside_a_condition_check(self, dynamodb_client, table):
        name, new_key = table
        removed = new_key()
        checked = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[
                _update(name, removed, "REMOVE a"),
                {
                    "ConditionCheck": {
                        "TableName": name,
                        "Key": checked,
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
            ]
        )
        _assert_absent(dynamodb_client, name, removed)
        _assert_absent(dynamodb_client, name, checked)

    def test_a_condition_check_that_fails_leaves_the_key_absent(
        self, dynamodb_client, table
    ):
        name, new_key = table
        removed = new_key()
        checked = new_key()
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _update(name, removed, "REMOVE a"),
                    {
                        "ConditionCheck": {
                            "TableName": name,
                            "Key": checked,
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    },
                ]
            )
        err = excinfo.value.response
        assert err["Error"]["Code"] == "TransactionCanceledException"
        codes = [r["Code"] for r in err["CancellationReasons"]]
        assert codes == ["None", "ConditionalCheckFailed"], codes
        _assert_absent(dynamodb_client, name, removed)
        _assert_absent(dynamodb_client, name, checked)

    def test_a_removal_only_update_and_a_condition_check_on_one_key_is_refused(
        self, dynamodb_client, table
    ):
        name, new_key = table
        key = new_key()
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _update(name, key, "REMOVE a"),
                    {
                        "ConditionCheck": {
                            "TableName": name,
                            "Key": key,
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }
                    },
                ]
            )
        err = excinfo.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert (
            err["Message"]
            == "Transaction request cannot include multiple operations on one item"
        )
        _assert_absent(dynamodb_client, name, key)

    def test_return_values_on_condition_check_failure_carries_no_item(
        self, dynamodb_client, table
    ):
        name, new_key = table
        key = new_key()
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _update(
                        name,
                        key,
                        "REMOVE a",
                        ConditionExpression="attribute_exists(pk)",
                        ReturnValuesOnConditionCheckFailure="ALL_OLD",
                    )
                ]
            )
        reasons = excinfo.value.response["CancellationReasons"]
        assert reasons[0]["Code"] == "ConditionalCheckFailed"
        assert "Item" not in reasons[0], reasons[0]
        _assert_absent(dynamodb_client, name, key)

    def test_return_values_on_condition_check_failure_on_an_existing_item(
        self, dynamodb_client, table
    ):
        name, new_key = table
        key = new_key()
        dynamodb_client.put_item(TableName=name, Item={**key, "a": {"S": "1"}})
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    _update(
                        name,
                        key,
                        "REMOVE a",
                        ConditionExpression="attribute_not_exists(pk)",
                        ReturnValuesOnConditionCheckFailure="ALL_OLD",
                    )
                ]
            )
        reasons = excinfo.value.response["CancellationReasons"]
        assert reasons[0]["Code"] == "ConditionalCheckFailed"
        assert reasons[0]["Item"] == {**key, "a": {"S": "1"}}

    def test_a_client_request_token_replay_writes_nothing(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        items = [_update(name, key, "REMOVE a")]
        token = uuid.uuid4().hex
        first = dynamodb_client.transact_write_items(
            TransactItems=items, ClientRequestToken=token, ReturnConsumedCapacity="TOTAL"
        )
        replay = dynamodb_client.transact_write_items(
            TransactItems=items, ClientRequestToken=token, ReturnConsumedCapacity="TOTAL"
        )
        assert first["ConsumedCapacity"] == [
            {"TableName": name, "CapacityUnits": 2.0, "WriteCapacityUnits": 2.0}
        ]
        # A replay is served as a transactional read and its capacity says so.
        assert replay["ConsumedCapacity"] == [
            {"TableName": name, "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}
        ]
        _assert_absent(dynamodb_client, name, key)

    def test_a_token_reused_with_another_body_is_refused(self, dynamodb_client, table):
        name, new_key = table
        token = uuid.uuid4().hex
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, new_key(), "REMOVE a")],
            ClientRequestToken=token,
        )
        other = new_key()
        with pytest.raises(ClientError) as excinfo:
            dynamodb_client.transact_write_items(
                TransactItems=[_update(name, other, "REMOVE a")],
                ClientRequestToken=token,
            )
        assert (
            excinfo.value.response["Error"]["Code"] == "IdempotentParameterMismatchException"
        )
        _assert_absent(dynamodb_client, name, other)

    def test_the_key_can_be_written_afterwards(self, dynamodb_client, table):
        """The update must leave nothing reserved behind on the key."""
        name, new_key = table
        key = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, key, "REMOVE a")]
        )
        _assert_absent(dynamodb_client, name, key)
        dynamodb_client.transact_write_items(
            TransactItems=[
                _update(
                    name, key, "SET a = :v", ExpressionAttributeValues={":v": {"S": "x"}}
                )
            ]
        )
        assert _get(dynamodb_client, name, key)["Item"]["a"] == {"S": "x"}


class TestUpdatesThatStillCreate:
    @pytest.mark.parametrize(
        "expr,extra",
        [
            pytest.param(
                "REMOVE a SET b = :v",
                {"ExpressionAttributeValues": {":v": {"S": "x"}}},
                id="remove-and-set",
            ),
            pytest.param(
                "REMOVE a ADD n :n",
                {"ExpressionAttributeValues": {":n": {"N": "1"}}},
                id="remove-and-add",
            ),
            pytest.param(
                "DELETE s :s ADD n :n",
                {"ExpressionAttributeValues": {":s": {"SS": ["x"]}, ":n": {"N": "1"}}},
                id="delete-and-add",
            ),
        ],
    )
    def test_a_write_action_beside_a_removal_creates_the_item(
        self, dynamodb_client, table, expr, extra
    ):
        name, new_key = table
        key = new_key()
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, key, expr, **extra)]
        )
        assert "Item" in _get(dynamodb_client, name, key)


class TestExistingItem:
    def test_a_removal_on_an_existing_item_still_removes(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.put_item(
            TableName=name, Item={**key, "a": {"S": "1"}, "b": {"S": "2"}}
        )
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, key, "REMOVE a")]
        )
        item = _get(dynamodb_client, name, key)["Item"]
        assert "a" not in item
        assert item["b"] == {"S": "2"}

    def test_removing_the_last_attribute_keeps_the_item(self, dynamodb_client, table):
        name, new_key = table
        key = new_key()
        dynamodb_client.put_item(TableName=name, Item={**key, "a": {"S": "1"}})
        dynamodb_client.transact_write_items(
            TransactItems=[_update(name, key, "REMOVE a")]
        )
        assert _get(dynamodb_client, name, key)["Item"] == key


# ---------------------------------------------------------------------------
# A table with a GSI and an LSI: no index entry, and the capacity and item
# collection metrics the service returns.
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
        yield name


def _index_key():
    return {"pk": {"S": f"t-{uuid.uuid4().hex[:12]}"}, "sk": {"S": "s1"}}


class TestTableWithSecondaryIndexes:
    def test_no_index_entry_appears(self, dynamodb_client, index_table):
        key = _index_key()
        dynamodb_client.transact_write_items(
            TransactItems=[_update(index_table, key, "REMOVE g1pk, l1sk, a")]
        )
        _assert_absent(dynamodb_client, index_table, key)
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

    def test_consumed_capacity_indexes_charges_the_table_only(
        self, dynamodb_client, index_table
    ):
        key = _index_key()
        resp = dynamodb_client.transact_write_items(
            TransactItems=[_update(index_table, key, "REMOVE g1pk")],
            ReturnConsumedCapacity="INDEXES",
        )
        assert resp["ConsumedCapacity"] == [
            {
                "TableName": index_table,
                "CapacityUnits": 2.0,
                "WriteCapacityUnits": 2.0,
                "Table": {"CapacityUnits": 2.0, "WriteCapacityUnits": 2.0},
            }
        ]
        _assert_absent(dynamodb_client, index_table, key)

    def test_item_collection_metrics_come_from_the_key(self, dynamodb_client, index_table):
        key = _index_key()
        resp = dynamodb_client.transact_write_items(
            TransactItems=[_update(index_table, key, "REMOVE a")],
            ReturnItemCollectionMetrics="SIZE",
        )
        assert resp["ItemCollectionMetrics"] == {
            index_table: [
                {
                    "ItemCollectionKey": {"pk": key["pk"]},
                    "SizeEstimateRangeGB": [0.0, 1.0],
                }
            ]
        }
        _assert_absent(dynamodb_client, index_table, key)


# ---------------------------------------------------------------------------
# Streams: no record for a removal-only update on a missing key.
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


def _configured_backend():
    """The backend under test: `EXTENDDB_TEST_BACKEND` (exported by
    devtools/run-tests), else the `[storage] backend` of the server's config
    file, else None (real DynamoDB, or a server started elsewhere)."""
    explicit = os.environ.get("EXTENDDB_TEST_BACKEND", "").strip()
    if explicit:
        return explicit
    path = os.environ.get("EXTENDDB_CONFIG", "extenddb.toml")
    try:
        with open(path) as f:
            text = f.read()
    except OSError:
        return None
    section = None
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line.startswith("["):
            section = line.strip("[]").strip()
        elif section == "storage" and line.split("=", 1)[0].strip() == "backend":
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


@pytest.mark.skipif(
    _configured_backend() == "cassandra",
    reason="the Cassandra transaction path writes no stream record for any operation",
)
def test_no_stream_record_for_a_removal_only_update(
    dynamodb_client, streams_client, stream_table
):
    name, arn = stream_table
    removed = f"t-{uuid.uuid4().hex[:12]}"
    control = f"t-{uuid.uuid4().hex[:12]}"
    dynamodb_client.transact_write_items(
        TransactItems=[
            _update(name, {"pk": {"S": removed}}, "REMOVE a"),
            _update(
                name,
                {"pk": {"S": control}},
                "SET a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
        ]
    )
    # The control record proves the stream is delivering before the absence of
    # a record for the removal-only key means anything.
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
