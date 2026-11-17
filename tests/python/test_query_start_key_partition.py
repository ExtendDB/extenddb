# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query ExclusiveStartKey from another partition (issue #440).

Every row here was measured on Amazon DynamoDB (2026-10-23 and 2026-10-24).
The sort key rows live in test_query_start_key_range.py (base table) and
test_query_start_key_index.py. With no sort key condition, a start key from
another partition answers ValidationException: the partition sentence for
every index and for a base table with a RANGE key, the hash-only sentence for
a base table without one (which refuses every start key).
"""

from __future__ import annotations

import os
import time

import boto3
import botocore.config
import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

OUTSIDE_BOUNDARIES = (
    "The provided starting key is outside query boundaries based on provided conditions"
)
EXCLUSIVE_INVALID = "The provided exclusive start key is invalid"
SHAPE = "The provided starting key is invalid"


def _put(client, table, pk, sk=None, **extra):
    item = {"pk": {"S": pk}}
    if sk is not None:
        item["sk"] = {"S": sk}
    item.update(extra)
    client.put_item(TableName=table, Item=item)


def _assert_validation(exc_info, message):
    err = exc_info.value.response["Error"]
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Code"] == "ValidationException"
    assert err["Message"] == message


def _expect(client, message, **query):
    with pytest.raises(ClientError) as exc_info:
        client.query(**query)
    _assert_validation(exc_info, message)


def _alice_query(name, esk, **more):
    q = {
        "TableName": name,
        "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "alice"}},
        "ExclusiveStartKey": esk,
    }
    q.update(more)
    return q


def _wait_for_index(client, table, index, key, value, count):
    """GSI writes propagate asynchronously; wait until the index holds them."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        resp = client.query(
            TableName=table,
            IndexName=index,
            KeyConditionExpression=f"{key} = :v",
            ExpressionAttributeValues={":v": {"S": value}},
        )
        if resp["Count"] == count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"index {index} did not reach {count} items")


class TestPartition:
    """Base table Query, start key from another partition."""

    def test_table_with_sort_key(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _put(dynamodb_client, name, "alice", "x")
        _put(dynamodb_client, name, "bob", "x")
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            **_alice_query(name, {"pk": {"S": "bob"}, "sk": {"S": "x"}}),
        )

    def test_hash_only_table(self, table_factory, dynamodb_client):
        name = table_factory()
        _put(dynamodb_client, name, "alice")
        _put(dynamodb_client, name, "bob")
        _expect(dynamodb_client, EXCLUSIVE_INVALID, **_alice_query(name, {"pk": {"S": "bob"}}))

    def test_hash_only_table_refuses_the_queried_partition_too(
        self, table_factory, dynamodb_client
    ):
        # Measured 2026-10-24: pk = alice with the start key {pk: alice}, where
        # alice is the one item, is refused with the same sentence.
        name = table_factory()
        _put(dynamodb_client, name, "alice")
        _expect(
            dynamodb_client,
            EXCLUSIVE_INVALID,
            **_alice_query(name, {"pk": {"S": "alice"}}),
        )

    def test_other_partition_in_reverse_without_a_sort_key_condition(
        self, table_factory, dynamodb_client
    ):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            **_alice_query(
                name, {"pk": {"S": "bob"}, "sk": {"S": "m"}}, ScanIndexForward=False
            ),
        )

    def test_legacy_key_conditions(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=name,
            KeyConditions={
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "alice"}]}
            },
            ExclusiveStartKey={"pk": {"S": "bob"}, "sk": {"S": "x"}},
        )

    @pytest.mark.parametrize(
        "condition,start",
        [("1", "1.0"), ("1e0", "1"), ("01", "1.000"), ("1", "+1")],
    )
    def test_number_key_equal_after_normalisation_is_accepted(
        self, table_factory, dynamodb_client, condition, start
    ):
        name = table_factory(hash_type="N", range_key="sk", range_type="N")
        for sk in ("1", "2", "10"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"N": "1"}, "sk": {"N": sk}})
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"N": condition}},
            ExclusiveStartKey={"pk": {"N": start}, "sk": {"N": "0"}},
        )
        assert resp["Count"] == 3
        assert "LastEvaluatedKey" not in resp

    def test_same_partition_still_paginates(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        for i in range(5):
            _put(dynamodb_client, name, "alice", f"s{i}")
        _put(dynamodb_client, name, "bob", "s0")
        seen, start, pages = [], None, 0
        while True:
            kwargs = {
                "TableName": name,
                "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "alice"}},
                "Limit": 2,
            }
            if start:
                kwargs["ExclusiveStartKey"] = start
            resp = dynamodb_client.query(**kwargs)
            pages += 1
            seen.extend(i["sk"]["S"] for i in resp["Items"])
            start = resp.get("LastEvaluatedKey")
            if not start:
                break
        assert seen == ["s0", "s1", "s2", "s3", "s4"]
        assert pages == 3


@pytest.fixture(scope="module")
def index_table(dynamodb_client):
    """pk+sk table with a GSI (g, gs), a hash-only KEYS_ONLY GSI (h) and an LSI."""
    name = unique_name("esk-idx")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "gs", "AttributeType": "S"},
            {"AttributeName": "h", "AttributeType": "S"},
            {"AttributeName": "ls", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-g",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-h",
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            },
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "by-l",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "ls", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(dynamodb_client, name)
    for pk, g, gs, h in (
        ("p1", "one", "x1", "h1"),
        ("p2", "one", "x2", "h1"),
        ("p3", "one", "x3", "h1"),
        ("p4", "one", "x4", "h1"),
        ("p5", "two", "y1", "h2"),
    ):
        _put(
            dynamodb_client,
            name,
            pk,
            "a",
            g={"S": g},
            gs={"S": gs},
            h={"S": h},
            ls={"S": "a"},
        )
    _wait_for_index(dynamodb_client, name, "by-g", "g", "one", 4)
    _wait_for_index(dynamodb_client, name, "by-h", "h", "h1", 4)
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass


@pytest.fixture(scope="module")
def hash_only_with_gsi(dynamodb_client):
    """Hash-only table `pk` with a GSI (g HASH, gs RANGE)."""
    name = unique_name("esk-h")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "gs", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-g",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(dynamodb_client, name)
    _put(dynamodb_client, name, "p1", g={"S": "one"}, gs={"S": "x"})
    _put(dynamodb_client, name, "p2", g={"S": "two"}, gs={"S": "x"})
    _wait_for_index(dynamodb_client, name, "by-g", "g", "one", 1)
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass


class TestIndexes:
    """The partition compared is the queried key schema's HASH attribute, and
    every index Query uses the partition sentence."""

    def test_gsi_other_index_partition(self, index_table, dynamodb_client):
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=index_table,
            IndexName="by-g",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "one"}},
            ExclusiveStartKey={
                "pk": {"S": "p1"},
                "sk": {"S": "a"},
                "g": {"S": "two"},
                "gs": {"S": "x1"},
            },
        )

    def test_gsi_same_index_partition_other_base_key_is_accepted(
        self, index_table, dynamodb_client
    ):
        # Tiebreak dependency: the start key's index key (one, x1) is also the
        # index key of the item p1. The service orders identical index keys by
        # its own internal order of the base key, so the count on a hand-built
        # start key depends on the fixture's base keys. With p1 stored and p9
        # as the start key's base key, p1 is skipped on the service and here,
        # and the three items x2 to x4 follow. Keep the exact count; a renamed
        # fixture key can change it.
        resp = dynamodb_client.query(
            TableName=index_table,
            IndexName="by-g",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "one"}},
            ExclusiveStartKey={
                "pk": {"S": "p9"},
                "sk": {"S": "a"},
                "g": {"S": "one"},
                "gs": {"S": "x1"},
            },
        )
        assert resp["Count"] == 3
        assert "LastEvaluatedKey" not in resp

    def test_lsi_other_partition(self, index_table, dynamodb_client):
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=index_table,
            IndexName="by-l",
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "p1"}},
            ExclusiveStartKey={"pk": {"S": "p2"}, "sk": {"S": "a"}, "ls": {"S": "a"}},
        )

    def test_hash_only_gsi_other_partition_uses_the_partition_sentence(
        self, index_table, dynamodb_client
    ):
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=index_table,
            IndexName="by-h",
            KeyConditionExpression="h = :h",
            ExpressionAttributeValues={":h": {"S": "h1"}},
            ExclusiveStartKey={"pk": {"S": "p5"}, "sk": {"S": "a"}, "h": {"S": "h2"}},
        )

    def test_hash_only_gsi_same_partition_base_key_not_in_index_is_accepted(
        self, index_table, dynamodb_client
    ):
        resp = dynamodb_client.query(
            TableName=index_table,
            IndexName="by-h",
            KeyConditionExpression="h = :h",
            ExpressionAttributeValues={":h": {"S": "h1"}},
            ExclusiveStartKey={"pk": {"S": "nobody"}, "sk": {"S": "q"}, "h": {"S": "h1"}},
        )
        # Accepted on both targets. The number of items after a hand-built start
        # key depends on the order among identical index keys, which the service
        # decides internally (0 of 5 or 1 of 5 were both observed on 2026-10-24),
        # so only acceptance and the partition of the returned items are asserted.
        assert "LastEvaluatedKey" not in resp
        assert all(item["h"]["S"] == "h1" for item in resp["Items"])

    def test_hash_only_table_gsi_with_range_other_partition(
        self, hash_only_with_gsi, dynamodb_client
    ):
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=hash_only_with_gsi,
            IndexName="by-g",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "one"}},
            ExclusiveStartKey={"pk": {"S": "p2"}, "g": {"S": "two"}, "gs": {"S": "x"}},
        )

    def test_hash_only_table_gsi_with_range_same_partition_is_accepted(
        self, hash_only_with_gsi, dynamodb_client
    ):
        resp = dynamodb_client.query(
            TableName=hash_only_with_gsi,
            IndexName="by-g",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "one"}},
            ExclusiveStartKey={"pk": {"S": "p2"}, "g": {"S": "one"}, "gs": {"S": "x"}},
        )
        assert resp["Count"] == 0

    def test_hash_only_table_base_query_keeps_the_hash_only_sentence(
        self, hash_only_with_gsi, dynamodb_client
    ):
        _expect(
            dynamodb_client,
            EXCLUSIVE_INVALID,
            TableName=hash_only_with_gsi,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "p1"}},
            ExclusiveStartKey={"pk": {"S": "p2"}},
        )


class TestOrdering:
    """A start key from another partition paired with another fault: each of
    these errors is reported first."""

    def test_shape_check_first_when_sort_key_is_missing(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(dynamodb_client, SHAPE, **_alice_query(name, {"pk": {"S": "bob"}}))

    def test_shape_check_first_when_key_type_is_wrong(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            SHAPE,
            **_alice_query(name, {"pk": {"N": "1"}, "sk": {"S": "x"}}),
        )

    def test_shape_check_first_when_the_start_key_has_an_extra_attribute(
        self, table_factory, dynamodb_client
    ):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            SHAPE,
            **_alice_query(
                name, {"pk": {"S": "bob"}, "sk": {"S": "a"}, "extra": {"S": "x"}}
            ),
        )

    def test_filter_name_error_first(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            "Invalid FilterExpression: An expression attribute name used in the document "
            "path is not defined; attribute name: #n",
            **_alice_query(
                name,
                {"pk": {"S": "bob"}, "sk": {"S": "x"}},
                FilterExpression="#n = :p",
            ),
        )

    def test_filter_operand_type_error_first(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        query = _alice_query(
            name,
            {"pk": {"S": "bob"}, "sk": {"S": "a"}},
            FilterExpression="begins_with(x, :n)",
        )
        query["ExpressionAttributeValues"][":n"] = {"N": "1"}
        _expect(
            dynamodb_client,
            "Invalid FilterExpression: Incorrect operand type for operator or function; "
            "operator or function: begins_with, operand type: N",
            **query,
        )

    def test_unused_value_error_first(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            "Value provided in ExpressionAttributeValues unused in expressions: keys: {:z}",
            TableName=name,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "alice"}, ":z": {"S": "unused"}},
            ExclusiveStartKey={"pk": {"S": "bob"}, "sk": {"S": "x"}},
        )

    def test_limit_zero_first(self, table_factory):
        name = table_factory(range_key="sk")
        # boto3 rejects Limit=0 client-side; send it unvalidated.
        endpoint = os.environ.get("DYNAMODB_ENDPOINT")
        kwargs = {
            "service_name": "dynamodb",
            "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "config": botocore.config.Config(parameter_validation=False),
        }
        if endpoint:
            kwargs["endpoint_url"] = endpoint
            if endpoint.startswith("https://"):
                kwargs["verify"] = False
        raw_client = boto3.client(**kwargs)
        _expect(
            raw_client,
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
            "Member must have value greater than or equal to 1",
            **_alice_query(name, {"pk": {"S": "bob"}, "sk": {"S": "a"}}, Limit=0),
        )

    def test_missing_index_first(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        _expect(
            dynamodb_client,
            "The table does not have the specified index: nope",
            **_alice_query(
                name, {"pk": {"S": "bob"}, "sk": {"S": "a"}}, IndexName="nope"
            ),
        )

    def test_consistent_read_on_a_gsi_first(self, index_table, dynamodb_client):
        _expect(
            dynamodb_client,
            "Consistent reads are not supported on global secondary indexes",
            TableName=index_table,
            IndexName="by-g",
            ConsistentRead=True,
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": "one"}},
            ExclusiveStartKey={
                "pk": {"S": "bob"},
                "sk": {"S": "a"},
                "g": {"S": "two"},
                "gs": {"S": "x1"},
            },
        )

    def test_start_key_sentence_before_select_all_attributes_on_a_keys_only_gsi(
        self, index_table, dynamodb_client
    ):
        _expect(
            dynamodb_client,
            OUTSIDE_BOUNDARIES,
            TableName=index_table,
            IndexName="by-h",
            Select="ALL_ATTRIBUTES",
            KeyConditionExpression="h = :h",
            ExpressionAttributeValues={":h": {"S": "h1"}},
            ExclusiveStartKey={"pk": {"S": "p5"}, "sk": {"S": "a"}, "h": {"S": "h2"}},
        )

    def test_shape_check_before_select_all_attributes_on_a_keys_only_gsi(
        self, index_table, dynamodb_client
    ):
        # Measured 2026-10-24: an extra attribute in the start key answers the
        # shape sentence, ahead of the Select check.
        _expect(
            dynamodb_client,
            SHAPE,
            TableName=index_table,
            IndexName="by-h",
            Select="ALL_ATTRIBUTES",
            KeyConditionExpression="h = :h",
            ExpressionAttributeValues={":h": {"S": "h1"}},
            ExclusiveStartKey={
                "pk": {"S": "p5"},
                "sk": {"S": "a"},
                "h": {"S": "h2"},
                "extra": {"S": "x"},
            },
        )

    def test_select_all_attributes_on_a_keys_only_gsi_without_a_start_key(
        self, index_table, dynamodb_client
    ):
        _expect(
            dynamodb_client,
            "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not "
            "supported for global secondary index by-h because its projection type is not ALL",
            TableName=index_table,
            IndexName="by-h",
            Select="ALL_ATTRIBUTES",
            KeyConditionExpression="h = :h",
            ExpressionAttributeValues={":h": {"S": "h1"}},
        )

    def test_missing_table_first(self, dynamodb_client):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                **_alice_query(
                    unique_name("esk-missing"), {"pk": {"S": "bob"}, "sk": {"S": "a"}}
                )
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ResourceNotFoundException"
        assert err["Message"] == "Requested resource not found"
