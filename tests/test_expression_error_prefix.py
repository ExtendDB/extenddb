# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Expression parse errors must carry the expression-parameter-specific prefix.

Amazon DynamoDB prefixes expression parse errors with the specific parameter
name (Invalid ProjectionExpression / ConditionExpression / FilterExpression /
KeyConditionExpression / UpdateExpression), not a generic "Invalid expression".

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

RESERVED = "Attribute name is a reserved keyword; reserved keyword: status"


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}})
        yield name


def _msg(func) -> str:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    return err["Message"]


class TestExpressionErrorPrefix:
    """Reserved-keyword parse errors carry the per-parameter prefix."""

    def test_projection_prefix(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "k1"}},
            ProjectionExpression="status",
        ))
        assert msg == f"Invalid ProjectionExpression: {RESERVED}"

    def test_filter_prefix(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.scan(
            TableName=hash_table,
            FilterExpression="status = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        ))
        assert msg == f"Invalid FilterExpression: {RESERVED}"

    def test_condition_prefix_put(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.put_item(
            TableName=hash_table, Item={"pk": {"S": "k9"}},
            ConditionExpression="status = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        ))
        assert msg == f"Invalid ConditionExpression: {RESERVED}"

    def test_condition_prefix_delete(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.delete_item(
            TableName=hash_table, Key={"pk": {"S": "k1"}},
            ConditionExpression="status = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        ))
        assert msg == f"Invalid ConditionExpression: {RESERVED}"

    def test_update_prefix(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.update_item(
            TableName=hash_table, Key={"pk": {"S": "k1"}},
            UpdateExpression="SET status = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        ))
        assert msg == f"Invalid UpdateExpression: {RESERVED}"

    def test_key_condition_prefix(self, dynamodb_client, hash_table):
        msg = _msg(lambda: dynamodb_client.query(
            TableName=hash_table,
            KeyConditionExpression="status = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        ))
        assert msg == f"Invalid KeyConditionExpression: {RESERVED}"

    def test_projection_syntax_prefix(self, dynamodb_client, hash_table):
        # Syntax error also carries the ProjectionExpression prefix.
        msg = _msg(lambda: dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "k1"}},
            ProjectionExpression="a..b",
        ))
        assert msg.startswith("Invalid ProjectionExpression:")


def _s(value: str) -> dict:
    return {"S": value}


def _key(pk: str, sk: str) -> dict:
    return {"pk": _s(pk), "sk": _s(sk)}


SIZE_ITEMS = [
    {**_key("rw-size", "0"), "size": _s("v1"),
     "a": {"M": {"SIZE": _s("v2"), "other": _s("o")}}, "b": _s("b")},
    {**_key("f1", "s1"), "size": _s("k")},
    {**_key("f1", "s2"), "size": _s("z")},
    {**_key("c1", "c1"), "size": _s("c")},
    {**_key("c1", "c2"), "x": _s("1")},
    {**_key("r1", "r1"), "size": _s("r"), "keep": _s("1")},
    {**_key("g1", "g1"), "size": {"M": {"x": _s("vx"), "y": _s("vy")}}, "b": _s("vb")},
]
MISSING = "<a table that does not exist>"
CONDITION_FAILED = ("ConditionalCheckFailedException", "The conditional request failed")
TABLE_NOT_FOUND = ("ResourceNotFoundException", "Requested resource not found")

# `size` with no `(` after it names an attribute. Each row is a request sent
# to Amazon DynamoDB in us-east-1 on 2026-11-14, 2026-11-16 or 2026-11-18,
# and the service's answer: an error code and message, or response fields.
SIZE_AS_NAME = [
    pytest.param("get_item", {
        "Key": _key("rw-size", "0"), "ProjectionExpression": "size, a.SIZE",
        "ConsistentRead": True,
    }, {"Item": {"size": _s("v1"), "a": {"M": {"SIZE": _s("v2")}}}},
        id="projection-size-and-a.SIZE"),
    pytest.param("get_item", {
        "Key": _key("g1", "g1"), "ProjectionExpression": "size.x, b",
        "ConsistentRead": True,
    }, {"Item": {"size": {"M": {"x": _s("vx")}}, "b": _s("vb")}},
        id="projection-size.x"),
    pytest.param("update_item", {
        "TableName": MISSING, "Key": {"pk": _s("u1")},
        "UpdateExpression": "SET size = :n",
        "ExpressionAttributeValues": {":n": {"N": "1"}},
    }, TABLE_NOT_FOUND, id="update-set-size-missing-table"),
    pytest.param("update_item", {
        "TableName": MISSING, "Key": {"pk": _s("u1")},
        "UpdateExpression": "SET a = size",
    }, TABLE_NOT_FOUND, id="update-set-a-to-size-missing-table"),
    pytest.param("update_item", {
        "Key": _key("r1", "r1"), "UpdateExpression": "REMOVE size",
        "ReturnValues": "UPDATED_OLD",
    }, {"Attributes": {"size": _s("r")}}, id="update-remove-size"),
    pytest.param("query", {
        "KeyConditionExpression": "size = :v",
        "ExpressionAttributeValues": {":v": _s("k")},
    }, ("ValidationException", "Query condition missed key schema element: pk"),
        id="key-condition-size"),
    pytest.param("put_item", {
        "Item": _key("us-msg", "c4"), "ConditionExpression": "size = :v",
        "ExpressionAttributeValues": {":v": _s("k")},
    }, CONDITION_FAILED, id="condition-size-no-item"),
    pytest.param("put_item", {
        "Item": {**_key("c1", "c1"), "size": _s("c")},
        "ConditionExpression": "attribute_exists(size)",
    }, {"Attributes": None}, id="condition-attribute-exists-size-present"),
    pytest.param("put_item", {
        "Item": {**_key("c1", "c2"), "x": _s("1")},
        "ConditionExpression": "attribute_exists(size)",
    }, CONDITION_FAILED, id="condition-attribute-exists-size-absent"),
    pytest.param("query", {
        "KeyConditionExpression": "pk = :p", "FilterExpression": "size = :v",
        "ExpressionAttributeValues": {":p": _s("f1"), ":v": _s("k")},
        "ConsistentRead": True,
    }, {"Count": 1, "ScannedCount": 2, "Items": [{**_key("f1", "s1"), "size": _s("k")}]},
        id="query-filter-size"),
    pytest.param("scan", {
        "FilterExpression": "size = :v",
        "ExpressionAttributeValues": {":v": _s("k")}, "ConsistentRead": True,
    }, {"Count": 1, "Items": [{**_key("f1", "s1"), "size": _s("k")}]},
        id="scan-filter-size"),
]


@pytest.fixture(scope="class")
def size_table(dynamodb_client):
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
        for item in SIZE_ITEMS:
            dynamodb_client.put_item(TableName=name, Item=item)
        yield name


class TestSizeIsAnAttributeName:
    @pytest.mark.parametrize("operation,params,answer", SIZE_AS_NAME)
    def test_size_with_no_paren_after_it(
        self, dynamodb_client, size_table, operation, params, answer
    ):
        request = {"TableName": size_table, **params}
        if request["TableName"] == MISSING:
            request["TableName"] = f"{size_table}-missing"
        call = getattr(dynamodb_client, operation)
        if isinstance(answer, tuple):
            with pytest.raises(ClientError) as exc_info:
                call(**request)
            err = exc_info.value.response["Error"]
            assert (err["Code"], err["Message"]) == answer
        else:
            resp = call(**request)
            assert {k: resp.get(k) for k in answer} == answer
