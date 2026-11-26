# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A name that starts with `_`, written in an expression without a `#name`.

Amazon DynamoDB refuses it in every expression member with a syntax error at
the `_`. The `near` text runs from the token before the `_` to the token
after it. PutItem, UpdateItem and DeleteItem put "1 validation error
detected: " ahead of the text, and the other operations do not. A key
condition, a filter and an update expression are checked before the table
lookup. The same name through ExpressionAttributeNames is served, and so are
`a_b`, `#_x`, `:_v` and a PartiQL statement that names `_x`. Measured in
us-east-1 on 2026-11-14 and 2026-11-18, each request twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

COUNT = "1 validation error detected: "
MISSING = f"extenddb-test-missing-{uuid.uuid4().hex[:12]}"


def _syntax(member: str, near: str) -> str:
    return f'Invalid {member}: Syntax error; token: "_", near: "{near}"'


def _key(sk: str) -> dict:
    return {"pk": {"S": "us-msg"}, "sk": {"S": sk}}


def _v(placeholder: str = ":v") -> dict:
    return {"ExpressionAttributeValues": {placeholder: {"S": "v"}}}


def _query(**extra) -> tuple:
    request = {"KeyConditionExpression": "pk = :p",
               "ExpressionAttributeValues": {":p": {"S": "us-msg"}}}
    request.update(extra)
    return ("query", request)


def _put(sk: str, condition: str, **extra) -> tuple:
    return ("put_item", {"Item": _key(sk), "ConditionExpression": condition, **extra})


def _update(sk: str, update: str, **extra) -> tuple:
    return ("update_item", {"Key": _key(sk), "UpdateExpression": update, **extra})


# (id, (operation, request), expected message). `TableName` is the table
# unless the request names one. BatchGetItem, the transactions and the PartiQL
# statements name it as "<t>", which the test fills in.
REFUSED = [
    ("query-filter", _query(FilterExpression="attribute_exists(_x)"),
     _syntax("FilterExpression", "(_x")),
    ("scan-filter", ("scan", {"FilterExpression": "attribute_exists(_x)"}),
     _syntax("FilterExpression", "(_x")),
    ("query-projection", _query(ProjectionExpression="pk, _x"),
     _syntax("ProjectionExpression", ", _x")),
    ("query-key", ("query", {"KeyConditionExpression": "_pk = :v",
                             "ExpressionAttributeValues": {":v": {"S": "k"}}}),
     _syntax("KeyConditionExpression", "_pk")),
    ("query-sort-key", ("query", {
        "KeyConditionExpression": "pk = :v AND begins_with(_sk, :p)",
        "ExpressionAttributeValues": {":v": {"S": "k"}, ":p": {"S": "r"}}}),
     _syntax("KeyConditionExpression", "(_sk")),
    ("query-index-missing-table", ("query", {
        "TableName": MISSING, "IndexName": "g-keys", "KeyConditionExpression": "_g = :v",
        "ExpressionAttributeValues": {":v": {"S": "k"}}}),
     _syntax("KeyConditionExpression", "_g")),
    ("put-condition", _put("r7", "attribute_not_exists(_x)"),
     COUNT + _syntax("ConditionExpression", "(_x")),
    ("update-set", _update("r8", "SET _x = :v", **_v()),
     COUNT + _syntax("UpdateExpression", "SET _x")),
    ("scan-filter-first", ("scan", {"FilterExpression": "_x = :v", **_v()}),
     _syntax("FilterExpression", "_x")),
    ("query-projection-alone", _query(ProjectionExpression="_x"),
     _syntax("ProjectionExpression", "_x")),
    ("query-projection-nested", _query(ProjectionExpression="a._b"),
     _syntax("ProjectionExpression", "._b")),
    ("put-nested", _put("n4", "a._b = :v", **_v()),
     COUNT + _syntax("ConditionExpression", "._b")),
    ("put-nested-index", _put("n5", "a[0]._b = :v", **_v()),
     COUNT + _syntax("ConditionExpression", "._b")),
    ("update-nested", _update("n6", "SET a._b = :v", **_v()),
     COUNT + _syntax("UpdateExpression", "._b")),
    ("update-remove", _update("n7", "REMOVE _x"),
     COUNT + _syntax("UpdateExpression", "REMOVE _x")),
    ("put-underscore-alone", _put("n8", "attribute_not_exists(_)"),
     COUNT + _syntax("ConditionExpression", "(_)")),
    ("put-two-underscores", _put("n9", "attribute_not_exists(__x)"),
     COUNT + _syntax("ConditionExpression", "(__")),
    ("put-underscore-digit", _put("n10", "attribute_not_exists(_1)"),
     COUNT + _syntax("ConditionExpression", "(_1")),
    ("get-projection", ("get_item", {"Key": _key("cond"), "ProjectionExpression": "_x"}),
     _syntax("ProjectionExpression", "_x")),
    ("batch-get-projection", ("batch_get_item", {"RequestItems": {
        "<t>": {"Keys": [_key("cond")], "ProjectionExpression": "_x"}}}),
     _syntax("ProjectionExpression", "_x")),
    ("delete-condition", ("delete_item", {"Key": _key("del"),
                                          "ConditionExpression": "attribute_exists(_x)"}),
     COUNT + _syntax("ConditionExpression", "(_x")),
    ("transact-update", ("transact_write_items", {"TransactItems": [{"Update": {
        "TableName": "<t>", "Key": _key("t4"), "UpdateExpression": "SET _x = :v", **_v()}}]}),
     _syntax("UpdateExpression", "SET _x")),
    ("transact-put", ("transact_write_items", {"TransactItems": [{"Put": {
        "TableName": "<t>", "Item": _key("t1"), "ConditionExpression": "attribute_exists(_x)"}}]}),
     _syntax("ConditionExpression", "(_x")),
    ("transact-delete", ("transact_write_items", {"TransactItems": [{"Delete": {
        "TableName": "<t>", "Key": _key("t2"), "ConditionExpression": "attribute_exists(_x)"}}]}),
     _syntax("ConditionExpression", "(_x")),
    ("transact-condition-check", ("transact_write_items", {"TransactItems": [{"ConditionCheck": {
        "TableName": "<t>", "Key": _key("t3"), "ConditionExpression": "attribute_exists(_x)"}}]}),
     _syntax("ConditionExpression", "(_x")),
    ("update-condition", _update("u1", "SET a = :v", ConditionExpression="attribute_exists(_x)",
                                 **_v()),
     COUNT + _syntax("ConditionExpression", "(_x")),
    ("query-projection-index-digit", _query(ProjectionExpression="a[1_]"),
     _syntax("ProjectionExpression", "1_]")),
    ("put-index-digit", _put("d2", "attribute_exists(a[1_])"),
     COUNT + _syntax("ConditionExpression", "1_]")),
    ("transact-get-projection", ("transact_get_items", {"TransactItems": [{"Get": {
        "TableName": "<t>", "Key": _key("cond"), "ProjectionExpression": "_x"}}]}),
     _syntax("ProjectionExpression", "_x")),
    ("scan-missing-table", ("scan", {"TableName": MISSING,
                                     "FilterExpression": "attribute_exists(_x)"}),
     _syntax("FilterExpression", "(_x")),
    ("update-missing-table", ("update_item", {"TableName": MISSING, "Key": _key("upd"),
                                              "UpdateExpression": "SET _x = :v", **_v()}),
     COUNT + _syntax("UpdateExpression", "SET _x")),
]

X = {"ExpressionAttributeNames": {"#x": "_x"}}
SERVED = [
    ("query-filter-name", _query(FilterExpression="attribute_exists(#x)", **X)),
    ("query-projection-name", _query(ProjectionExpression="pk, #x", **X)),
    ("scan-filter-name", ("scan", {"FilterExpression": "attribute_exists(#x)", **X})),
    ("put-condition-name", _put("s4", "attribute_not_exists(#x)", **X)),
    ("update-set-name", _update("s5", "SET #x = :v", **X, **_v())),
    ("put-underscore-inside", _put("s6", "attribute_not_exists(a_b)")),
    ("put-name-placeholder", _put("p1", "attribute_not_exists(#_x)",
                                  ExpressionAttributeNames={"#_x": "a"})),
    ("put-value-placeholder", _put("p2", "attribute_not_exists(a) OR a = :_v", **_v(":_v"))),
    ("partiql-select", ("execute_statement", {
        "Statement": "SELECT _x, a._b FROM \"<t>\" WHERE pk = 'us-msg'"})),
    ("partiql-update", ("execute_statement", {
        "Statement": "UPDATE \"<t>\" SET _y = 1 WHERE pk = 'us-msg' AND sk = 'partiql'"})),
]


@pytest.fixture(scope="module")
def table(dynamodb_client):
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
        # The item that the PartiQL UPDATE of `SERVED` changes.
        dynamodb_client.put_item(TableName=name, Item=_key("partiql"))
        yield name


def _fill(value, table: str):
    """`value` with each "<t>" key, value or part of a string replaced by the table's name."""
    if isinstance(value, dict):
        return {table if k == "<t>" else k: _fill(v, table) for k, v in value.items()}
    if isinstance(value, list):
        return [_fill(v, table) for v in value]
    return value.replace("<t>", table) if isinstance(value, str) else value


def _send(client, table: str, operation: str, request: dict):
    body = _fill(request, table)
    if not ("RequestItems" in body or "TransactItems" in body or "Statement" in body):
        body = {"TableName": table, **body}
    return getattr(client, operation)(**body)


@pytest.mark.parametrize(("case", "expected"), [(r[1], r[2]) for r in REFUSED],
                         ids=[r[0] for r in REFUSED])
def test_refused(dynamodb_client, table, case, expected):
    operation, request = case
    with pytest.raises(ClientError) as exc_info:
        _send(dynamodb_client, table, operation, request)
    error = exc_info.value.response["Error"]
    assert (error["Code"], error["Message"]) == ("ValidationException", expected)
    if operation in ("put_item", "update_item") and "TableName" not in request:
        key = request.get("Key") or request["Item"]
        stored = dynamodb_client.get_item(TableName=table, Key=_key(key["sk"]["S"]))
        assert "Item" not in stored


@pytest.mark.parametrize("case", [r[1] for r in SERVED], ids=[r[0] for r in SERVED])
def test_served(dynamodb_client, table, case):
    operation, request = case
    response = _send(dynamodb_client, table, operation, request)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.parametrize("key_condition", [
    {"IndexName": "g-keys", "KeyConditionExpression": "g = :v"},
    {"KeyConditionExpression": "size = :v"},
], ids=["index-key", "size-name"])
def test_valid_key_condition_on_a_missing_table_is_resource_not_found(
        dynamodb_client, key_condition):
    """The key condition is read first only when it is refused."""
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.query(
            TableName=MISSING, ExpressionAttributeValues={":v": {"S": "k"}}, **key_condition,
        )
    error = exc_info.value.response["Error"]
    assert (error["Code"], error["Message"]) == (
        "ResourceNotFoundException", "Requested resource not found")
