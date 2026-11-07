# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Nesting limits on the values of TransactWriteItems.

Measured against Amazon DynamoDB on 2026-10-27 (two samples, identical
bytes). A value is counted in `M` or `L` wrappers around a leaf.

  * an ExpressionAttributeValues value, used or not, may hold 32 wrappers and
    is refused at 33, as a top-level ValidationException worded
    "ExpressionAttributeValues contains invalid value: ... for key :v";
  * a Put item attribute may hold 31 wrappers and is refused at 32, as a
    top-level ValidationException;
  * a value an Update stores with SET is an attribute of the item, so 32
    wrappers pass the first rule and break the item limit: a ValidationError
    cancellation reason "Nesting Levels have exceeded supported limits",
    reported after the lookup, the 4 MB total, the key pass and the duplicate
    check.

Dual-target: runs against the service and against ExtendDB on every backend.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

ITEM_NESTING = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
STORED_NESTING = "Nesting Levels have exceeded supported limits"
GOOD_KEY = {"pk": {"S": "p"}, "sk": {"S": "s"}}
OTHER_KEY = {"pk": {"S": "other"}, "sk": {"S": "s"}}
BAD_KEY = {"pk": {"N": "1"}, "sk": {"S": "s"}}
KEY_MISMATCH = "The provided key element does not match the schema"
KEY_UPDATE = (
    "One or more parameter values were invalid: Cannot update attribute pk. "
    "This attribute is part of the key"
)
EMPTY_KEY = (
    "One or more parameter values are not valid. The AttributeValue for a key "
    "attribute cannot contain an empty string value. Key: pk"
)
DUPLICATE = "Transaction request cannot include multiple operations on one item"


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
        dynamodb_client.put_item(TableName=name, Item=GOOD_KEY)
        yield name


@pytest.fixture(scope="module")
def missing_table() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _deep(wrappers: int) -> dict:
    value: dict = {"S": "leaf"}
    for _ in range(wrappers):
        value = {"M": {"a": value}}
    return value


def _set_value(table_name, wrappers, key=GOOD_KEY):
    return {
        "Update": {
            "TableName": table_name,
            "Key": key,
            "UpdateExpression": "SET d = :v",
            "ExpressionAttributeValues": {":v": _deep(wrappers)},
        }
    }


def _fail(client, items, code):
    with pytest.raises(ClientError) as exc_info:
        client.transact_write_items(TransactItems=items)
    err = exc_info.value.response["Error"]
    assert err["Code"] == code, err
    return exc_info.value.response


def _top(client, items, message):
    assert _fail(client, items, "ValidationException")["Error"]["Message"] == message


def _reasons(client, items):
    """(Code, Message) of every cancellation reason; an item that did not fail has no message."""
    got = _fail(client, items, "TransactionCanceledException")["CancellationReasons"]
    return [(r["Code"], None if r["Code"] == "None" else r.get("Message")) for r in got]


class TestExpressionAttributeValues:
    def test_a_condition_check_value_32_deep_is_accepted_and_33_is_refused(self, dynamodb_client, table):
        def check(wrappers):
            return {
                "ConditionCheck": {
                    "TableName": table,
                    "Key": GOOD_KEY,
                    "ConditionExpression": "attribute_exists(pk) OR :d = :d",
                    "ExpressionAttributeValues": {":d": _deep(wrappers)},
                }
            }

        dynamodb_client.transact_write_items(TransactItems=[check(32)])
        _top(
            dynamodb_client,
            [check(33)],
            f"ExpressionAttributeValues contains invalid value: {ITEM_NESTING} for key :d",
        )

    def test_a_value_33_deep_is_refused_before_the_lookup_in_either_order(
        self, dynamodb_client, table, missing_table
    ):
        message = f"ExpressionAttributeValues contains invalid value: {ITEM_NESTING} for key :v"
        absent = {"Put": {"TableName": missing_table, "Item": GOOD_KEY}}
        _top(dynamodb_client, [_set_value(table, 33), absent], message)
        _top(dynamodb_client, [absent, _set_value(table, 33)], message)


class TestStoredValues:
    def test_a_put_attribute_31_deep_is_stored_and_32_is_refused(self, dynamodb_client, table):
        def put(wrappers):
            item = {"pk": {"S": f"put{wrappers}"}, "sk": {"S": "s"}, "d": _deep(wrappers)}
            return {"Put": {"TableName": table, "Item": item}}

        dynamodb_client.transact_write_items(TransactItems=[put(31)])
        _top(dynamodb_client, [put(32)], ITEM_NESTING)

    def test_an_update_stores_31_deep_and_cancels_at_32(self, dynamodb_client, table):
        dynamodb_client.transact_write_items(TransactItems=[_set_value(table, 31)])
        assert _reasons(dynamodb_client, [_set_value(table, 32)]) == [("ValidationError", STORED_NESTING)]

    def test_every_offending_update_is_flagged(self, dynamodb_client, table):
        items = [_set_value(table, 32), _set_value(table, 32, OTHER_KEY)]
        assert _reasons(dynamodb_client, items) == [("ValidationError", STORED_NESTING)] * 2

    def test_the_reason_comes_after_every_other_phase(self, dynamodb_client, table, missing_table):
        stored = _set_value(table, 32)
        twin = {"Put": {"TableName": table, "Item": GOOD_KEY}}
        mismatch = {"Delete": {"TableName": table, "Key": BAD_KEY}}
        key_update = {
            "Update": {
                "TableName": table, "Key": OTHER_KEY, "UpdateExpression": "SET pk = :v",
                "ExpressionAttributeValues": {":v": {"S": "z"}},
            }
        }
        empty_key = {
            "ConditionCheck": {
                "TableName": table, "Key": {"pk": {"S": ""}, "sk": {"S": "a"}},
                "ConditionExpression": "attribute_exists(pk)",
            }
        }
        absent = {"Put": {"TableName": missing_table, "Item": GOOD_KEY}}
        for items in ([stored, absent], [absent, stored]):
            _fail(dynamodb_client, items, "ResourceNotFoundException")
        fail = ("ValidationError", KEY_MISMATCH)
        assert _reasons(dynamodb_client, [stored, mismatch]) == [("None", None), fail]
        assert _reasons(dynamodb_client, [mismatch, stored]) == [fail, ("None", None)]
        fail = ("ValidationError", KEY_UPDATE)
        assert _reasons(dynamodb_client, [stored, key_update]) == [("None", None), fail]
        assert _reasons(dynamodb_client, [key_update, stored]) == [fail, ("None", None)]
        for items in ([stored, empty_key], [empty_key, stored]):
            _top(dynamodb_client, items, EMPTY_KEY)
        for items in ([stored, twin], [twin, stored]):
            _top(dynamodb_client, items, DUPLICATE)
