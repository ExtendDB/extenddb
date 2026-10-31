# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Where four per-item checks sit in TransactWriteItems validation.

Amazon DynamoDB reports them in two places (measured 2026-10-27):

  * before any table is looked up, one item after another, in the pass that
    also checks the expressions: the nesting of every ExpressionAttributeValues
    value of the item, and for a Put the attribute name size, then the item
    size, then the item nesting;
  * after every table is looked up and after the 4 MB total, in the key pass,
    one item after another: a key that does not fit the schema, then a key
    attribute named in an Update (both a ValidationError cancellation reason
    on that item and None on the others), then an empty key value (a
    top-level ValidationException).

The attribute name size is worded differently by the two targets, so only its
class and the words "attribute name" are compared.

Dual-target: runs against the service and against ExtendDB on every backend.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NESTING = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
VALUE_NESTING = f"ExpressionAttributeValues contains invalid value: {NESTING} for key :v"
EMPTY_KEY = (
    "One or more parameter values are not valid. The AttributeValue for a key "
    "attribute cannot contain an empty string value. Key: pk"
)
KEY_UPDATE = (
    "One or more parameter values were invalid: Cannot update attribute pk. "
    "This attribute is part of the key"
)
KEY_UPDATE_SK = KEY_UPDATE.replace("attribute pk.", "attribute sk.")
KEY_MISMATCH = "The provided key element does not match the schema"
PUT_MISMATCH_SK = "One or more parameter values were invalid: Type mismatch for key sk expected: S actual: N"
ITEM_SIZE = "Item size has exceeded the maximum allowed size"
UNDEFINED_VALUE = (
    "Invalid ConditionExpression: An expression attribute value used in "
    "expression is not defined; attribute value: :q"
)
NOT_FOUND = "Requested resource not found"

GOOD_KEY = {"pk": {"S": "p"}, "sk": {"S": "s"}}
BAD_KEY = {"pk": {"N": "1"}, "sk": {"S": "s"}}
EMPTY_PK = {"pk": {"S": ""}, "sk": {"S": "a"}}


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
        dynamodb_client.put_item(TableName=name, Item={**GOOD_KEY, "note": {"S": "old"}})
        yield name


@pytest.fixture(scope="module")
def missing_table() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _deep(levels: int) -> dict:
    root: dict = {"L": []}
    cur = root
    for _ in range(levels - 1):
        nxt: dict = {"L": []}
        cur["L"].append(nxt)
        cur = nxt
    return root


def _put(table_name, item, **extra):
    return {"Put": {"TableName": table_name, "Item": item, **extra}}


def _delete(table_name, key):
    return {"Delete": {"TableName": table_name, "Key": key}}


def _update(table_name, key, expression, values=None):
    body = {"TableName": table_name, "Key": key, "UpdateExpression": expression}
    if values is not None:
        body["ExpressionAttributeValues"] = values
    return {"Update": body}


def _check(table_name, key, expression):
    return {"ConditionCheck": {"TableName": table_name, "Key": key, "ConditionExpression": expression}}


def _fail(client, items, code):
    with pytest.raises(ClientError) as exc_info:
        client.transact_write_items(TransactItems=items)
    err = exc_info.value.response["Error"]
    assert err["Code"] == code, err
    return exc_info.value.response


def _top(client, items, message):
    assert _fail(client, items, "ValidationException")["Error"]["Message"] == message


def _cancelled(client, items, reasons):
    """`reasons` holds None for an item that did not fail and a message otherwise."""
    response = _fail(client, items, "TransactionCanceledException")
    got = response["CancellationReasons"]
    assert [r["Code"] for r in got] == ["None" if r is None else "ValidationError" for r in reasons], got
    assert [r["Message"] for r in got if r["Code"] == "ValidationError"] == [r for r in reasons if r], got


def _both_orders(client, defect, other, check):
    check(client, [defect, other])
    check(client, [other, defect])


def _nested_put(table):
    return _put(table, {"pk": {"S": "nd"}, "sk": {"S": "a"}, "d": _deep(40)})


def _long_name_put(table):
    return _put(table, {"pk": {"S": "an"}, "sk": {"S": "a"}, "a" * 65536: {"S": "v"}})


def _deep_value_update(table):
    return _update(table, GOOD_KEY, "SET d = :v", {":v": _deep(40)})


def _attribute_name_error(client, items):
    message = _fail(client, items, "ValidationException")["Error"]["Message"]
    assert "attribute name" in message.lower(), message


class TestBeforeTheLookup:
    def test_item_nesting_beats_a_missing_table_in_either_order(self, dynamodb_client, table, missing_table):
        absent = _put(missing_table, {"pk": {"S": "x"}, "sk": {"S": "a"}})
        _both_orders(dynamodb_client, _nested_put(table), absent, lambda c, i: _top(c, i, NESTING))

    def test_value_nesting_beats_a_missing_table_in_either_order(self, dynamodb_client, table, missing_table):
        absent = _put(missing_table, {"pk": {"S": "x"}, "sk": {"S": "a"}})
        check = lambda c, i: _top(c, i, VALUE_NESTING)  # noqa: E731
        _both_orders(dynamodb_client, _deep_value_update(table), absent, check)
        used_in_a_condition = {
            "ConditionCheck": {
                "TableName": table, "Key": GOOD_KEY, "ConditionExpression": "note = :v",
                "ExpressionAttributeValues": {":v": _deep(40)},
            }
        }
        _both_orders(dynamodb_client, used_in_a_condition, absent, check)

    def test_attribute_name_size_beats_a_missing_table_in_either_order(self, dynamodb_client, table, missing_table):
        absent = _put(missing_table, {"pk": {"S": "x"}, "sk": {"S": "a"}})
        _both_orders(dynamodb_client, _long_name_put(table), absent, _attribute_name_error)

    def test_the_checks_follow_item_order_against_an_expression_error(self, dynamodb_client, table):
        undefined = _check(table, GOOD_KEY, "note = :q")
        for defect, check in (
            (_nested_put(table), lambda c, i: _top(c, i, NESTING)),
            (_deep_value_update(table), lambda c, i: _top(c, i, VALUE_NESTING)),
            (_long_name_put(table), _attribute_name_error),
        ):
            check(dynamodb_client, [defect, undefined])
            _top(dynamodb_client, [undefined, defect], UNDEFINED_VALUE)

    def test_a_put_reports_name_size_then_item_size_then_nesting(self, dynamodb_client, table):
        pad = {"S": "x" * 410_000}
        name_and_size = {"pk": {"S": "w"}, "sk": {"S": "a"}, "a" * 65536: {"S": "v"}, "pad": pad}
        nesting_and_size = {"pk": {"S": "w"}, "sk": {"S": "a"}, "d": _deep(40), "pad": pad}
        nesting_and_name = {"pk": {"S": "w"}, "sk": {"S": "a"}, "d": _deep(40), "a" * 65536: {"S": "v"}}
        _attribute_name_error(dynamodb_client, [_put(table, name_and_size)])
        _top(dynamodb_client, [_put(table, nesting_and_size)], ITEM_SIZE)
        _attribute_name_error(dynamodb_client, [_put(table, nesting_and_name)])

    def test_inside_one_item_the_expressions_come_first_and_the_value_nesting_before_them(
        self, dynamodb_client, table
    ):
        deep_item = {"pk": {"S": "w"}, "sk": {"S": "a"}, "d": _deep(40)}
        _top(
            dynamodb_client,
            [_put(table, deep_item, ConditionExpression="note = :q")],
            UNDEFINED_VALUE,
        )
        _top(dynamodb_client, [_update(table, GOOD_KEY, "SET d = ", {":v": _deep(40)})], VALUE_NESTING)
        _top(
            dynamodb_client,
            [_update(table, GOOD_KEY, "SET d = :v", {":v": _deep(40), ":u": {"N": "1"}})],
            VALUE_NESTING,
        )


class TestKeyPass:
    @pytest.mark.parametrize("kind", ["check", "put", "delete", "update"])
    def test_an_empty_key_value_waits_for_every_lookup(self, dynamodb_client, table, missing_table, kind):
        defect = {
            "check": _check(table, EMPTY_PK, "attribute_exists(pk)"),
            "put": _put(table, {"pk": {"S": ""}, "sk": {"S": "a"}}),
            "delete": _delete(table, EMPTY_PK),
            "update": _update(table, EMPTY_PK, "SET x = :v", {":v": {"S": "1"}}),
        }[kind]
        absent = _put(missing_table, {"pk": {"S": "x"}, "sk": {"S": "a"}})
        _both_orders(dynamodb_client, defect, absent, lambda c, i: _expect_not_found(c, i))
        _top(dynamodb_client, [defect], EMPTY_KEY)

    def test_a_key_attribute_update_waits_for_every_lookup(self, dynamodb_client, table, missing_table):
        absent = _put(missing_table, {"pk": {"S": "x"}, "sk": {"S": "a"}})
        key_update = _update(table, GOOD_KEY, "SET pk = :v", {":v": {"S": "z"}})
        _both_orders(dynamodb_client, key_update, absent, lambda c, i: _expect_not_found(c, i))

    def test_a_key_attribute_update_is_a_cancellation_reason(self, dynamodb_client, table):
        _cancelled(dynamodb_client, [_update(table, GOOD_KEY, "SET pk = :v", {":v": {"S": "z"}})], [KEY_UPDATE])
        _cancelled(dynamodb_client, [_update(table, GOOD_KEY, "REMOVE sk")], [KEY_UPDATE_SK])

    def test_the_request_total_beats_the_key_pass(self, dynamodb_client, table):
        big = [
            _put(table, {"pk": {"S": f"big{i}"}, "sk": {"S": "a"}, "pad": {"S": "x" * 389_000}})
            for i in range(11)
        ]
        for items in (
            [_check(table, EMPTY_PK, "attribute_exists(pk)")] + big,
            big + [_update(table, GOOD_KEY, "SET pk = :v", {":v": {"S": "z"}})],
        ):
            message = _fail(dynamodb_client, items, "ValidationException")["Error"]["Message"]
            assert "4MB" in message or "4 MB" in message, message

    def test_items_are_examined_in_order(self, dynamodb_client, table):
        empty = _check(table, EMPTY_PK, "attribute_exists(pk)")
        key_update = _update(table, GOOD_KEY, "SET pk = :v", {":v": {"S": "z"}})
        mismatch = _delete(table, BAD_KEY)
        _cancelled(dynamodb_client, [key_update, empty], [KEY_UPDATE, None])
        _top(dynamodb_client, [empty, key_update], EMPTY_KEY)
        _top(dynamodb_client, [empty, mismatch], EMPTY_KEY)
        _cancelled(dynamodb_client, [mismatch, empty], [KEY_MISMATCH, None])
        _cancelled(dynamodb_client, [key_update, mismatch], [KEY_UPDATE, None])
        _cancelled(dynamodb_client, [mismatch, key_update], [KEY_MISMATCH, None])

    def test_inside_one_item_a_mismatch_beats_a_key_update_beats_an_empty_value(self, dynamodb_client, table):
        _cancelled(dynamodb_client, [_delete(table, {"pk": {"S": ""}, "sk": {"N": "1"}})], [KEY_MISMATCH])
        _cancelled(
            dynamodb_client,
            [_update(table, {"pk": {"N": "1"}, "sk": {"S": "a"}}, "SET pk = :v", {":v": {"S": "z"}})],
            [KEY_MISMATCH],
        )
        _cancelled(
            dynamodb_client, [_update(table, EMPTY_PK, "SET pk = :v", {":v": {"S": "z"}})], [KEY_UPDATE]
        )
        _cancelled(
            dynamodb_client, [_put(table, {"pk": {"S": ""}, "sk": {"N": "1"}})], [PUT_MISMATCH_SK]
        )

    def test_the_key_pass_beats_duplicate_items(self, dynamodb_client, table):
        twin = _put(table, {"pk": {"S": "dup"}, "sk": {"S": "a"}})
        key_update = _update(table, GOOD_KEY, "SET pk = :v", {":v": {"S": "z"}})
        empty = _check(table, EMPTY_PK, "attribute_exists(pk)")
        _cancelled(dynamodb_client, [key_update, twin, twin], [KEY_UPDATE, None, None])
        _cancelled(dynamodb_client, [twin, twin, key_update], [None, None, KEY_UPDATE])
        _top(dynamodb_client, [empty, twin, twin], EMPTY_KEY)
        _top(dynamodb_client, [twin, twin, empty], EMPTY_KEY)


def _expect_not_found(client, items):
    response = _fail(client, items, "ResourceNotFoundException")
    assert response["Error"]["Message"] == NOT_FOUND
