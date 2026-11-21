# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The answer to a request body with two faults, in both member orders.

Amazon DynamoDB answers a body with two type faults (members it cannot
convert to their type) with the fault of the member written first, and a
type fault beside a validation fault with the type fault in either order.
For a Limit of 0.0 beside an empty value it gives the Limit fault in both
orders. Inside one map the other checks visit the members in the order a
Java HashMap of the map yields them. There the order written decides only
between two members that share a bucket, such as `:a` and `:e`. For two
malformed keys in ExpressionAttributeValues and for two empty names, the
service named either member from one send to the next, so no row pins them.
Measured in us-east-1 on 2026-11-12 (Query, Scan, TransactWriteItems,
ExecuteStatement), 2026-11-16 (GetItem, PutItem, UpdateItem) and 2026-11-19
(UpdateItem and ExecuteStatement). Each row pins the service's status, error
type and message for one body in one member order.

ExtendDB words a type fault with serde_json's text or with its own (#1012),
so on ExtendDB a type-fault row checks that text instead. For two type faults
the text names the member written first, so a server that reads the members
in another order fails the row.

The bodies are posted with their members in the order written, signed as the
client's caller, because botocore refuses these values before it sends them.

Dual-target against Amazon DynamoDB and ExtendDB.
"""

from __future__ import annotations

from typing import Any

import boto3
import pytest

from conftest import _is_real_dynamodb, scoped_table
from raw_signed_request import answer, signed_post
from test_partiql_numbers import NESTED, UNCONVERTIBLE, deep

ON_EXTENDDB = not _is_real_dynamodb()


def _differs(reason: str):
    """ExtendDB answers the row otherwise, in every build."""
    return pytest.mark.xfail(ON_EXTENDDB, reason=reason, raises=AssertionError, strict=True)


# 720: a fractional Limit is a SerializationException on ExtendDB.
FRACTIONAL_LIMIT = _differs("#720: a fractional Limit answers SerializationException")
# 778: a Limit fault after an ExpressionAttributeValues fault answers the value fault.
LIMIT_AFTER_VALUE = _differs("#778: the value fault written first is answered")

SERDE = "Start of structure or map found where not expected: "

P = {"S": "p0"}
KEY = {"pk": {"S": "k0"}}
EAV_P = {"ExpressionAttributeValues": {":p": P}}
EAV_EMPTY = {"ExpressionAttributeValues": {":p": P, ":a": {}}}

STRING_TO_INTEGER = (400, "SerializationException", "STRING_VALUE cannot be converted to Integer")
NUMBER_TO_STRING = (400, "SerializationException", "NUMBER_VALUE cannot be converted to String")
UNEXPECTED_TOKEN = (400, "SerializationException", "Unexpected token received from parser")
UNEXPECTED_FIELD_TYPE = (400, "SerializationException", "Unexpected field type")
LIMIT_BELOW_ONE = (
    400,
    "ValidationException",
    "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1",
)


def _empty(key: str) -> tuple[int, str, str]:
    return (
        400,
        "ValidationException",
        "ExpressionAttributeValues contains invalid value: Supplied AttributeValue is empty, "
        f"must contain exactly one of the supported datatypes for key {key}",
    )


EMPTY_B = _empty(":b")
EMPTY_NAME_C = (
    400,
    "ValidationException",
    "ExpressionAttributeNames contains invalid value: Empty attribute name for key #c",
)
BAD_VALUES_KEY_B = (
    400,
    "ValidationException",
    'ExpressionAttributeValues contains invalid key: Syntax error; key: "b"',
)
NOT_A_NUMBER = (400, "ValidationException", f"{UNCONVERTIBLE}: abc")
NESTING = (400, "ValidationException", NESTED)

# serde_json's description of each faulty member, or ExtendDB's own, as
# ExtendDB reports it.
LIMIT = 'invalid type: string "0", expected i64'
CONSISTENT_READ = 'invalid type: string "x", expected a boolean'
NUMBER_FOR_STRING = "invalid type: integer `{}`, expected a string"
STRING_FOR_MAP = 'invalid type: string "x", expected a map'
NUMBER_IN_S = "S value must be a string"
STRING_IN_BOOL = "BOOL value must be a boolean"


def _query(table: str, *members: dict[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {"TableName": table, "KeyConditionExpression": "pk = :p"}
    for member in members:
        body.update(member)
    return body


def _scan(table: str, values: dict[str, Any]) -> dict[str, Any]:
    return {
        "TableName": table,
        "FilterExpression": "pk IN (:p, :a, :b)",
        "ExpressionAttributeValues": {":p": P, **values},
    }


def _put(put: dict[str, Any]) -> dict[str, Any]:
    return {"TransactItems": [{"Put": put}]}


def _update(table: str, expression: str, values: dict[str, Any], names=None) -> dict[str, Any]:
    body: dict[str, Any] = {"TableName": table, "Key": KEY, "UpdateExpression": expression}
    if names is not None:
        body["ExpressionAttributeNames"] = names
    body["ExpressionAttributeValues"] = values
    return body


def _select(table: str, members: dict[str, Any]) -> dict[str, Any]:
    return {
        "Statement": f"SELECT * FROM \"{table}\" WHERE pk = 'p' AND x = ?",
        "Parameters": [{"M": members}],
    }


def _one_map(name, operation, build, first, second, answers, extenddb_texts=None):
    """The rows of one map with two faults: the members of ``first`` written
    ahead of those of ``second``, then after them. ``build`` takes the table
    and the map. ``answers`` is the service's answer to both orders, or a list
    of its answer to each, and ``extenddb_texts`` the same for ExtendDB's
    text."""
    if not isinstance(answers, list):
        answers = [answers, answers]
    if not isinstance(extenddb_texts, list):
        extenddb_texts = [extenddb_texts, extenddb_texts]
    orders = [(first, second, "then"), (second, first, "after")]
    return [
        pytest.param(
            operation,
            lambda t, a=a, b=b: build(t, {**dict(a), **dict(b)}),
            answer,
            extenddb_text,
            id=f"{name}_{word}",
        )
        for (a, b, word), answer, extenddb_text in zip(orders, answers, extenddb_texts)
    ]


# (operation, body, service answer, ExtendDB's serde_json text or None)
ROWS = [
    # Two type faults: the member written first.
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_P, {"Limit": "0"}, {"ConsistentRead": "x"}),
        STRING_TO_INTEGER,
        LIMIT,
        id="query_limit_then_consistent_read",
    ),
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_P, {"ConsistentRead": "x"}, {"Limit": "0"}),
        UNEXPECTED_TOKEN,
        CONSISTENT_READ,
        id="query_consistent_read_then_limit",
    ),
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_P, {"Select": 7}, {"Limit": "0"}),
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(7),
        id="query_select_then_limit",
    ),
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_P, {"Limit": "0"}, {"Select": 7}),
        STRING_TO_INTEGER,
        LIMIT,
        id="query_limit_then_select",
    ),
    pytest.param(
        "TransactWriteItems",
        lambda t: _put({"TableName": 5, "Item": "x"}),
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(5),
        id="transact_put_table_name_then_item",
    ),
    pytest.param(
        "TransactWriteItems",
        lambda t: _put({"Item": "x", "TableName": 5}),
        UNEXPECTED_FIELD_TYPE,
        STRING_FOR_MAP,
        id="transact_put_item_then_table_name",
    ),
    pytest.param(
        "GetItem",
        lambda t: {"TableName": t, "Key": KEY, "ConsistentRead": "x", "ProjectionExpression": 5},
        UNEXPECTED_TOKEN,
        CONSISTENT_READ,
        id="get_consistent_read_then_projection",
    ),
    pytest.param(
        "GetItem",
        lambda t: {"TableName": t, "Key": KEY, "ProjectionExpression": 5, "ConsistentRead": "x"},
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(5),
        id="get_projection_then_consistent_read",
    ),
    pytest.param(
        "PutItem",
        lambda t: {"TableName": t, "Item": KEY, "ReturnValues": 5, "Expected": "x"},
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(5),
        id="put_return_values_then_expected",
    ),
    pytest.param(
        "PutItem",
        lambda t: {"TableName": t, "Item": KEY, "Expected": "x", "ReturnValues": 5},
        UNEXPECTED_FIELD_TYPE,
        STRING_FOR_MAP,
        id="put_expected_then_return_values",
    ),
    pytest.param(
        "UpdateItem",
        lambda t: {"TableName": t, "Key": KEY, "ReturnValues": 5, "ExpressionAttributeNames": "x"},
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(5),
        id="update_return_values_then_names",
    ),
    pytest.param(
        "UpdateItem",
        lambda t: {"TableName": t, "Key": KEY, "ExpressionAttributeNames": "x", "ReturnValues": 5},
        UNEXPECTED_FIELD_TYPE,
        STRING_FOR_MAP,
        id="update_names_then_return_values",
    ),
    # A type fault beside a validation fault: the type fault in both orders.
    pytest.param(
        "Query",
        lambda t: _query(t, {"Limit": "0"}, EAV_EMPTY),
        STRING_TO_INTEGER,
        LIMIT,
        id="query_string_limit_then_empty_value",
    ),
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_EMPTY, {"Limit": "0"}),
        STRING_TO_INTEGER,
        None,
        id="query_empty_value_then_string_limit",
        marks=LIMIT_AFTER_VALUE,
    ),
    # Two validation faults in two members: one answer in both orders. The
    # service reads a Limit of 0.0 as 0. ExtendDB answers it as a type fault
    # (#720).
    pytest.param(
        "Query",
        lambda t: _query(t, {"Limit": 0.0}, EAV_EMPTY),
        LIMIT_BELOW_ONE,
        None,
        id="query_float_limit_then_empty_value",
        marks=FRACTIONAL_LIMIT,
    ),
    pytest.param(
        "Query",
        lambda t: _query(t, EAV_EMPTY, {"Limit": 0.0}),
        LIMIT_BELOW_ONE,
        None,
        id="query_empty_value_then_float_limit",
        marks=LIMIT_AFTER_VALUE,
    ),
    # Two faults in one map. A value of the wrong JSON type is answered
    # first. Then come the keys in ExpressionAttributeValues, and the empty
    # names ahead of the keys in ExpressionAttributeNames. The other checks
    # visit the members in the order a Java HashMap of the map yields them.
    # In 4 buckets :b is in bucket 0, :c in 1, and :a and :e in 3. Also in
    # 4, bad and x are in 0, and a and e in 1. In the 8 buckets of a map of
    # four members, a is in 1 and bad in 4.
    *_one_map(
        "update_empty_c_b",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :b, y = :c", v),
        [(":c", {})],
        [(":b", {})],
        EMPTY_B,
    ),
    *_one_map(
        "update_empty_a_c",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :a, y = :c", v),
        [(":a", {})],
        [(":c", {})],
        _empty(":c"),
    ),
    *_one_map(
        "update_empty_a_e",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :a, y = :e", v),
        [(":a", {})],
        [(":e", {})],
        [_empty(":a"), _empty(":e")],
    ),
    # Three members take 4 buckets. In 8, :e would come ahead of :a.
    *_one_map(
        "update_empty_a_e_p",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :a, y = :e, z = :p", v),
        [(":a", {})],
        [(":e", {}), (":p", P)],
        [_empty(":a"), _empty(":e")],
    ),
    # The service answers {"X": "1"} with the text for an empty value.
    # ExtendDB answers it as a type fault (#629), but it checks :b first.
    *_one_map(
        "scan_empty_b_unknown_a",
        "Scan",
        _scan,
        [(":b", {})],
        [(":a", {"X": "1"})],
        EMPTY_B,
    ),
    *_one_map(
        "update_empty_b_number_in_a",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :a, y = :b", v),
        [(":b", {})],
        [(":a", {"S": 5})],
        NUMBER_TO_STRING,
        NUMBER_IN_S,
    ),
    # Two type faults in one map: the member written first, though :b is in
    # a lower bucket than :c.
    *_one_map(
        "update_number_in_c_string_in_b",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :b, y = :c", v),
        [(":c", {"S": 5})],
        [(":b", {"BOOL": "x"})],
        [NUMBER_TO_STRING, UNEXPECTED_TOKEN],
        [NUMBER_IN_S, STRING_IN_BOOL],
    ),
    *_one_map(
        "update_key_b_empty_c",
        "UpdateItem",
        lambda t, v: _update(t, "SET x = :c", v),
        [("b", P)],
        [(":c", {})],
        BAD_VALUES_KEY_B,
    ),
    *_one_map(
        "update_empty_name_c_number_name_b",
        "UpdateItem",
        lambda t, n: _update(t, "SET #b = :p, #c = :p", {":p": P}, n),
        [("#c", "")],
        [("#b", 5)],
        NUMBER_TO_STRING,
        NUMBER_FOR_STRING.format(5),
    ),
    *_one_map(
        "update_empty_name_c_null_name_b",
        "UpdateItem",
        lambda t, n: _update(t, "SET #b = :p, #c = :p", {":p": P}, n),
        [("#c", "")],
        [("#b", None)],
        EMPTY_NAME_C,
    ),
    *_one_map(
        "select_deep_e_bad",
        "ExecuteStatement",
        _select,
        [("e", deep(40))],
        [("bad", {"N": "abc"})],
        NOT_A_NUMBER,
    ),
    *_one_map(
        "select_bad_deep_a",
        "ExecuteStatement",
        _select,
        [("bad", {"N": "abc"})],
        [("a", deep(40))],
        NOT_A_NUMBER,
    ),
    *_one_map(
        "select_bad_deep_a_x",
        "ExecuteStatement",
        _select,
        [("bad", {"N": "abc"})],
        [("a", deep(40)), ("x", {"S": "1"})],
        NOT_A_NUMBER,
    ),
    *_one_map(
        "select_bad_deep_a_x_y",
        "ExecuteStatement",
        _select,
        [("bad", {"N": "abc"})],
        [("a", deep(40)), ("x", {"S": "1"}), ("y", {"S": "1"})],
        NESTING,
    ),
]


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    """Hash-only table for the module, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name


def _answer(client, endpoint_url: str | None, operation: str, body: dict[str, Any]):
    """The status, the error type and the message of ``body``'s answer."""
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    return answer(
        signed_post(
            endpoint_url or client.meta.endpoint_url,
            creds.access_key,
            creds.secret_key,
            client.meta.region_name,
            operation,
            body,
            session_token=creds.token,
        )
    )


@pytest.mark.parametrize("operation, body, expected, extenddb_text", ROWS)
def test_two_fault_body_gets_the_service_answer(
    dynamodb_client, endpoint_url, hash_table, operation, body, expected, extenddb_text
):
    if endpoint_url and extenddb_text:
        expected = (400, "SerializationException", SERDE + extenddb_text)
    assert _answer(dynamodb_client, endpoint_url, operation, body(hash_table)) == expected
