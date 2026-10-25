# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A character outside the expression grammar.

Amazon DynamoDB answers ``Invalid <Kind>: Syntax error; token: "<t>", near:
"<window>"``: the token is the character as sent, a whole code point, and the
window runs from the start of the previous token to the end of the next one,
with each character outside the grammar counted as one token. The write paths
carry a ``1 validation error detected: `` prefix that ExtendDB does not emit;
those assertions compare the text after it.

Every expected text was measured against Amazon DynamoDB on 2026-10-25
(us-east-1), each request twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

PREFIX = "1 validation error detected: "
NAMES = {"#v": "v"}
VALUES = {":v": {"S": "x"}}
ABSENT = {"pk": {"S": "absent"}, "sk": {"S": "absent"}}


def _message(func) -> str:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    msg = err["Message"]
    return msg[len(PREFIX) :] if msg.startswith(PREFIX) else msg


def syntax(kind: str, token: str, near: str) -> str:
    return f'Invalid {kind}: Syntax error; token: "{token}", near: "{near}"'


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
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "v": {"S": "x"}}
        )
        yield name


@pytest.mark.parametrize(
    "expr, token, near",
    [
        ("#v = \u00e9\u00e9\u00e9", "\u00e9", "= \u00e9\u00e9"),
        ("#v = \u00e9", "\u00e9", "= \u00e9"),
        ("\u00e9 = :v", "\u00e9", "\u00e9 ="),
        ("#v = :v AND \u00e9", "\u00e9", "AND \u00e9"),
        ("#v = \u00e9 #v", "\u00e9", "= \u00e9 #v"),
        ("#v = \u65e5\u672c\u8a9e", "\u65e5", "= \u65e5\u672c"),
        ("#v = \U0001F600x", "\U0001F600", "= \U0001F600x"),
        ("#v = $x", "$", "= $x"),
        ("#v = @", "@", "= @"),
        ("#v = a\u00e9", "\u00e9", "a\u00e9"),
        ("ab\u00e9c = :v", "\u00e9", "ab\u00e9c"),
        ("#longname = \u00e9", "\u00e9", "= \u00e9"),
        ("#v = \u00e9 \u00e9", "\u00e9", "= \u00e9 \u00e9"),
        ("(#v = \u00e9)", "\u00e9", "= \u00e9)"),
        ("contains(#v, \u00e9)", "\u00e9", ", \u00e9)"),
        ("#v = :v !", "!", ":v !"),
        ("#v =\t\u00e9", "\u00e9", "=\t\u00e9"),
        ("#v =\u00e9", "\u00e9", "=\u00e9"),
    ],
)
def test_scan_filter(dynamodb_client, table, expr, token, near):
    msg = _message(
        lambda: dynamodb_client.scan(
            TableName=table,
            Limit=1,
            FilterExpression=expr,
            ExpressionAttributeNames=NAMES,
            ExpressionAttributeValues=VALUES,
        )
    )
    assert msg == syntax("FilterExpression", token, near)


@pytest.mark.parametrize(
    "condition, token, near",
    [
        ("#v = \u00e9\u00e9\u00e9", "\u00e9", "= \u00e9\u00e9"),
        ("#v = $x", "$", "= $x"),
    ],
)
def test_put_item_condition(dynamodb_client, table, condition, token, near):
    msg = _message(
        lambda: dynamodb_client.put_item(
            TableName=table,
            Item=ABSENT,
            ConditionExpression="attribute_exists(pk) AND " + condition,
            ExpressionAttributeNames=NAMES,
            ExpressionAttributeValues=VALUES,
        )
    )
    assert msg == syntax("ConditionExpression", token, near)


@pytest.mark.parametrize(
    "kce, token, near",
    [
        ("pk = \u00e9", "\u00e9", "= \u00e9"),
        ("pk = :v AND \u00e9", "\u00e9", "AND \u00e9"),
    ],
)
def test_query_key_condition(dynamodb_client, table, kce, token, near):
    msg = _message(
        lambda: dynamodb_client.query(
            TableName=table, Limit=1, KeyConditionExpression=kce, ExpressionAttributeValues=VALUES
        )
    )
    assert msg == syntax("KeyConditionExpression", token, near)


@pytest.mark.parametrize(
    "update, token, near",
    [
        ("SET #v = \u00e9\u00e9", "\u00e9", "= \u00e9\u00e9"),
        ("SET #v = $x", "$", "= $x"),
    ],
)
def test_update_expression(dynamodb_client, table, update, token, near):
    msg = _message(
        lambda: dynamodb_client.update_item(
            TableName=table,
            Key=ABSENT,
            UpdateExpression=update,
            ConditionExpression="attribute_exists(pk)",
            ExpressionAttributeNames=NAMES,
        )
    )
    assert msg == syntax("UpdateExpression", token, near)


@pytest.mark.parametrize(
    "projection, token, near",
    [
        ("#v, \u00e9", "\u00e9", ", \u00e9"),
        ("#v, $x", "$", ", $x"),
    ],
)
def test_projection_expression(dynamodb_client, table, projection, token, near):
    msg = _message(
        lambda: dynamodb_client.get_item(
            TableName=table, Key=ABSENT, ProjectionExpression=projection,
            ExpressionAttributeNames=NAMES,
        )
    )
    assert msg == syntax("ProjectionExpression", token, near)
