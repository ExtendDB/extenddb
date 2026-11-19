# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Request helpers and the table fixture shared by the expression fault tests.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

PREFIX = "1 validation error detected: "


def _error(func) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


def _validation(func) -> str:
    code, msg = _error(func)
    assert code == "ValidationException", (code, msg)
    return msg


def _after_prefix(msg: str) -> str:
    return msg[len(PREFIX) :] if msg.startswith(PREFIX) else msg


def chain(term: str, n: int, joiner: str = " OR ") -> str:
    return joiner.join([term] * n)


def syntax(kind: str, token: str, near: str) -> str:
    return f'Invalid {kind}: Syntax error; token: "{token}", near: "{near}"'


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """Composite-key table with one item for the Query and Scan rows."""
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
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "v": {"S": "x"}},
        )
        yield name


NAMES = {"#v": "v"}
VALUES = {":v": {"S": "x"}}


def put(client, table, condition, **kwargs):
    args = {
        "TableName": table,
        "Item": {"pk": {"S": "absent"}, "sk": {"S": "absent"}},
        "ConditionExpression": condition,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.put_item(**args)


def scan(client, table, filter_expr, **kwargs):
    args = {
        "TableName": table,
        "FilterExpression": filter_expr,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.scan(**args)


def query(client, table, kce, **kwargs):
    args = {
        "TableName": table,
        "KeyConditionExpression": kce,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.query(**args)
