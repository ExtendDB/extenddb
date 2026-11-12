# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Select=ALL_PROJECTED_ATTRIBUTES without an IndexName.

Amazon DynamoDB answers ``1 validation error detected:
ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName``
on Query, with a KeyConditionExpression or legacy KeyConditions and with
ConsistentRead, and on Scan (measured 2026-10-25 at 21:13 UTC, us-east-1,
each request twice).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

MESSAGE = (
    "1 validation error detected: "
    "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
)


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
        yield name


def _message(func) -> str:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


@pytest.mark.parametrize("consistent", [False, True])
def test_query_key_condition_expression(dynamodb_client, table, consistent):
    msg = _message(
        lambda: dynamodb_client.query(
            TableName=table,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "absent"}},
            Select="ALL_PROJECTED_ATTRIBUTES",
            ConsistentRead=consistent,
        )
    )
    assert msg == MESSAGE


def test_query_legacy_key_conditions(dynamodb_client, table):
    msg = _message(
        lambda: dynamodb_client.query(
            TableName=table,
            KeyConditions={
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "absent"}]}
            },
            Select="ALL_PROJECTED_ATTRIBUTES",
        )
    )
    assert msg == MESSAGE


def test_scan(dynamodb_client, table):
    msg = _message(
        lambda: dynamodb_client.scan(TableName=table, Select="ALL_PROJECTED_ATTRIBUTES")
    )
    assert msg == MESSAGE
