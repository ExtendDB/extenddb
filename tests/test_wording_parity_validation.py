# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Validation message/behavior parity (verified against real DynamoDB, us-east-1):

- PutItem/DeleteItem ReturnValues: a valid-but-disallowed enum value
  (UPDATED_OLD) -> "ReturnValues can only be ALL_OLD or NONE"; a non-enum value
  (GARBAGE) -> generic constraint error with the full enum set.
- Query/Scan Select + ProjectionExpression rejection carries the
  "1 validation error detected: " prefix.
- Query/Scan Select=ALL_ATTRIBUTES on a non-ALL GSI is rejected.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


@pytest.fixture()
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]["TableName"]


@pytest.fixture()
def gsi_table(create_and_cleanup_table):
    # KEYS_ONLY GSI so Select=ALL_ATTRIBUTES against it is invalid.
    result = create_and_cleanup_table(
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "g_index",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    )
    return result["TableDescription"]["TableName"]


def test_put_disallowed_return_values(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.put_item(
            TableName=table, Item={"pk": {"S": "k"}}, ReturnValues="UPDATED_OLD"
        )
    assert ei.value.response["Error"]["Message"] == "ReturnValues can only be ALL_OLD or NONE"


def test_delete_disallowed_return_values(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.delete_item(
            TableName=table, Key={"pk": {"S": "k"}}, ReturnValues="ALL_NEW"
        )
    assert ei.value.response["Error"]["Message"] == "ReturnValues can only be ALL_OLD or NONE"


def test_put_invalid_return_values_enum(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.put_item(
            TableName=table, Item={"pk": {"S": "k"}}, ReturnValues="GARBAGE"
        )
    msg = ei.value.response["Error"]["Message"]
    assert msg == (
        "1 validation error detected: Value 'GARBAGE' at 'returnValues' failed to "
        "satisfy constraint: Member must satisfy enum value set: "
        "[ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]"
    )


def test_query_count_with_projection_prefix(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.query(
            TableName=table,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "x"}},
            Select="COUNT",
            ProjectionExpression="pk",
        )
    assert ei.value.response["Error"]["Message"] == (
        "1 validation error detected: Cannot specify the ProjectionExpression "
        "when choosing to get only the Count"
    )


def test_scan_all_attributes_on_non_all_gsi(dynamodb_client, gsi_table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.scan(
            TableName=gsi_table, IndexName="g_index", Select="ALL_ATTRIBUTES"
        )
    assert ei.value.response["Error"]["Code"] == "ValidationException"
    assert (
        "Select type ALL_ATTRIBUTES is not supported for global secondary index "
        "g_index because its projection type is not ALL"
        in ei.value.response["Error"]["Message"]
    )


def test_query_all_attributes_on_non_all_gsi(dynamodb_client, gsi_table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.query(
            TableName=gsi_table,
            IndexName="g_index",
            KeyConditionExpression="g = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
            Select="ALL_ATTRIBUTES",
        )
    assert (
        "Select type ALL_ATTRIBUTES is not supported"
        in ei.value.response["Error"]["Message"]
    )
