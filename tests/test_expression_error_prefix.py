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
