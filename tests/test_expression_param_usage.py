# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""ExpressionAttributeNames/Values supplied with no expression that uses them.

Amazon DynamoDB rejects a request that provides ExpressionAttributeNames or
ExpressionAttributeValues when there is no expression parameter that could
reference them. The names message is always suffix-free; the values message
names the null value-capable expression parameter(s) for that API.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NAMES_MSG = "ExpressionAttributeNames can only be specified when using expressions"
VALUES_PREFIX = "ExpressionAttributeValues can only be specified when using expressions"


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class, with one item, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "foo": {"S": "a"}},
        )
        yield name


def _expect_validation(func, expected_message: str):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert err["Message"] == expected_message


class TestExpressionParamsWithoutExpression:
    """Names/values with no referencing expression must be rejected."""

    def test_get_item_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ExpressionAttributeNames={"#a": "foo"},
            ),
            NAMES_MSG,
        )

    def test_batch_get_item_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.batch_get_item(
                RequestItems={
                    hash_table: {
                        "Keys": [{"pk": {"S": "k1"}}],
                        "ExpressionAttributeNames": {"#a": "foo"},
                    }
                }
            ),
            NAMES_MSG,
        )

    def test_scan_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ExpressionAttributeNames={"#a": "foo"},
            ),
            NAMES_MSG,
        )

    def test_scan_values_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            f"{VALUES_PREFIX}: FilterExpression is null",
        )

    def test_delete_item_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.delete_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ExpressionAttributeNames={"#a": "foo"},
            ),
            NAMES_MSG,
        )

    def test_delete_item_values_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.delete_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            f"{VALUES_PREFIX}: ConditionExpression is null",
        )

    def test_update_item_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ExpressionAttributeNames={"#a": "foo"},
            ),
            NAMES_MSG,
        )

    def test_update_item_values_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            f"{VALUES_PREFIX}: UpdateExpression and ConditionExpression are null",
        )

    def test_put_item_names_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                ExpressionAttributeNames={"#a": "foo"},
            ),
            NAMES_MSG,
        )

    def test_put_item_values_no_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            f"{VALUES_PREFIX}: ConditionExpression is null",
        )

    def test_query_names_no_expression(self, dynamodb_client, hash_table):
        # Legacy KeyConditions is not an expression; stray names are rejected.
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                KeyConditions={
                    "pk": {
                        "AttributeValueList": [{"S": "k1"}],
                        "ComparisonOperator": "EQ",
                    }
                },
                ExpressionAttributeNames={"#a": "pk"},
            ),
            NAMES_MSG,
        )

    def test_query_values_no_expression(self, dynamodb_client, hash_table):
        # Query emits no values suffix, unlike Scan/Delete/Update.
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                KeyConditions={
                    "pk": {
                        "AttributeValueList": [{"S": "k1"}],
                        "ComparisonOperator": "EQ",
                    }
                },
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            VALUES_PREFIX,
        )
