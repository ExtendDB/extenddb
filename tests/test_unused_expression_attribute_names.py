# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Unused ExpressionAttributeNames validation on the read path.

Amazon DynamoDB rejects a request that declares an entry in
ExpressionAttributeNames which is never referenced by any expression
(here, the ProjectionExpression). GetItem and BatchGetItem must enforce
this the same way Query, Scan, PutItem, DeleteItem, and UpdateItem do.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "foo": {"S": "a"}, "bar": {"S": "b"}},
        )
        yield name


class TestUnusedExpressionAttributeNames:
    """ExpressionAttributeNames entries must be referenced by an expression."""

    def test_get_item_unused_name_existing_item(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="pk",
                ExpressionAttributeNames={"#abc": "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "unused in expressions" in err["Message"]
        assert "#abc" in err["Message"]

    def test_get_item_unused_name_missing_item(self, dynamodb_client, hash_table):
        # Validation must fire even when the key does not exist.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "does-not-exist"}},
                ProjectionExpression="pk",
                ExpressionAttributeNames={"#abc": "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "unused in expressions" in err["Message"]

    def test_get_item_used_name_succeeds(self, dynamodb_client, hash_table):
        # Positive control: a referenced name is fine and projection narrows.
        resp = dynamodb_client.get_item(
            TableName=hash_table,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression="#abc",
            ExpressionAttributeNames={"#abc": "foo"},
        )
        assert resp["Item"] == {"foo": {"S": "a"}}

    def test_batch_get_item_unused_name(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={
                    hash_table: {
                        "Keys": [{"pk": {"S": "k1"}}],
                        "ProjectionExpression": "pk",
                        "ExpressionAttributeNames": {"#abc": "foo"},
                    }
                }
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "unused in expressions" in err["Message"]

    def test_batch_get_item_used_name_succeeds(self, dynamodb_client, hash_table):
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                hash_table: {
                    "Keys": [{"pk": {"S": "k1"}}],
                    "ProjectionExpression": "#abc",
                    "ExpressionAttributeNames": {"#abc": "foo"},
                }
            }
        )
        assert resp["Responses"][hash_table] == [{"foo": {"S": "a"}}]
