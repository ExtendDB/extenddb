# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for SET evaluation snapshot semantics and parenthesised arithmetic."""

from __future__ import annotations

import uuid

import pytest


@pytest.fixture()
def hash_table(dynamodb_client):
    name = f"set_eval_{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    from conftest import wait_for_active
    wait_for_active(dynamodb_client, name)
    yield name
    dynamodb_client.delete_table(TableName=name)


class TestSetSnapshotSemantics:
    """SET clauses evaluate RHS against the pre-update item snapshot."""

    def test_second_set_reads_old_value(self, dynamodb_client, hash_table):
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "x"}, "a": {"S": "OLD"}},
        )
        resp = dynamodb_client.update_item(
            TableName=hash_table,
            Key={"pk": {"S": "x"}},
            UpdateExpression="SET a = :v, b = a",
            ExpressionAttributeValues={":v": {"S": "NEW"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"]["a"]["S"] == "NEW"
        assert resp["Attributes"]["b"]["S"] == "OLD"


class TestParenthesisedArithmetic:
    """SET supports parenthesised arithmetic expressions."""

    def test_subtraction_in_parens(self, dynamodb_client, hash_table):
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "y"}, "c": {"N": "10"}},
        )
        resp = dynamodb_client.update_item(
            TableName=hash_table,
            Key={"pk": {"S": "y"}},
            UpdateExpression="SET c = (c - :v)",
            ExpressionAttributeValues={":v": {"N": "3"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"]["c"]["N"] == "7"

    def test_addition_in_parens(self, dynamodb_client, hash_table):
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "z"}, "n": {"N": "5"}},
        )
        resp = dynamodb_client.update_item(
            TableName=hash_table,
            Key={"pk": {"S": "z"}},
            UpdateExpression="SET n = (n + :v)",
            ExpressionAttributeValues={":v": {"N": "2"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"]["n"]["N"] == "7"
