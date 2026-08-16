# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Transaction-API primary-key size validation.

An oversized hash key (> 2048 bytes) or range key (> 1024 bytes) in any
transaction sub-op must cancel the transaction with a per-item ValidationError
cancellation reason (TransactionCanceledException), matching real DynamoDB.
Covers TransactGetItems (Get) and each TransactWriteItems sub-op
(Put / Delete / Update / ConditionCheck).

An EMPTY key value, by contrast, is a top-level ValidationException — verified
here to lock the size-vs-empty distinction.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

OVERSIZED_HASH = "a" * 2049


@pytest.fixture()
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]["TableName"]


def _assert_cancelled_validation(ei):
    err = ei.value.response
    assert err["Error"]["Code"] == "TransactionCanceledException", err["Error"]
    reasons = err.get("CancellationReasons")
    assert reasons, "expected CancellationReasons in the error response"
    assert any(r.get("Code") == "ValidationError" for r in reasons), reasons


def test_transact_write_put_oversized_hash_key_cancels(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": table, "Item": {"pk": {"S": OVERSIZED_HASH}}}}
            ]
        )
    _assert_cancelled_validation(ei)


def test_transact_write_delete_oversized_hash_key_cancels(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Delete": {"TableName": table, "Key": {"pk": {"S": OVERSIZED_HASH}}}}
            ]
        )
    _assert_cancelled_validation(ei)


def test_transact_write_update_oversized_hash_key_cancels(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"pk": {"S": OVERSIZED_HASH}},
                        "UpdateExpression": "SET #d = :v",
                        "ExpressionAttributeNames": {"#d": "data"},
                        "ExpressionAttributeValues": {":v": {"S": "x"}},
                    }
                }
            ]
        )
    _assert_cancelled_validation(ei)


def test_transact_write_condition_check_oversized_hash_key_cancels(
    dynamodb_client, table
):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": table,
                        "Key": {"pk": {"S": OVERSIZED_HASH}},
                        "ConditionExpression": "attribute_exists(pk)",
                    }
                }
            ]
        )
    _assert_cancelled_validation(ei)


def test_transact_get_oversized_hash_key_cancels(dynamodb_client, table):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": table, "Key": {"pk": {"S": OVERSIZED_HASH}}}}
            ]
        )
    _assert_cancelled_validation(ei)


def test_transact_write_empty_hash_key_is_top_level_validation(dynamodb_client, table):
    # Empty key value is a top-level ValidationException, NOT a per-item
    # cancellation — the size-vs-empty distinction.
    with pytest.raises(ClientError) as ei:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": table, "Item": {"pk": {"S": ""}}}}
            ]
        )
    assert ei.value.response["Error"]["Code"] == "ValidationException"


def test_transact_get_valid_key_succeeds(dynamodb_client, table):
    resp = dynamodb_client.transact_get_items(
        TransactItems=[{"Get": {"TableName": table, "Key": {"pk": {"S": "ok"}}}}]
    )
    assert "Responses" in resp
