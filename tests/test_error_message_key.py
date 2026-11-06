# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The key that carries an error's message in the raw response body.

Amazon DynamoDB names it ``Message`` on TransactionCanceledException and
IdempotentParameterMismatchException, whose modeled member is ``Message``,
and ``message`` on the other errors (raw bodies measured in us-east-1 on
2026-10-29 and 2026-11-03). botocore reads either key, so these tests read
the raw body from botocore's ``before-parse`` event before it normalizes it.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    """Hash-only table for the module, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name


def _error_code_and_keys(client, operation: str, call) -> tuple[str, list[str]]:
    """Run ``call``, which must fail, and return the error code and the sorted
    top-level keys of the raw response body."""
    raw: list[bytes] = []

    def capture(response_dict, **_kwargs):
        raw.append(response_dict["body"])

    event = f"before-parse.dynamodb.{operation}"
    client.meta.events.register(event, capture)
    try:
        with pytest.raises(ClientError) as exc_info:
            call()
    finally:
        client.meta.events.unregister(event, capture)
    return exc_info.value.response["Error"]["Code"], sorted(json.loads(raw[-1]))


def test_transaction_canceled_carries_message_under_capital_m(dynamodb_client, hash_table):
    code, keys = _error_code_and_keys(
        dynamodb_client,
        "TransactWriteItems",
        lambda: dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": f"missing-{uuid.uuid4().hex}"}},
                        "ConditionExpression": "attribute_exists(pk)",
                    }
                }
            ]
        ),
    )
    assert code == "TransactionCanceledException"
    assert keys == ["CancellationReasons", "Message", "__type"]


def test_idempotent_parameter_mismatch_carries_message_under_capital_m(
    dynamodb_client, hash_table
):
    token = uuid.uuid4().hex
    pk = f"token-{token}"

    def put(value: str):
        return dynamodb_client.transact_write_items(
            ClientRequestToken=token,
            TransactItems=[
                {"Put": {"TableName": hash_table, "Item": {"pk": {"S": pk}, "v": {"S": value}}}}
            ],
        )

    put("first")
    code, keys = _error_code_and_keys(dynamodb_client, "TransactWriteItems", lambda: put("second"))
    assert code == "IdempotentParameterMismatchException"
    assert keys == ["Message", "__type"]


def test_conditional_check_failed_carries_message_under_lower_case(dynamodb_client, hash_table):
    code, keys = _error_code_and_keys(
        dynamodb_client,
        "PutItem",
        lambda: dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": f"missing-{uuid.uuid4().hex}"}},
            ConditionExpression="attribute_exists(pk)",
        ),
    )
    assert code == "ConditionalCheckFailedException"
    assert keys == ["__type", "message"]


def test_validation_error_carries_message_under_lower_case(dynamodb_client, hash_table):
    code, keys = _error_code_and_keys(
        dynamodb_client,
        "GetItem",
        lambda: dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"N": "1"}}),
    )
    assert code == "ValidationException"
    assert keys == ["__type", "message"]
