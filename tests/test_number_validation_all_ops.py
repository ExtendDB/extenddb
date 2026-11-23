# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Number validation on the batch, transaction and condition value paths (#618).

One request for each form of the message, and one showing that a refused
BatchWriteItem writes nothing. Every expectation is DynamoDB's answer, measured
in us-east-1 with each request sent twice and the same answer both times.
BatchWriteItem gives the number message with no prefix and PutItem puts
"1 validation error detected: " ahead of it (2026-10-27). TransactWriteItems
gives the number message for a value in ExpressionAttributeValues with no
prefix and no wrapper (2026-11-11). The Rust wire tests in
tests/rust/src/number_validation_all_ops.rs hold the other paths.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

NOT_A_NUMBER = "The parameter cannot be converted to a numeric value: abc"
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than "
    "supported range"
)


@pytest.fixture()
def table(create_and_cleanup_table) -> str:
    return create_and_cleanup_table()["TableDescription"]["TableName"]


def _refusal(call) -> tuple[str, str]:
    with pytest.raises(ClientError) as ei:
        call()
    err = ei.value.response["Error"]
    return err["Code"], err["Message"]


def test_batch_write_item_gives_the_number_message_without_a_prefix(dynamodb_client, table):
    answer = _refusal(lambda: dynamodb_client.batch_write_item(RequestItems={table: [
        {"PutRequest": {"Item": {"pk": {"S": "b618"}, "n": {"N": "abc"}}}},
    ]}))
    assert answer == ("ValidationException", NOT_A_NUMBER)


def test_put_item_puts_the_prefix_on_a_condition_value(dynamodb_client, table):
    answer = _refusal(lambda: dynamodb_client.put_item(
        TableName=table,
        Item={"pk": {"S": "p618"}},
        ConditionExpression="attribute_not_exists(amount) OR amount = :v",
        ExpressionAttributeValues={":v": {"N": "1E126"}},
    ))
    assert answer == ("ValidationException", f"1 validation error detected: {OVERFLOW}")


def test_transact_write_items_gives_a_value_number_message_without_a_wrapper(
    dynamodb_client, table
):
    answer = _refusal(lambda: dynamodb_client.transact_write_items(TransactItems=[
        {"Update": {
            "TableName": table,
            "Key": {"pk": {"S": "t618"}},
            "UpdateExpression": "SET amount = :v",
            "ExpressionAttributeValues": {":v": {"N": "1E126"}},
        }},
    ]))
    assert answer == ("ValidationException", OVERFLOW)


def test_a_refused_batch_write_item_writes_nothing(dynamodb_client, table):
    """A valid put ahead of a put holding `abc`: neither is written."""
    answer = _refusal(lambda: dynamodb_client.batch_write_item(RequestItems={table: [
        {"PutRequest": {"Item": {"pk": {"S": "good"}}}},
        {"PutRequest": {"Item": {"pk": {"S": "bad"}, "n": {"N": "abc"}}}},
    ]}))
    assert answer == ("ValidationException", NOT_A_NUMBER)
    stored = dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": "good"}}, ConsistentRead=True
    )
    assert "Item" not in stored
