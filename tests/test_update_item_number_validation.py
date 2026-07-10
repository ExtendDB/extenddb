# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateItem number-value validation.

Malformed, out-of-range, and invalid number-set values supplied via an
UpdateExpression (ExpressionAttributeValues) or the legacy AttributeUpdates
map must be rejected with a ValidationException, matching real DynamoDB.
Values referenced only from an UpdateExpression were previously stored
without number validation.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


@pytest.fixture()
def table_with_item(dynamodb_client, create_and_cleanup_table):
    result = create_and_cleanup_table()
    name = result["TableDescription"]["TableName"]
    dynamodb_client.put_item(
        TableName=name, Item={"pk": {"S": "row1"}, "n": {"N": "5"}}
    )
    return name


def _update_set(dynamodb_client, table, value):
    dynamodb_client.update_item(
        TableName=table,
        Key={"pk": {"S": "row1"}},
        UpdateExpression="SET bad = :v",
        ExpressionAttributeValues={":v": value},
    )


def test_update_item_rejects_malformed_number_in_expression(
    dynamodb_client, table_with_item
):
    with pytest.raises(ClientError) as ei:
        _update_set(dynamodb_client, table_with_item, {"N": "12e"})
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "ExpressionAttributeValues contains invalid value" in err["Message"]
    assert "cannot be converted to a numeric value: 12e" in err["Message"]


def test_update_item_rejects_number_overflow_in_expression(
    dynamodb_client, table_with_item
):
    with pytest.raises(ClientError) as ei:
        _update_set(dynamodb_client, table_with_item, {"N": "1" + "0" * 200})
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "Number overflow" in err["Message"]


def test_update_item_rejects_invalid_number_set_member_in_expression(
    dynamodb_client, table_with_item
):
    with pytest.raises(ClientError) as ei:
        _update_set(dynamodb_client, table_with_item, {"NS": ["1", "abc"]})
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "cannot be converted to a numeric value: abc" in err["Message"]


def test_update_item_rejects_bad_number_in_attribute_updates(
    dynamodb_client, table_with_item
):
    with pytest.raises(ClientError) as ei:
        dynamodb_client.update_item(
            TableName=table_with_item,
            Key={"pk": {"S": "row1"}},
            AttributeUpdates={
                "bad": {"Value": {"N": "not_a_num"}, "Action": "PUT"}
            },
        )
    err = ei.value.response["Error"]
    assert err["Code"] == "ValidationException"
    # Legacy AttributeUpdates path: bare numeric-value message, no
    # ExpressionAttributeValues wrapper.
    assert "cannot be converted to a numeric value: not_a_num" in err["Message"]


def test_update_item_accepts_valid_number_in_expression(
    dynamodb_client, table_with_item
):
    dynamodb_client.update_item(
        TableName=table_with_item,
        Key={"pk": {"S": "row1"}},
        UpdateExpression="SET good = :v",
        ExpressionAttributeValues={":v": {"N": "42"}},
    )
    resp = dynamodb_client.get_item(
        TableName=table_with_item,
        Key={"pk": {"S": "row1"}},
        ConsistentRead=True,
    )
    assert resp["Item"]["good"] == {"N": "42"}
