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

import uuid

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
    # Measured 2026-11-11: the prefix and the number message, with no
    # ExpressionAttributeValues wrapper.
    assert err["Message"] == (
        "1 validation error detected: "
        "The parameter cannot be converted to a numeric value: 12e"
    )


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


PREFIX = "1 validation error detected: "
BOB = PREFIX + "The parameter cannot be converted to a numeric value: bob"
OVERFLOW = (
    PREFIX + "Number overflow. Attempting to store a number with magnitude "
    "larger than supported range"
)


def _update_missing_table(dynamodb_client, **fields):
    """UpdateItem on a table that does not exist, answered with an error."""
    with pytest.raises(ClientError) as ei:
        dynamodb_client.update_item(
            TableName=f"absent-{uuid.uuid4().hex[:12]}",
            Key={"pk": {"S": "k"}},
            **fields,
        )
    err = ei.value.response["Error"]
    return err["Code"], err["Message"]


# Measured 2026-11-23 (us-east-1) on a table that does not exist, each request
# twice: a number in ExpressionAttributeValues that DynamoDB cannot read, nested
# ones included, is refused ahead of the parameter usage checks, the
# expressions, the undefined and unused names and values, and the table lookup.
@pytest.mark.parametrize(
    ("fields", "value", "message"),
    [
        ({"UpdateExpression": "SET a = :v"}, {"N": "bob"}, BOB),
        ({"UpdateExpression": "SET a = :v,"}, {"N": "bob"}, BOB),
        ({"UpdateExpression": "SET a = :v", "values": {":w": {"S": "x"}}}, {"N": "bob"}, BOB),
        ({"UpdateExpression": "SET #n = :v"}, {"N": "bob"}, BOB),
        ({"UpdateExpression": "SET a = :v", "ConditionExpression": "a = "}, {"N": "bob"}, BOB),
        ({"UpdateExpression": "SET a = :v, b = :x"}, {"N": "bob"}, BOB),
        (
            {
                "UpdateExpression": "SET b = :c",
                "ConditionExpression": "a = :v",
                "values": {":c": {"S": "x"}},
            },
            {"N": "bob"},
            BOB,
        ),
        (
            {"UpdateExpression": "SET a = :v", "ExpressionAttributeNames": {"#n": "b"}},
            {"N": "bob"},
            BOB,
        ),
        ({"UpdateExpression": "SET a = :v,"}, {"L": [{"N": "bob"}]}, BOB),
        ({"UpdateExpression": "SET a = :v,"}, {"NS": ["1", "bob"]}, BOB),
        ({"UpdateExpression": "SET a = :v,"}, {"M": {"x": {"N": "1e126"}}}, OVERFLOW),
        ({}, {"N": "bob"}, BOB),
        ({"ExpressionAttributeNames": {"#n": "a"}}, {"N": "bob"}, BOB),
        (
            {"AttributeUpdates": {"b": {"Action": "PUT", "Value": {"S": "x"}}}},
            {"N": "bob"},
            BOB,
        ),
    ],
    ids=[
        "alone",
        "update-syntax",
        "unused-value",
        "undefined-name",
        "condition-syntax",
        "undefined-value",
        "condition-only-value",
        "unused-name",
        "in-a-list",
        "in-a-number-set",
        "in-a-map",
        "no-expression",
        "names-and-no-expression",
        "attribute-updates-and-no-expression",
    ],
)
def test_a_value_number_is_refused_ahead_of_the_expressions_and_the_table(
    dynamodb_client, fields, value, message
):
    fields = dict(fields)
    values = {":v": value, **fields.pop("values", {})}
    assert _update_missing_table(
        dynamodb_client, ExpressionAttributeValues=values, **fields
    ) == ("ValidationException", message)


# Measured the same way: the mixed parameters check and an empty
# ExpressionAttributeNames map come ahead of the number. Only the check is
# asserted here, since the prefix of these texts is a separate difference.
@pytest.mark.parametrize(
    ("fields", "text"),
    [
        (
            {"AttributeUpdates": {"b": {"Action": "PUT", "Value": {"S": "x"}}}},
            "Can not use both expression and non-expression parameters",
        ),
        ({"ExpressionAttributeNames": {}}, "ExpressionAttributeNames must not be empty"),
    ],
    ids=["mixed-parameters", "empty-names"],
)
def test_mixing_and_an_empty_names_map_come_ahead_of_a_value_number(
    dynamodb_client, fields, text
):
    code, message = _update_missing_table(
        dynamodb_client,
        UpdateExpression="SET a = :v",
        ExpressionAttributeValues={":v": {"N": "bob"}},
        **fields,
    )
    assert code == "ValidationException"
    assert text in message
