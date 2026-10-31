# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan request validation: an empty ``ScanFilter`` in the parameter mix, an
empty ``AttributesToGet``, a malformed number in ``ExclusiveStartKey`` and the
nesting depth of ``ExpressionAttributeValues``.

Every request below is sent as raw signed JSON. Each is a row of the table
measured against DynamoDB (us-east-1) on 2026-10-25 at 10:38 and 10:55 UTC on
a Scan of an existing table of 29 items, or of the one measured on 2026-10-30
at 12:40 UTC where a row says so, each sent twice, and each expected answer is
the service's. The table here holds 12 items, so the count the accepted
request returns is 12.

Depth is the number of ``L`` wrappers around the leaf: 32 or more answers the
nesting text, 31 reaches the unused-value check. A malformed number at the
bottom is reported first, at the depths 31 to 60 that were measured.
"""

from __future__ import annotations

import json

import pytest
from botocore.awsrequest import AWSRequest

from helpers import unique_name, wait_for_active, wait_for_deleted

ITEMS = 12

MIX = (
    "Can not use both expression and non-expression parameters in the same "
    "request: Non-expression parameters: {ScanFilter} "
    "Expression parameters: {FilterExpression}"
)
LENGTH = (
    "1 validation error detected: Value at 'AttributesToGet' failed to "
    "satisfy constraint: Member must have length greater than or equal to 1"
)
LENGTH_AND_LIMIT = (
    "2 validation errors detected: Value at 'AttributesToGet' failed to "
    "satisfy constraint: Member must have length greater than or equal to 1; "
    "Value at 'Limit' failed to satisfy constraint: Member must have value "
    "greater than or equal to 1"
)
NUMERIC_BOB = "The parameter cannot be converted to a numeric value: bob"
RCC_ENUM = (
    "1 validation error detected: Value 'BAD' at 'returnConsumedCapacity' failed "
    "to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
)
NESTING = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
UNUSED = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:d}"

VALID_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1"}}'
BOB_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "bob"}}'
ONE = {"N": "1"}
BOB = {"N": "bob"}

# (id, the fields after TableName as JSON text, the expected message)
REFUSED = [
    ("ScanFilter-empty-with-FilterExpression", '"ScanFilter": {}, ' + VALID_FILTER, MIX),
    ("ScanFilter-empty-with-unused-bob",
     '"ScanFilter": {}, "ExpressionAttributeValues": {":n": {"N": "bob"}}', NUMERIC_BOB),
    ("AttributesToGet-empty", '"AttributesToGet": []', LENGTH),
    ("AttributesToGet-empty-with-FilterExpression",
     '"AttributesToGet": [], ' + VALID_FILTER, LENGTH),
    ("AttributesToGet-empty-with-FilterExpression-and-bob",
     '"AttributesToGet": [], ' + BOB_FILTER, LENGTH),
    ("ExclusiveStartKey-bob-on-an-S-key",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}, ' + VALID_FILTER,
     NUMERIC_BOB),
    # Measured 2026-10-30 12:40 UTC.
    ("AttributesToGet-empty-with-Limit-0", '"AttributesToGet": [], "Limit": 0',
     LENGTH_AND_LIMIT),
    ("AttributesToGet-empty-with-ScanFilter-empty-and-FilterExpression",
     '"AttributesToGet": [], "ScanFilter": {}, ' + VALID_FILTER, LENGTH),
    ("ExclusiveStartKey-bob-with-no-filter",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}', NUMERIC_BOB),
    ("ExclusiveStartKey-bob-in-the-index-key-of-a-GSI-scan",
     '"IndexName": "gsi", "ExclusiveStartKey": {"gpk": {"N": "bob"}, "pk": {"S": "p"}, '
     '"sk": {"S": "x"}}', NUMERIC_BOB),
    ("ExclusiveStartKey-bob-with-ReturnConsumedCapacity-BAD",
     '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}, '
     '"ReturnConsumedCapacity": "BAD"', RCC_ENUM),
]


def nest(leaf: dict, levels: int) -> dict:
    """``leaf`` inside ``levels`` lists."""
    value = leaf
    for _ in range(levels):
        value = {"L": [value]}
    return value


def unused_value(value: dict) -> str:
    """A filter that reads ``:p`` and ``value`` under ``:d``, which nothing reads."""
    return ('"FilterExpression": "a = :p", "ExpressionAttributeValues": '
            + json.dumps({":p": {"S": "x"}, ":d": value}))


def _raw(client, operation: str, body_text: str):
    """POST ``body_text`` exactly as written, signed with the client's credentials."""
    req = AWSRequest(method="POST", url=client.meta.endpoint_url, data=body_text,
                     headers={"Content-Type": "application/x-amz-json-1.0",
                              "X-Amz-Target": f"DynamoDB_20120810.{operation}"})
    client._request_signer.sign(operation, req)
    resp = client._endpoint.http_session.send(req.prepare())
    return resp.status_code, json.loads(resp.content.decode("utf-8", "replace") or "{}")


def _scan(client, table: str, fields: str):
    return _raw(client, "Scan", '{"TableName": %s, %s}' % (json.dumps(table), fields))


def _validation_message(status: int, payload: dict) -> str:
    assert status == 400, payload
    assert payload.get("__type", "").endswith("ValidationException"), payload
    return payload.get("message", payload.get("Message"))


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """pk S, sk S, a GSI ``gsi`` on gpk S, with ITEMS items."""
    name = unique_name("scanreq")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    for i in range(ITEMS):
        dynamodb_client.put_item(TableName=name, Item={
            "pk": {"S": f"p{i}"}, "sk": {"S": "s"}, "a": {"N": str(i)}})
    yield name
    dynamodb_client.delete_table(TableName=name)
    wait_for_deleted(dynamodb_client, name)


class TestRequestChecks:
    """The parameter mix, the length constraint and the numbers in the key."""

    @pytest.mark.parametrize("fields, expected", [r[1:] for r in REFUSED],
                             ids=[r[0] for r in REFUSED])
    def test_rows(self, dynamodb_client, table, fields, expected):
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == expected

    def test_an_empty_scan_filter_alone_is_accepted(self, dynamodb_client, table):
        status, payload = _scan(dynamodb_client, table,
                                '"ScanFilter": {}, "ConsistentRead": true')
        assert status == 200, payload
        assert (payload["Count"], payload["ScannedCount"]) == (ITEMS, ITEMS)


class TestNestingDepth:
    """``ExpressionAttributeValues`` nested in lists."""

    @pytest.mark.parametrize("levels", [32, 33, 40, 60])
    def test_a_valid_number_at_the_bottom_is_refused_from_32_levels(
            self, dynamodb_client, table, levels):
        fields = unused_value(nest(ONE, levels))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NESTING

    def test_31_levels_reach_the_unused_value_check(self, dynamodb_client, table):
        fields = unused_value(nest(ONE, 31))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == UNUSED

    def test_32_levels_are_refused_ahead_of_the_select_enum(self, dynamodb_client, table):
        # Measured 2026-10-30 12:40 UTC.
        fields = unused_value(nest(ONE, 32)) + ', "Select": "BAD"'
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NESTING

    @pytest.mark.parametrize("levels", [31, 32, 33, 40, 60])
    def test_a_malformed_number_at_the_bottom_is_reported_first(
            self, dynamodb_client, table, levels):
        fields = unused_value(nest(BOB, levels))
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NUMERIC_BOB

    def test_a_map_at_32_levels_with_a_malformed_number_is_reported_first(
            self, dynamodb_client, table):
        fields = unused_value({"M": {"k": nest(BOB, 31)}})
        assert _validation_message(*_scan(dynamodb_client, table, fields)) == NUMERIC_BOB
