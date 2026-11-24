# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Texts, request helpers and table fixtures the Scan request validation
tests share: test_scan_request_validation.py and test_scan_request_rows.py."""

from __future__ import annotations

import contextlib
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
LIMIT = (
    "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)
SEGMENT = (
    "1 validation error detected: Value at 'Segment' failed to satisfy constraint: "
    "Member must have value greater than or equal to 0"
)
SELECT_ENUM = (
    "1 validation error detected: Value 'BAD' at 'select' failed to satisfy constraint: "
    "Member must satisfy enum value set: "
    "[SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
)
SELECT_SPECIFIC = (
    "Must specify the AttributesToGet or ProjectionExpression when choosing to get "
    "SPECIFIC_ATTRIBUTES"
)
UNUSED_U = "Value provided in ExpressionAttributeValues unused in expressions: keys: {:u}"
KEY_PREFIX = "The provided starting key is invalid: "
EMPTY_KEY_VALUE = (
    KEY_PREFIX + "Supplied AttributeValue is empty, must contain exactly one of the "
    "supported datatypes"
)
TWO_DESCRIPTORS = (
    KEY_PREFIX + "Supplied AttributeValue has more than one datatypes set, must contain "
    "exactly one of the supported datatypes"
)
INVALID = "One or more parameter values were invalid: "
NS_EMPTY = INVALID + "An number set  may not be empty"
SS_EMPTY = INVALID + "An string set  may not be empty"
BS_EMPTY = INVALID + "Binary sets should not be empty"
NS_DUP = INVALID + "Input collection contains duplicates."
SS_DUP = INVALID + 'Input collection ["a", "a"] contains duplicates.'
NUMERIC_FRED = NUMERIC_BOB.replace("bob", "fred")
EAV_NO_EXPRESSION = "ExpressionAttributeValues can only be specified when using expressions"
MISSING_INDEX = "The table does not have the specified index: nope"

VALID_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "1"}}'
BOB_FILTER = '"FilterExpression": "a = :n", "ExpressionAttributeValues": {":n": {"N": "bob"}}'
ONE = {"N": "1"}
BOB = {"N": "bob"}
BOB_KEY = '"ExclusiveStartKey": {"pk": {"N": "bob"}, "sk": {"S": "x"}}'

def nest(leaf: dict, levels: int) -> dict:
    """``leaf`` inside ``levels`` lists."""
    value = leaf
    for _ in range(levels):
        value = {"L": [value]}
    return value


EAV_VALUE = "ExpressionAttributeValues contains invalid value: "
EAV_EMPTY_E = (EAV_VALUE + "Supplied AttributeValue is empty, must contain exactly one of the "
               "supported datatypes for key :e")
EAV_EMPTY_A = EAV_EMPTY_E.replace(":e", ":a")
EAV_TWO_A = (EAV_VALUE + "Supplied AttributeValue has more than one datatypes set, must contain "
             "exactly one of the supported datatypes for key :a")
EAV_NESTING_D = EAV_VALUE + NESTING + " for key :d"
EAN_NO_EXPRESSION = "ExpressionAttributeNames can only be specified when using expressions"
CONDOP = "ConditionalOperator can only be used when Filter or Expected has two or more elements"
UNDEFINED_ZZ = ("Invalid FilterExpression: An expression attribute value used in expression "
                "is not defined; attribute value: :zz")
BEGINS_N = ("Invalid FilterExpression: Incorrect operand type for operator or function; "
            "operator or function: begins_with, operand type: N")
SEGMENT_ALONE = ("The TotalSegments parameter is required but was not present in the request "
                 "when Segment parameter is present")
EMPTY_KEY = '"ExclusiveStartKey": {"pk": {}, "sk": {"S": "x"}}'
FILTER_P = '"FilterExpression": "a = :p"'
NEST33_S = json.dumps(nest({"S": "x"}, 33))
NEST33_N = json.dumps(nest({"N": "1"}, 33))

EMPTY_VALUE_BARE = ("Supplied AttributeValue is empty, must contain exactly one of the "
                    "supported datatypes")
MORE_THAN_ONE_BARE = ("Supplied AttributeValue has more than one datatypes set, must contain "
                      "exactly one of the supported datatypes")
EAV_EMPTY_B = EAV_EMPTY_E.replace(":e", ":b")
EAV_EMPTY_P = EAV_EMPTY_E.replace(":e", ":p")
EAV_NUMBER_A = EAV_VALUE + NUMERIC_BOB + " for key :a"
EAV_NUMBER_N = EAV_VALUE + NUMERIC_BOB + " for key :n"
SF_EMPTY = '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{}]}}'
SF_BOB = '"ScanFilter": {"a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "bob"}]}}'
NONASCII_KEY_A_BOB = '"ExclusiveStartKey": {"\\u00e4": {"N": "bob"}, "\\u00e9": {}}'
NONASCII_KEY_E_EMPTY = '"ExclusiveStartKey": {"\\u00e9": {}, "\\u00e4": {"N": "bob"}}'

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


@contextlib.contextmanager
def _created_table(client, prefix: str, seed=(), **params):
    """Create a table named from ``prefix`` with ``params``, wait for it, put
    the ``seed`` items, and delete it on exit, also when the wait or a put
    raises."""
    name = unique_name(prefix)
    client.create_table(TableName=name, BillingMode="PAY_PER_REQUEST", **params)
    try:
        wait_for_active(client, name)
        for item in seed:
            client.put_item(TableName=name, Item=item)
        yield name
    finally:
        client.delete_table(TableName=name)
        wait_for_deleted(client, name)


def _keys(*names: str) -> dict:
    """The definitions and the key schema of an S hash key and an optional S
    range key."""
    return {
        "AttributeDefinitions": [{"AttributeName": n, "AttributeType": "S"} for n in names],
        "KeySchema": [{"AttributeName": n, "KeyType": t} for n, t in zip(names, ("HASH", "RANGE"))],
    }


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """pk S, sk S, a GSI ``gsi`` on gpk S, an LSI ``lsi`` on ln N, with ITEMS items."""
    with _created_table(
        dynamodb_client, "scanreq",
        seed=[{"pk": {"S": f"p{i}"}, "sk": {"S": "s"}, "a": {"N": str(i)}} for i in range(ITEMS)],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
            {"AttributeName": "ln", "AttributeType": "N"},
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
        LocalSecondaryIndexes=[{
            "IndexName": "lsi",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "ln", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def n_table(dynamodb_client):
    """pk N, with items 1 to 6."""
    with _created_table(
        dynamodb_client, "scanreqn",
        seed=[{"pk": {"N": str(i)}} for i in range(1, 7)],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "N"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def ab_table(dynamodb_client):
    """HASH a S, RANGE b S, no items."""
    with _created_table(dynamodb_client, "scanreqab", **_keys("a", "b")) as name:
        yield name


@pytest.fixture(scope="module")
def pa_table(dynamodb_client):
    """HASH pk S, RANGE a S, no items."""
    with _created_table(dynamodb_client, "scanreqpa", **_keys("pk", "a")) as name:
        yield name


@pytest.fixture(scope="module")
def it_table(dynamodb_client):
    """HASH id S, RANGE ts S, no items."""
    with _created_table(dynamodb_client, "scanreqit", **_keys("id", "ts")) as name:
        yield name


@pytest.fixture(scope="module")
def t4_table(dynamodb_client):
    """pk S, sk S, with a GSI ``gsi2`` on gpk S HASH and gsk S RANGE: a four-member index key."""
    with _created_table(
        dynamodb_client, "scanreqt4",
        AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"}
                              for n in ("pk", "sk", "gpk", "gsk")],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi2",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"},
                          {"AttributeName": "gsk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def u_table(dynamodb_client):
    """HASH \u00e4 S, RANGE \u00e9 S: member names outside ASCII."""
    with _created_table(dynamodb_client, "scanrequ", **_keys("\u00e4", "\u00e9")) as name:
        yield name


@pytest.fixture(scope="module")
def v_table(dynamodb_client):
    """HASH \U0001d11e S, RANGE a S: a member name outside the Basic Multilingual Plane."""
    with _created_table(dynamodb_client, "scanreqv", **_keys("\U0001d11e", "a")) as name:
        yield name
