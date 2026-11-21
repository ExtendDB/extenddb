# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A JSON null in a member Amazon DynamoDB reads as absent or refuses.

DynamoDB reads ``"ReturnConsumedCapacity": null`` as a request without the
member: it serves the request and returns no ``ConsumedCapacity``. It refuses
a null element of ``AttributesToGet`` with ``Empty attribute name`` and a null
item attribute with ``The supplied Item contains a null AttributeValue:
<name> is null``, both as ``ValidationException``. Measured in us-east-1 on
14 Nov 2026, and on 17 Nov 2026 for a null attribute that is not the key.

boto3 refuses a null before it sends, so each request is a signed raw body.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import os

import boto3
import pytest

from conftest import scoped_table
from raw_signed_request import signed_post

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
NULL_ITEM = "The supplied Item contains a null AttributeValue"
EMPTY_VALUE = (
    "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
)
K1 = {"pk": {"S": "k1"}}


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on pk (S) holding the item k1, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item=K1)
        yield name


@pytest.fixture(scope="module")
def post(endpoint_url):
    """Send a body as written, signed with the caller's credentials."""
    endpoint = endpoint_url or f"https://dynamodb.{REGION}.amazonaws.com"
    creds = boto3.Session().get_credentials().get_frozen_credentials()

    def _post(operation: str, body: dict):
        return signed_post(
            endpoint,
            creds.access_key,
            creds.secret_key,
            REGION,
            operation,
            body,
            session_token=creds.token,
        )

    return _post


def _select(t: str) -> str:
    return f"SELECT * FROM \"{t}\" WHERE pk = 'k1'"


# Requests served with no ConsumedCapacity when they carry
# "ReturnConsumedCapacity": null: (operation, body for the table t).
SERVED = [
    ("PutItem", lambda t: {"TableName": t, "Item": K1}),
    ("GetItem", lambda t: {"TableName": t, "Key": K1}),
    ("DeleteItem", lambda t: {"TableName": t, "Key": {"pk": {"S": "k2"}}}),
    ("UpdateItem", lambda t: {"TableName": t, "Key": K1}),
    (
        "Query",
        lambda t: {
            "TableName": t,
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "k1"}},
        },
    ),
    ("Scan", lambda t: {"TableName": t}),
    ("BatchGetItem", lambda t: {"RequestItems": {t: {"Keys": [K1]}}}),
    ("BatchWriteItem", lambda t: {"RequestItems": {t: [{"PutRequest": {"Item": K1}}]}}),
    ("TransactGetItems", lambda t: {"TransactItems": [{"Get": {"TableName": t, "Key": K1}}]}),
    ("TransactWriteItems", lambda t: {"TransactItems": [{"Put": {"TableName": t, "Item": K1}}]}),
    ("ExecuteStatement", lambda t: {"Statement": _select(t)}),
    ("BatchExecuteStatement", lambda t: {"Statements": [{"Statement": _select(t)}]}),
    ("ExecuteTransaction", lambda t: {"TransactStatements": [{"Statement": _select(t)}]}),
]


def _null_capacity(body):
    return lambda t: {**body(t), "ReturnConsumedCapacity": None}


# Items with a value of the wrong JSON type beside a null or empty value:
# (id, the attributes after pk, in the order written). The service refuses
# the wrong type first, as SerializationException (measured 17 Nov 2026).
# The service words that refusal as its JSON reader does, which ExtendDB
# does not, so only its code is compared.
WRONG_JSON_TYPE = [
    ("null-attribute-before-non-map-attribute", {"a": None, "b": "x"}),
    ("null-attribute-before-number-S-payload", {"a": None, "b": {"S": 1}}),
    ("non-map-attribute-before-null-attribute", {"b": "x", "a": None}),
    ("number-S-payload-before-null-attribute", {"a": {"S": 1}, "b": None}),
    ("empty-attribute-before-number-S-payload", {"a": {}, "b": {"S": 1}}),
    ("empty-attribute-before-non-map-attribute", {"a": {}, "b": "x"}),
]

# (id, operation, body for the table t, expected): expected None is a request
# served with no ConsumedCapacity, else the refusal's type and message. When
# the message is None, only the type is compared.
ROWS = [
    (f"{op}-null-ReturnConsumedCapacity", op, _null_capacity(body), None)
    for op, body in SERVED
] + [
    (
        "Scan-null-AttributesToGet-element",
        "Scan",
        lambda t: {"TableName": t, "AttributesToGet": [None]},
        ("ValidationException", "Empty attribute name"),
    ),
    (
        "PutItem-null-key-attribute",
        "PutItem",
        lambda t: {"TableName": t, "Item": {"pk": None}},
        ("ValidationException", f"{NULL_ITEM}: pk is null"),
    ),
    (
        "PutItem-null-attribute-before-empty-attribute",
        "PutItem",
        lambda t: {"TableName": t, "Item": {"pk": {"S": "k1"}, "a": None, "b": {}}},
        ("ValidationException", f"{NULL_ITEM}: a is null"),
    ),
    (
        "PutItem-empty-attribute-before-null-attribute",
        "PutItem",
        lambda t: {"TableName": t, "Item": {"pk": {"S": "k1"}, "a": {}, "b": None}},
        ("ValidationException", EMPTY_VALUE),
    ),
] + [
    (
        f"PutItem-{name}",
        "PutItem",
        lambda t, attributes=attributes: {"TableName": t, "Item": {**K1, **attributes}},
        ("SerializationException", None),
    )
    for name, attributes in WRONG_JSON_TYPE
]


@pytest.mark.parametrize(
    "operation,body,expected",
    [row[1:] for row in ROWS],
    ids=[row[0] for row in ROWS],
)
def test_a_json_null_member(post, table, operation, body, expected):
    resp = post(operation, body(table))
    if expected is None:
        assert resp.status_code == 200, resp.text
        assert "ConsumedCapacity" not in resp.json(), resp.text
        return
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    assert payload["__type"].rsplit("#", 1)[-1] == expected[0], resp.text
    if expected[1] is not None:
        assert payload.get("message", payload.get("Message")) == expected[1], resp.text
