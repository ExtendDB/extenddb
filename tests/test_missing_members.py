# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Requests without their TableName, Key or Item, with one of them as null, or
with a member of the wrong JSON type.

Amazon DynamoDB refuses a missing or null member with ``ValidationException``.
The message holds ``Value null at '<path>' failed to satisfy constraint:
Member must not be null`` for each such member, in an order of the
operation's own. A member of the wrong JSON type is answered first, with
``SerializationException`` in the words of the service's JSON reader, so only
its code is compared. CreateTable gives a missing or null name a text of its
own. Measured in us-east-1 on 17, 18 and 20 Nov 2026. Each request went out
twice and got the same answer both times. No request names a table that
exists.

boto3 drops a None member before it sends, so each request is a signed raw
body.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest

from raw_signed_request import signed_post

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
# The name of a table that does not exist.
T = f"extenddb-missing-{uuid.uuid4().hex[:12]}"
KEY = {"pk": {"S": "a"}}
QUERY = {
    "KeyConditionExpression": "pk = :v",
    "ExpressionAttributeValues": {":v": {"S": "a"}},
}
CREATE = {
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "BillingMode": "PAY_PER_REQUEST",
}
# The text CreateTable gives a missing name.
REQUIRED = "The parameter 'TableName' is required but was not present in the request"
# An answer of a type other than SerializationException: the body passes the read.
READ = "read"

# (operation, body, paths): the paths of the null clauses, None for
# SerializationException, or REQUIRED or READ.
ROWS = [
    ("CreateTable", CREATE, REQUIRED),
    ("CreateTable", {"TableName": None, **CREATE}, REQUIRED),
    ("CreateTable", {"TableName": None, **CREATE, "SSESpecification": 5}, None),
    ("CreateTable", {"TableName": None, **CREATE, "ResourcePolicy": 5}, None),
    ("CreateTable", {"TableName": None, **CREATE, "ResourcePolicy": "x"}, REQUIRED),
    ("CreateTable", {"TableName": None, **CREATE, "SSESpecification": {"Enabled": True}}, REQUIRED),
    ("UpdateTable", {"BillingMode": "PAY_PER_REQUEST"}, ["tableName"]),
    ("UpdateTable", {"TableName": None, "BillingMode": "PAY_PER_REQUEST"}, ["tableName"]),
    ("UpdateTable", {"BillingMode": "BOGUS"}, ["tableName"]),
    ("UpdateTable", {"TableName": None, "BillingMode": "BOGUS"}, ["tableName"]),
    ("UpdateTable", {"BillingMode": 5}, None),
    ("UpdateTable", {"SSESpecification": 5}, None),
    ("UpdateTable", {"ReplicaUpdates": 5}, None),
    ("UpdateTable", {"TableName": T, "SSESpecification": 5}, None),
    ("UpdateTable", {"TableName": T, "ReplicaUpdates": 5}, None),
    ("UpdateTable", {"TableName": T, "MultiRegionConsistency": 5}, None),
    ("UpdateTable", {"TableName": T, "GlobalTableWitnessUpdates": 5}, None),
    ("UpdateTable", {"MultiRegionConsistency": 5}, None),
    ("UpdateTable", {"GlobalTableWitnessUpdates": 5}, None),
    ("UpdateTable", {"SSESpecification": "x"}, None),
    ("UpdateTable", {"SSESpecification": []}, None),
    ("UpdateTable", {"ReplicaUpdates": [5]}, None),
    ("UpdateTable", {"ReplicaUpdates": {}}, None),
    ("UpdateTable", {"TableName": T, "SSESpecification": {}}, READ),
    ("UpdateTable", {"SSESpecification": {}}, ["tableName"]),
    ("DescribeTable", {}, ["tableName"]),
    ("DescribeTable", {"TableName": None}, ["tableName"]),
    ("DeleteTable", {}, ["tableName"]),
    ("DeleteTable", {"TableName": None}, ["tableName"]),
    ("Query", QUERY, ["tableName"]),
    ("Query", {"TableName": None, **QUERY}, ["tableName"]),
    ("Scan", {}, ["tableName"]),
    ("Scan", {"TableName": None}, ["tableName"]),
    ("Scan", {"Limit": "x"}, None),
    ("Scan", {"TableName": None, "Limit": "x"}, None),
    ("GetItem", {}, ["tableName", "key"]),
    ("GetItem", {"Key": KEY}, ["tableName"]),
    ("GetItem", {"TableName": None, "Key": KEY}, ["tableName"]),
    ("GetItem", {"TableName": None, "Key": None}, ["tableName", "key"]),
    ("GetItem", {"TableName": T}, ["key"]),
    ("GetItem", {"TableName": T, "Key": None}, ["key"]),
    ("GetItem", {"TableName": T, "ProjectionExpression": "a", "AttributesToGet": ["a"]}, ["key"]),
    ("PutItem", {}, ["item", "tableName"]),
    ("PutItem", {"Item": KEY}, ["tableName"]),
    ("PutItem", {"TableName": None, "Item": KEY}, ["tableName"]),
    ("PutItem", {"TableName": T}, ["item"]),
    ("PutItem", {"TableName": T, "Item": None}, ["item"]),
    ("PutItem", {"TableName": T, "ReturnValues": 5}, None),
    ("PutItem", {"ReturnValues": 5}, None),
    ("PutItem", {"Item": KEY, "ReturnValues": 5}, None),
    ("DeleteItem", {}, ["key", "tableName"]),
    ("DeleteItem", {"TableName": T}, ["key"]),
    ("DeleteItem", {"TableName": T, "Key": None}, ["key"]),
    ("UpdateItem", {}, ["tableName", "key"]),
    ("UpdateItem", {"TableName": T}, ["key"]),
    ("UpdateItem", {"TableName": T, "Key": None}, ["key"]),
    ("UpdateItem", {"TableName": T, "UpdateExpression": "SET a = :x", "ExpressionAttributeValues": {":x": {"S": "x"}}}, ["key"]),
]


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


@pytest.mark.parametrize("operation,body,paths", ROWS)
def test_refusal(post, operation, body, paths):
    resp = post(operation, body)
    data = resp.json()
    code = str(data.get("__type", "")).rsplit("#", 1)[-1]
    message = data.get("message", data.get("Message"))
    assert resp.status_code == 400, data
    if paths is None:
        assert code == "SerializationException", data
    elif paths == READ:
        assert code != "SerializationException", data
    elif paths == REQUIRED:
        assert (code, message) == ("ValidationException", REQUIRED)
    else:
        clauses = [f"Value null at '{p}' failed to satisfy constraint: Member must not be null" for p in paths]
        s = "" if len(paths) == 1 else "s"
        assert (code, message) == (
            "ValidationException",
            f"{len(paths)} validation error{s} detected: {'; '.join(clauses)}",
        )
