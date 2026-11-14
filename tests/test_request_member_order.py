# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The order of a request body's members does not change the error it gets.

Each body below has two faults that ExtendDB's typed input deserializer
catches in different members. A server that reads the members in the order
the client wrote them answers the two orders with different errors; the
MongoDB build did, because ``bson`` turns on ``serde_json``'s
``preserve_order`` feature for the whole binary. Amazon DynamoDB answered
both orders of each body with the same error (us-east-1, 2026-11-03).

The bodies are posted as written, signed with the credentials the client
signs with, because botocore refuses these values before it sends them.

Dual-target against Amazon DynamoDB and ExtendDB.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from conftest import scoped_table

P = {"S": "p0"}


@pytest.fixture(scope="module")
def hash_table(dynamodb_client):
    """Hash-only table for the module, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name


def _answer(client, endpoint_url: str | None, operation: str, body: dict[str, Any]):
    """POST ``body`` with its members in insertion order and return the HTTP
    status, the error code and the message."""
    url = endpoint_url or client.meta.endpoint_url
    payload = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    request = AWSRequest(method="POST", url=url, data=payload, headers=headers)
    creds = client._request_signer._credentials  # noqa: SLF001 - test helper
    SigV4Auth(creds.get_frozen_credentials(), "dynamodb", client.meta.region_name).add_auth(
        request
    )
    # The certificate `extenddb init` writes is self-signed; the service's is not.
    resp = requests.post(
        url, data=payload, headers=dict(request.headers), verify=endpoint_url is None, timeout=30
    )
    doc = resp.json()
    return (
        resp.status_code,
        doc.get("__type", "").rsplit("#", 1)[-1],
        doc.get("message", doc.get("Message", "")),
    )


@pytest.mark.parametrize(
    "limit, value",
    [
        pytest.param(0.0, {}, id="float_limit_empty_value"),
        pytest.param("0", {}, id="string_limit_empty_value"),
    ],
)
def test_query_limit_and_value_faults_answer_alike_in_either_order(
    dynamodb_client, endpoint_url, hash_table, limit, value
):
    values = {":p": P, ":a": value}
    head = {"TableName": hash_table, "KeyConditionExpression": "pk = :p"}
    limit_first = _answer(
        dynamodb_client,
        endpoint_url,
        "Query",
        {**head, "Limit": limit, "ExpressionAttributeValues": values},
    )
    values_first = _answer(
        dynamodb_client,
        endpoint_url,
        "Query",
        {**head, "ExpressionAttributeValues": values, "Limit": limit},
    )
    assert limit_first[0] == 400, limit_first
    assert values_first == limit_first


def test_two_invalid_values_answer_alike_in_either_order(dynamodb_client, endpoint_url, hash_table):
    head = {"TableName": hash_table, "FilterExpression": "pk IN (:p, :a, :b)"}
    b_first = _answer(
        dynamodb_client,
        endpoint_url,
        "Scan",
        {**head, "ExpressionAttributeValues": {":p": P, ":b": {}, ":a": {"X": "1"}}},
    )
    a_first = _answer(
        dynamodb_client,
        endpoint_url,
        "Scan",
        {**head, "ExpressionAttributeValues": {":p": P, ":a": {"X": "1"}, ":b": {}}},
    )
    assert b_first[0] == 400, b_first
    assert a_first == b_first
