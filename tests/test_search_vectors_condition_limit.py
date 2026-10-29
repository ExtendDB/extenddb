# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors condition count limit and duplicate-attribute message.

Measured against Amazon DynamoDB on 2026-10-29 in us-east-1, on a table whose
vector index declares a HASH ``h`` and 18 inline filters ``f1`` to ``f18``.
Each request shape was sent twice with the same response both times:

- 19 conditions, all in the search schema: 200.
- 20 or 21 conditions: ``Invalid SearchConditionExpression:
  SearchConditionExpression cannot have more than 1 partition key and more than
  18 inline filter key comparisons``, whether the extra condition repeats an
  attribute or names one outside the schema, and wherever in the expression the
  out-of-schema attribute sits.
- 19 conditions with one outside the schema: ``SearchConditionExpression must
  not contain any attributes that is not in SearchSchema. Invalid attribute: x``.
- 19 conditions with one repeated: ``Invalid SearchConditionExpression:
  SearchConditionExpression must only contain one condition per attribute``.

Tests post raw SigV4-signed JSON. Against the service, SearchVectors is sent to
the ``search-dynamodb`` host the SDKs use for it. The module skips when the
target backend does not support vector indexes.

REQ-TEST-001, REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import json
import os
import time
import uuid

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

_ENDPOINT_VAR = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ENDPOINT = _ENDPOINT_VAR or f"https://dynamodb.{_REGION}.amazonaws.com/"
SEARCH_ENDPOINT = _ENDPOINT_VAR or f"https://search-dynamodb.{_REGION}.amazonaws.com/"

TOO_MANY = (
    "Invalid SearchConditionExpression: SearchConditionExpression cannot have "
    "more than 1 partition key and more than 18 inline filter key comparisons"
)
DUPLICATE = (
    "Invalid SearchConditionExpression: SearchConditionExpression must only "
    "contain one condition per attribute"
)
NOT_IN_SCHEMA = (
    "SearchConditionExpression must not contain any attributes that is not in "
    "SearchSchema. Invalid attribute: x"
)

INLINE = [f"f{i}" for i in range(1, 19)]
NINETEEN = ["h", *INLINE]


def _signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` under the DynamoDB JSON-1.0 protocol, SigV4-signed."""
    url = SEARCH_ENDPOINT if operation == "SearchVectors" else ENDPOINT
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    creds = boto3.Session().get_credentials()
    if creds is not None:
        aws_req = AWSRequest(method="POST", url=url, data=body_bytes, headers=headers)
        SigV4Auth(creds.get_frozen_credentials(), "dynamodb", _REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)
    verify = True if not _ENDPOINT_VAR else not url.startswith("https://")
    return requests.post(url, data=body_bytes, headers=headers, verify=verify, timeout=30)


def _error_message(resp: requests.Response) -> str:
    payload = resp.json()
    return payload.get("message", payload.get("Message", ""))


def _wait_for_vector_table_active(name: str, timeout: float = 1800.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = _signed_post("DescribeTable", {"TableName": name})
        if resp.status_code == 200:
            table = resp.json()["Table"]
            vector_indexes = table.get("VectorIndexes", [])
            if (
                table["TableStatus"] == "ACTIVE"
                and vector_indexes
                and all(vi.get("IndexStatus") == "ACTIVE" for vi in vector_indexes)
            ):
                return
        time.sleep(10.0 if not _ENDPOINT_VAR else 0.05)
    raise TimeoutError(f"table {name} did not become ACTIVE within {timeout}s")


@pytest.fixture(scope="module")
def search_table():
    """A table whose vector index declares a HASH and 18 inline filters."""
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    attrs = ["pk", "h", *INLINE]
    resp = _signed_post(
        "CreateTable",
        {
            "TableName": name,
            "AttributeDefinitions": [
                {"AttributeName": a, "AttributeType": "S"} for a in attrs
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "vidx",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "SearchSchema": [
                        {"AttributeName": "h", "SearchSchemaElementType": "HASH"},
                        *(
                            {"AttributeName": a, "SearchSchemaElementType": "INLINE_FILTER"}
                            for a in INLINE
                        ),
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
    )
    if resp.status_code == 400 and "not supported" in _error_message(resp):
        pytest.skip("target backend does not support vector indexes")
    assert resp.status_code == 200, f"CreateTable failed: {resp.text}"
    try:
        _wait_for_vector_table_active(name)
        yield name
    finally:
        _signed_post("DeleteTable", {"TableName": name})


def _search(table: str, attrs: list[str]) -> requests.Response:
    return _signed_post(
        "SearchVectors",
        {
            "TableName": table,
            "IndexName": "vidx",
            "SearchVector": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}],
            "TopK": 5,
            "SearchConditionExpression": " AND ".join(
                f"{a} = :v{i}" for i, a in enumerate(attrs)
            ),
            "ExpressionAttributeValues": {
                f":v{i}": {"S": f"x{i}"} for i in range(len(attrs))
            },
        },
    )


def _refused_with(resp: requests.Response, message: str) -> None:
    assert resp.status_code == 400, resp.text
    assert resp.json()["__type"].endswith("ValidationException"), resp.text
    assert _error_message(resp) == message


class TestSearchConditionLimit:
    """Measured 2026-10-29 in us-east-1; see the module docstring."""

    def test_nineteen_conditions_are_accepted(self, search_table):
        resp = _search(search_table, NINETEEN)
        assert resp.status_code == 200, resp.text

    def test_twentieth_condition_repeating_an_attribute_gets_count_message(
        self, search_table
    ):
        _refused_with(_search(search_table, [*NINETEEN, "f1"]), TOO_MANY)

    def test_twentieth_condition_outside_schema_gets_count_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN, "x"]), TOO_MANY)

    def test_twenty_one_conditions_get_count_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN, "x", "y"]), TOO_MANY)

    def test_count_is_checked_before_an_early_out_of_schema_attribute(
        self, search_table
    ):
        _refused_with(_search(search_table, ["h", "x", *INLINE]), TOO_MANY)

    def test_out_of_schema_attribute_within_limit_gets_schema_message(
        self, search_table
    ):
        _refused_with(_search(search_table, [*NINETEEN[:-1], "x"]), NOT_IN_SCHEMA)

    def test_repeat_within_limit_gets_duplicate_message(self, search_table):
        _refused_with(_search(search_table, [*NINETEEN[:-1], "f1"]), DUPLICATE)
