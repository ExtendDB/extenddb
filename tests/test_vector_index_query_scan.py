# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query and Scan naming a vector index — refusal message parity.

A vector index can never serve a Query or a Scan; the service refuses both with
an operation-specific message. Measured against Amazon DynamoDB on 2026-08-20
(whole strings, ACTIVE index):

- ``Query`` -> ``"Query operation not supported on this index type."`` (note the
  trailing period)
- ``Scan``  -> ``"Scan operation not supported on this index type"`` (no period)

A name that matches no index of any kind keeps the ordinary
``"The table does not have the specified index: <name>"`` message, so the two
cases must stay distinguishable.

Measured precedence on the same capture run:

- Query: a missing or syntactically invalid ``KeyConditionExpression`` fires
  before the vector refusal, and ``ConsistentRead=true`` fires before it too,
  reusing the GSI wording ("Consistent reads are not supported on global
  secondary indexes") even though the index is a vector index.
- Scan: the vector refusal fires before the ``ConsistentRead`` check.

Tests post raw SigV4-signed JSON because published SDK models do not carry
``VectorIndexes``. The whole module skips when the target backend does not
support vector indexes (it then cannot create the table these tests need).

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
# Real DynamoDB when no local endpoint is configured.
ENDPOINT = _ENDPOINT_VAR or f"https://dynamodb.{_REGION}.amazonaws.com/"

QUERY_REFUSAL = "Query operation not supported on this index type."
SCAN_REFUSAL = "Scan operation not supported on this index type"
CONSISTENT_READ_REFUSAL = (
    "Consistent reads are not supported on global secondary indexes"
)


def _signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` under the DynamoDB JSON-1.0 protocol, SigV4-signed.

    Credentials come from the default boto3 chain, so the same call works
    against real DynamoDB (profile or env creds) and against extenddb (the
    provisioned test keys in the environment).
    """
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    creds = boto3.Session().get_credentials()
    if creds is not None:
        aws_req = AWSRequest(method="POST", url=ENDPOINT, data=body_bytes, headers=headers)
        SigV4Auth(creds.get_frozen_credentials(), "dynamodb", _REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)
    # Real DynamoDB gets normal TLS verification; a local https extenddb
    # endpoint uses the self-signed cert from `extenddb init`.
    verify = True if not _ENDPOINT_VAR else not ENDPOINT.startswith("https://")
    return requests.post(
        ENDPOINT,
        data=body_bytes,
        headers=headers,
        verify=verify,
    )


def _error_message(resp: requests.Response) -> str:
    payload = resp.json()
    return payload.get("message", payload.get("Message", ""))


def _wait_for_vector_table_active(name: str, timeout: float = 180.0) -> None:
    """Poll until the table and its vector indexes are all ACTIVE."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = _signed_post("DescribeTable", {"TableName": name})
        if resp.status_code == 200:
            table = resp.json()["Table"]
            vector_indexes = table.get("VectorIndexes", [])
            if table["TableStatus"] == "ACTIVE" and all(
                vi.get("IndexStatus") == "ACTIVE" for vi in vector_indexes
            ):
                return
        time.sleep(2.0 if not _ENDPOINT_VAR else 0.05)
    raise TimeoutError(f"table {name} did not become ACTIVE within {timeout}s")


@pytest.fixture(scope="module")
def vector_table():
    """A table with a GSI and an ACTIVE vector index, or skip if unsupported.

    Module-scoped: vector tables are slow to create against the real service,
    and every test here is read-only against the same fixture.
    """
    name = f"extenddb-test-{uuid.uuid4().hex[:12]}"
    resp = _signed_post(
        "CreateTable",
        {
            "TableName": name,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "gidx",
                    "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            "VectorIndexes": [
                {
                    "IndexName": "vidx",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
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
        # A target may accept CreateTable while silently ignoring the unknown
        # VectorIndexes member (the wait above then passes vacuously). Without
        # the index every refusal test would fail with confusing not-found
        # diffs, so verify it exists and skip when it does not.
        describe = _signed_post("DescribeTable", {"TableName": name})
        assert describe.status_code == 200, describe.text
        index_names = [
            vi.get("IndexName")
            for vi in describe.json()["Table"].get("VectorIndexes", [])
        ]
        if "vidx" not in index_names:
            pytest.skip(
                "target accepted CreateTable but did not create the vector index"
            )
        put = _signed_post(
            "PutItem",
            {
                "TableName": name,
                "Item": {
                    "pk": {"S": "a"},
                    "gpk": {"S": "g"},
                    "emb": {
                        "L": [{"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}]
                    },
                },
            },
        )
        assert put.status_code == 200, f"PutItem failed: {put.text}"
        yield name
    finally:
        _signed_post("DeleteTable", {"TableName": name})


_KEY_CONDITION = {
    "KeyConditionExpression": "pk = :v",
    "ExpressionAttributeValues": {":v": {"S": "a"}},
}


class TestVectorIndexRefusal:
    """Query/Scan naming the ACTIVE vector index get the type refusal."""

    def test_query_on_vector_index_refused(self, vector_table):
        resp = _signed_post(
            "Query", {"TableName": vector_table, "IndexName": "vidx", **_KEY_CONDITION}
        )
        assert resp.status_code == 400, resp.text
        assert _error_message(resp) == QUERY_REFUSAL

    def test_scan_on_vector_index_refused(self, vector_table):
        resp = _signed_post("Scan", {"TableName": vector_table, "IndexName": "vidx"})
        assert resp.status_code == 400, resp.text
        assert _error_message(resp) == SCAN_REFUSAL


class TestNonexistentIndexUnchanged:
    """A genuinely absent index name keeps the index-not-found message."""

    def test_query_nonexistent_index(self, vector_table):
        resp = _signed_post(
            "Query",
            {"TableName": vector_table, "IndexName": "nosuchindex", **_KEY_CONDITION},
        )
        assert resp.status_code == 400, resp.text
        assert (
            _error_message(resp)
            == "The table does not have the specified index: nosuchindex"
        )

    def test_scan_nonexistent_index(self, vector_table):
        resp = _signed_post(
            "Scan", {"TableName": vector_table, "IndexName": "nosuchindex"}
        )
        assert resp.status_code == 400, resp.text
        assert (
            _error_message(resp)
            == "The table does not have the specified index: nosuchindex"
        )


class TestPrecedence:
    """Measured ordering between the vector refusal and neighbouring checks."""

    def test_query_consistent_read_fires_before_vector_refusal(self, vector_table):
        resp = _signed_post(
            "Query",
            {
                "TableName": vector_table,
                "IndexName": "vidx",
                "ConsistentRead": True,
                **_KEY_CONDITION,
            },
        )
        assert resp.status_code == 400, resp.text
        assert _error_message(resp) == CONSISTENT_READ_REFUSAL

    def test_scan_vector_refusal_fires_before_consistent_read(self, vector_table):
        resp = _signed_post(
            "Scan",
            {"TableName": vector_table, "IndexName": "vidx", "ConsistentRead": True},
        )
        assert resp.status_code == 400, resp.text
        assert _error_message(resp) == SCAN_REFUSAL

    def test_query_missing_key_condition_fires_before_vector_refusal(
        self, vector_table
    ):
        resp = _signed_post("Query", {"TableName": vector_table, "IndexName": "vidx"})
        assert resp.status_code == 400, resp.text
        assert _error_message(resp) == (
            "Either the KeyConditions or KeyConditionExpression parameter "
            "must be specified in the request."
        )

    def test_query_key_condition_syntax_fires_before_vector_refusal(
        self, vector_table
    ):
        resp = _signed_post(
            "Query",
            {
                "TableName": vector_table,
                "IndexName": "vidx",
                "KeyConditionExpression": "pk = = :v",
                "ExpressionAttributeValues": {":v": {"S": "a"}},
            },
        )
        assert resp.status_code == 400, resp.text
        # Only the prefix is asserted: the parse-failure suffix wording differs
        # between implementations for this expression. The point here is the
        # precedence, i.e. that a KeyConditionExpression parse error wins over
        # the vector-index refusal.
        assert _error_message(resp).startswith("Invalid KeyConditionExpression:")


class TestControlsUnaffected:
    """Base-table and GSI reads on the same table still succeed."""

    @staticmethod
    def _read_until_count(operation: str, body: dict, want: int = 1) -> requests.Response:
        """Re-issue an eventually-consistent read briefly until it sees the item.

        GSI propagation on the real service can lag the PutItem by a moment;
        these controls assert reachability, not propagation latency.
        """
        deadline = time.monotonic() + 10.0
        while True:
            resp = _signed_post(operation, body)
            if resp.status_code != 200 or resp.json().get("Count") == want:
                return resp
            if time.monotonic() >= deadline:
                return resp
            time.sleep(0.5)

    def test_query_base_table(self, vector_table):
        resp = self._read_until_count(
            "Query", {"TableName": vector_table, **_KEY_CONDITION}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["Count"] == 1

    def test_scan_base_table(self, vector_table):
        resp = self._read_until_count("Scan", {"TableName": vector_table})
        assert resp.status_code == 200, resp.text
        assert resp.json()["Count"] == 1

    def test_query_gsi(self, vector_table):
        resp = self._read_until_count(
            "Query",
            {
                "TableName": vector_table,
                "IndexName": "gidx",
                "KeyConditionExpression": "gpk = :v",
                "ExpressionAttributeValues": {":v": {"S": "g"}},
            },
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["Count"] == 1

    def test_scan_gsi(self, vector_table):
        resp = self._read_until_count(
            "Scan", {"TableName": vector_table, "IndexName": "gidx"}
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["Count"] == 1
