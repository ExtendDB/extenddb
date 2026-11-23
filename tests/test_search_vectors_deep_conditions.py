# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors with deeply nested search conditions, up to the 4096-byte limit.

Under the extended grammar the service answers each condition below with a
ValidationException, whatever its depth. A run of 4080 open parentheses gets
the same syntax error as a run of one. The server must go on answering: after
each request the test sends a valid search, and against ExtendDB it also
reads ``/health``.

Measured against the live service in eu-north-1, on an account with the
extended grammar, each request twice with the same answer, on 2026-11-23 on
this table and on a missing table; the 1500 and 4080 open parentheses also on
2026-11-22. Under the equality-only grammar the test asserts only that each
condition is refused and that the server keeps answering.

The module skips when the target backend does not support vector indexes,
unless ``EXTENDDB_EXPECT_VECTORS=1``, which makes that a failure.
"""

from __future__ import annotations

import pytest
import requests

from search_vectors_wire import (
    ENDPOINT_VAR,
    create_vector_table,
    delete_table,
    grammar_of,
    refused,
    signed_post,
    table_name,
)

P = "Invalid SearchConditionExpression: "
UNCLOSED = P + 'Syntax error; token: "<EOF>", near: ":v"'
VALUES = {":v": {"S": "x"}}

# (label, condition, the extended grammar's answer)
DEEP = [
    ("1500 open parentheses", "(" * 1500 + "a = :v", UNCLOSED),
    ("4080 open parentheses", "(" * 4080 + "a = :v", UNCLOSED),
    ("818 unclosed NOT (", "NOT (" * 818 + "a = :v", UNCLOSED),
    (
        "818 closed NOT(",
        "NOT(" * 818 + "a=:v" + ")" * 818,
        P + "The expression contains too many operators; operator count: 819",
    ),
]


@pytest.fixture(scope="module")
def table():
    """HASH Country (S) and inline filter Price (N), with one item."""
    name = table_name()
    create_vector_table(
        name,
        {
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "Country", "AttributeType": "S"},
                {"AttributeName": "Price", "AttributeType": "N"},
            ],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "vix",
                    "Dimensions": 4,
                    "DistanceFunction": "COSINE",
                    "VectorAttribute": {"AttributeName": "emb"},
                    "SearchSchema": [
                        {"AttributeName": "Country", "SearchSchemaElementType": "HASH"},
                        {"AttributeName": "Price", "SearchSchemaElementType": "INLINE_FILTER"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
    )
    try:
        put = signed_post(
            "PutItem",
            {
                "TableName": name,
                "Item": {
                    "id": {"S": "i1"},
                    "Country": {"S": "US"},
                    "Price": {"N": "5"},
                    "emb": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}, {"N": "0"}]},
                },
            },
        )
        assert put.status_code == 200, put.text
        yield name
    finally:
        delete_table(name)


def _search(table: str, condition: str, values: dict) -> requests.Response:
    return signed_post(
        "SearchVectors",
        {
            "TableName": table,
            "IndexName": "vix",
            "SearchVector": [{"N": "1"}, {"N": "0"}, {"N": "0"}, {"N": "0"}],
            "TopK": 10,
            "SearchConditionExpression": condition,
            "ExpressionAttributeValues": values,
        },
    )


@pytest.fixture(scope="module")
def grammar(table) -> str:
    return grammar_of(lambda: _search(table, "Country = :us AND Price > :p",
                                      {":us": {"S": "US"}, ":p": {"N": "0"}}))


def _still_answering(table: str, after: str) -> None:
    try:
        resp = _search(table, "Country = :us", {":us": {"S": "US"}})
        if ENDPOINT_VAR:
            health = requests.get(f"{ENDPOINT_VAR.rstrip('/')}/health", timeout=10, verify=False)
            assert health.status_code == 200, f"/health after {after}: {health.status_code}"
    except requests.exceptions.ConnectionError as e:
        pytest.fail(f"the target stopped answering after {after}: {e}")
    assert resp.status_code == 200, resp.text
    assert [r["Item"]["id"]["S"] for r in resp.json()["SearchResults"]] == ["i1"]


@pytest.mark.parametrize(("label", "condition", "extended"), DEEP, ids=[d[0] for d in DEEP])
def test_a_deep_condition_is_refused_and_the_server_answers(
    table, grammar, label, condition, extended
):
    assert len(condition.encode()) <= 4096
    try:
        resp = _search(table, condition, VALUES)
    except requests.exceptions.ConnectionError as e:
        pytest.fail(f"the target dropped the connection on {label}: {e}")
    if grammar == "extended":
        refused(resp, extended)
    else:
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("ValidationException"), resp.text
    _still_answering(table, label)
