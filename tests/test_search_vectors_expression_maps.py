# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""SearchVectors checks on ExpressionAttributeNames and ExpressionAttributeValues.

The service checks the two maps before it reads either expression, so these
answers do not depend on the grammar the account has. Every request here uses
conditions both grammars accept, and the module runs under either one.

Measured against the live service in eu-north-1, each request twice with
the same answer: on 2026-11-21, the 254, 255 and 256 byte keys and the
empty maps against a missing table on 2026-11-23, and a bad name ahead of a
bad key, also in us-east-1, on 2026-11-24.

The module skips when the target backend does not support vector indexes,
unless ``EXTENDDB_EXPECT_VECTORS=1``, which makes that a failure.
"""

from __future__ import annotations

import pytest
import requests

from search_vectors_wire import (
    create_vector_table,
    delete_table,
    message,
    refused,
    signed_post,
    table_name,
)

US = {":us": {"S": "US"}}
ONLY = "can only be specified when using expressions"


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


def _raw(table: str, **fields) -> requests.Response:
    body = {
        "TableName": table,
        "IndexName": "vix",
        "SearchVector": [{"N": "1"}, {"N": "0"}, {"N": "0"}, {"N": "0"}],
        "TopK": 10,
    }
    body.update(fields)
    return signed_post("SearchVectors", body)


def _accepted(resp: requests.Response) -> None:
    assert resp.status_code == 200, resp.text
    assert "SearchResults" in resp.json(), resp.text


def _too_long(map_name: str, size: int) -> str:
    return (
        f"{map_name} contains invalid key: The expression attribute map contains a key "
        f"that is too long; size of key: {size}"
    )


class TestEmptyMaps:
    def test_empty_maps_names_first(self, table):
        cond = {"SearchConditionExpression": "Country = :us"}
        refused(_raw(table, **cond, ExpressionAttributeValues={}),
                "ExpressionAttributeValues must not be empty")
        refused(
            _raw(table, **cond, ExpressionAttributeNames={}, ExpressionAttributeValues={}),
            "ExpressionAttributeNames must not be empty",
        )
        refused(_raw(table, ExpressionAttributeValues={}),
                "ExpressionAttributeValues must not be empty")
        resp = _raw(table, TopK=0, **cond, ExpressionAttributeValues={})
        assert resp.status_code == 400, resp.text
        assert "TopK" in message(resp), resp.text

    def test_an_empty_map_answers_before_a_missing_table(self, table):
        """The map checks run before the table lookup. Measured on 2026-11-23."""
        missing = table_name()
        cond = {"SearchConditionExpression": "Country = :us"}
        refused(_raw(missing, **cond, ExpressionAttributeValues={}),
                "ExpressionAttributeValues must not be empty")
        refused(_raw(missing, **cond, ExpressionAttributeNames={}, ExpressionAttributeValues=US),
                "ExpressionAttributeNames must not be empty")
        resp = _raw(missing, **cond, ExpressionAttributeValues=US)
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("ResourceNotFoundException"), resp.text
        assert message(resp) == "Requested resource not found", resp.text


class TestKeysAndNames:
    def test_keys_and_names(self, table):
        cases = [
            ({"#c": ""}, {}, "#c = :us",
             "ExpressionAttributeNames contains invalid value: Empty attribute name for key #c"),
            ({"#a-b": "x"}, {**US, ":b-k": {"S": "y"}}, "Country = :us",
             'ExpressionAttributeNames contains invalid key: Syntax error; key: "#a-b"'),
            ({"": "x"}, US, "Country = :us",
             "ExpressionAttributeNames contains invalid key: The expression attribute map "
             "contains an empty key;"),
            ({"#" + "k" * 300: "x"}, US, "Country = :us", _too_long("ExpressionAttributeNames", 301)),
            ({"#c": "C" * 65536}, US, "#c = :us",
             "Attribute name is too large, must be less than 65536 bytes for key #c"),
            (None, {**US, ":": {"S": "y"}}, "Country = :us",
             'ExpressionAttributeValues contains invalid key: Syntax error; key: ":"'),
        ]
        for names, values, cond, expected in cases:
            fields = {"SearchConditionExpression": cond, "ExpressionAttributeValues": values}
            if names is not None:
                fields["ExpressionAttributeNames"] = names
            refused(_raw(table, **fields), expected)

    def test_a_bad_name_answers_ahead_of_a_bad_key(self, table):
        """The service checks every name before any key. Measured in
        eu-north-1 and us-east-1 on 2026-11-24."""
        cases = [
            ({"n": ""},
             "ExpressionAttributeNames contains invalid value: Empty attribute name for key n"),
            ({"n": "C" * 65536},
             "Attribute name is too large, must be less than 65536 bytes for key n"),
        ]
        for names, expected in cases:
            refused(
                _raw(
                    table,
                    SearchConditionExpression="Country = :us",
                    ExpressionAttributeNames=names,
                    ExpressionAttributeValues=US,
                ),
                expected,
            )

    @pytest.mark.parametrize("size", [254, 255, 256])
    def test_a_key_is_at_most_255_bytes(self, table, size):
        name_key = "#" + "a" * (size - 1)
        names = _raw(
            table,
            SearchConditionExpression=f"{name_key} = :us",
            ExpressionAttributeNames={name_key: "Country"},
            ExpressionAttributeValues=US,
        )
        value_key = ":" + "v" * (size - 1)
        values = _raw(
            table,
            SearchConditionExpression=f"Country = {value_key}",
            ExpressionAttributeValues={value_key: {"S": "US"}},
        )
        if size <= 255:
            _accepted(names)
            _accepted(values)
        else:
            refused(names, _too_long("ExpressionAttributeNames", size))
            refused(values, _too_long("ExpressionAttributeValues", size))


class TestMapsNeedTheirExpression:
    def test_maps_need_their_expression(self, table):
        names = {"#p": "Price"}
        refused(_raw(table, ExpressionAttributeNames=names), "ExpressionAttributeNames " + ONLY)
        refused(_raw(table, ExpressionAttributeNames=names, ExpressionAttributeValues=US),
                "ExpressionAttributeNames " + ONLY)
        refused(_raw(table, ExpressionAttributeValues=US), "ExpressionAttributeValues " + ONLY)
        refused(
            _raw(table, ProjectionExpression="#p", ExpressionAttributeNames=names,
                 ExpressionAttributeValues=US),
            "ExpressionAttributeValues " + ONLY,
        )


class TestUnusedNames:
    def test_unused_names(self, table):
        unused = "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}"
        u = {"#u": "x"}
        refused(_raw(table, SearchConditionExpression="Country = :us",
                     ExpressionAttributeNames=u,
                     ExpressionAttributeValues={**US, ":w": {"S": "w"}}), unused)
        refused(_raw(table, SearchConditionExpression="Country = :us",
                     ProjectionExpression="Price", ExpressionAttributeNames=u,
                     ExpressionAttributeValues=US), unused)
        _accepted(_raw(table, SearchConditionExpression="Country = :us",
                       ProjectionExpression="#p", ExpressionAttributeNames={"#p": "Price"},
                       ExpressionAttributeValues=US))
