# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Where the Query key size refusal sits among the other Query refusals.

Each test sends a 2049-byte partition key value together with one other
fault and asserts which refusal comes back. Measured on the service in
us-east-1 on 2026-10-23: a missing table, a wrong
IndexName, ConsistentRead on a GSI, Limit 0, a key condition that misses the
key schema, a projection syntax error, Select against a projection, an
undefined filter name, a filter operand type, Select ALL_PROJECTED_ATTRIBUTES
on a base table, overlapping projection paths, an unused expression name, a
FilterExpression syntax error and an unused expression value are all reported
ahead of the size; an invalid ExclusiveStartKey is reported after it. On a
KEYS_ONLY GSI, Select ALL_ATTRIBUTES is reported after the size, with and
without an ExclusiveStartKey (measured 2026-11-20).

Where ExtendDB words a syntax error differently from the service, the test
checks the label that precedes the wording and that the size refusal did not
come back.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name
from query_key_size_support import (  # noqa: F401  (fixtures are used by name)
    HASH_MESSAGE,
    S,
    index_table,
    refused,
    refused_ending,
)

BIG = S(2049, "p")


def _error(client, **query) -> dict:
    with pytest.raises(ClientError) as exc_info:
        client.query(**query)
    err = exc_info.value.response
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    return err["Error"]


def _first(client, table: str, message: str, **query) -> None:
    """The query on ``table`` with a 2049-byte ``pk`` reports ``message``."""
    err = _error(
        client,
        TableName=table,
        KeyConditionExpression="pk = :big",
        **{
            **query,
            "ExpressionAttributeValues": {":big": BIG, **query.get("ExpressionAttributeValues", {})},
        },
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"] == message


def test_missing_table_is_reported_first(dynamodb_client):
    err = _error(
        dynamodb_client,
        TableName=unique_name("qks-absent"),
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": BIG},
    )
    assert err["Code"] == "ResourceNotFoundException"
    assert err["Message"] == "Requested resource not found"


def test_wrong_index_name_is_reported_first(dynamodb_client, index_table):
    err = _error(
        dynamodb_client,
        TableName=index_table,
        IndexName="nope",
        KeyConditionExpression="g = :p",
        ExpressionAttributeValues={":p": BIG},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"] == "The table does not have the specified index: nope"


def test_consistent_read_on_a_gsi_is_reported_first(dynamodb_client, index_table):
    err = _error(
        dynamodb_client,
        TableName=index_table,
        IndexName="gsi",
        ConsistentRead=True,
        KeyConditionExpression="g = :p",
        ExpressionAttributeValues={":p": BIG},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"] == "Consistent reads are not supported on global secondary indexes"


def test_limit_zero_is_reported_first(raw_client, index_table):
    err = _error(
        raw_client,
        TableName=index_table,
        Limit=0,
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": BIG},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"] == (
        "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
        "Member must have value greater than or equal to 1"
    )


def test_missed_key_schema_element_is_reported_first(dynamodb_client, index_table):
    err = _error(
        dynamodb_client,
        TableName=index_table,
        IndexName="gsi",
        KeyConditionExpression="gs = :v",
        ExpressionAttributeValues={":v": S(1025, "r")},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"] == "Query condition missed key schema element: g"


def test_projection_syntax_error_is_reported_first(dynamodb_client, index_table):
    err = _error(
        dynamodb_client,
        TableName=index_table,
        KeyConditionExpression="pk = :big",
        ProjectionExpression="a,,b",
        ExpressionAttributeValues={":big": BIG},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"].startswith("Invalid ProjectionExpression: ")
    assert err["Message"] != HASH_MESSAGE


def test_select_count_with_a_projection_is_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "1 validation error detected: Cannot specify the ProjectionExpression "
        "when choosing to get only the Count",
        Select="COUNT",
        ProjectionExpression="sk",
    )


def test_undefined_filter_name_is_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "Invalid FilterExpression: An expression attribute name used in the "
        "document path is not defined; attribute name: #z",
        FilterExpression="#z = :big",
    )


def test_filter_operand_type_is_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "Invalid FilterExpression: Incorrect operand type for operator or function; "
        "operator or function: begins_with, operand type: N",
        FilterExpression="begins_with(a, :n)",
        ExpressionAttributeValues={":n": {"N": "1"}},
    )


def test_select_all_projected_attributes_on_a_base_table_is_reported_first(
    dynamodb_client, index_table
):
    # The service prefixes this with `1 validation error detected: `; ExtendDB
    # omits that label (a wording gap that exists without the size check).
    refused_ending(
        dynamodb_client,
        "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName",
        TableName=index_table,
        KeyConditionExpression="pk = :big",
        Select="ALL_PROJECTED_ATTRIBUTES",
        ExpressionAttributeValues={":big": BIG},
    )


def test_overlapping_projection_paths_are_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "Invalid ProjectionExpression: Two document paths overlap with each other; "
        "must remove or rewrite one of these paths; path one: [a], path two: [a, b]",
        ProjectionExpression="a, a.b",
    )


def test_unused_expression_name_is_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}",
        ExpressionAttributeNames={"#u": "a"},
    )


def test_filter_expression_syntax_is_reported_first(dynamodb_client, index_table):
    err = _error(
        dynamodb_client,
        TableName=index_table,
        KeyConditionExpression="pk = :p",
        FilterExpression="nope(",
        ExpressionAttributeValues={":p": BIG},
    )
    assert err["Code"] == "ValidationException"
    assert err["Message"].startswith("Invalid FilterExpression: ")
    assert err["Message"] != HASH_MESSAGE


def test_unused_expression_value_is_reported_first(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:z}",
        ExpressionAttributeValues={":z": S(1, "z")},
    )


def test_invalid_exclusive_start_key_is_reported_after(dynamodb_client, index_table):
    _first(
        dynamodb_client,
        index_table,
        HASH_MESSAGE,
        ExclusiveStartKey={"pk": BIG},
    )


def test_exclusive_start_key_of_the_wrong_type_is_reported_after(
    dynamodb_client, index_table
):
    _first(
        dynamodb_client,
        index_table,
        HASH_MESSAGE,
        ExclusiveStartKey={"pk": {"N": "1"}, "sk": S(1, "s")},
    )


SELECT_ON_KEYS_ONLY_MESSAGE = (
    "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not "
    "supported for global secondary index gko because its projection type is not ALL"
)
START_KEY = {"g": S(1, "x"), "pk": S(1, "a"), "sk": S(1, "b")}


@pytest.mark.parametrize(
    ("value", "start_key", "message"),
    [
        pytest.param(BIG, None, HASH_MESSAGE, id="2049-bytes"),
        pytest.param(BIG, START_KEY, HASH_MESSAGE, id="2049-bytes-with-a-start-key"),
        pytest.param(S(1, "x"), START_KEY, SELECT_ON_KEYS_ONLY_MESSAGE, id="1-byte-with-a-start-key"),
    ],
)
def test_select_all_attributes_on_a_keys_only_gsi_is_reported_after(
    dynamodb_client, index_table, value, start_key, message
):
    # Measured 2026-11-20 in us-east-1, each shape twice.
    query = {
        "TableName": index_table,
        "IndexName": "gko",
        "Select": "ALL_ATTRIBUTES",
        "KeyConditionExpression": "g = :v",
        "ExpressionAttributeValues": {":v": value},
    }
    if start_key is not None:
        query["ExclusiveStartKey"] = start_key
    refused(dynamodb_client, message, **query)
