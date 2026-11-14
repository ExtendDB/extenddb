# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query key size rows for multi-byte values, ordered BETWEEN bounds, undefined
references, a hash-only table and a GSI.

Measured on the service on 2026-10-23 (``issues/query-parity/service-435-review.jsonl``,
labels 1 to 4, 6, 10). The main key size rows are in ``test_query_key_size.py``.
"""

from __future__ import annotations

from query_key_size_support import (  # noqa: F401  (fixtures are used by name)
    HASH_MESSAGE,
    RANGE_MESSAGE,
    S,
    base_table,
    hash_only_table,
    index_table,
    refused,
    refused_ending,
)

EURO = "\u20ac"  # three bytes in UTF-8


class TestMultiByteKeySize:
    """Strings are sized by their UTF-8 bytes, not their characters."""

    def test_683_three_byte_characters_in_a_hash_value_are_refused(
        self, dynamodb_client, base_table
    ):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": {"S": EURO * 683}},
        )

    def test_a_hash_value_of_2048_bytes_with_multi_byte_characters_is_accepted(
        self, dynamodb_client, base_table
    ):
        resp = dynamodb_client.query(
            TableName=base_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": {"S": EURO * 682 + "xx"}},
        )
        assert resp["Count"] == 0

    def test_342_three_byte_characters_in_a_range_value_are_refused(
        self, dynamodb_client, base_table
    ):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": S(1, "p"), ":v": {"S": EURO * 342}},
        )

    def test_a_range_value_of_1024_bytes_with_multi_byte_characters_is_accepted(
        self, dynamodb_client, base_table
    ):
        resp = dynamodb_client.query(
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": S(1, "p"), ":v": {"S": EURO * 341 + "x"}},
        )
        assert resp["Count"] == 0


class TestBetweenLowerBound:
    """An ordered BETWEEN is sized by its larger bound, which may be the lower one."""

    def test_a_1025_byte_lower_bound_is_refused(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk BETWEEN :l AND :u",
            ExpressionAttributeValues={
                ":p": S(1, "p"),
                ":l": S(1025, "a"),
                ":u": S(1, "z"),
            },
        )

    def test_a_1024_byte_lower_bound_is_accepted(self, dynamodb_client, base_table):
        resp = dynamodb_client.query(
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk BETWEEN :l AND :u",
            ExpressionAttributeValues={
                ":p": S(1, "p"),
                ":l": S(1024, "a"),
                ":u": S(1, "z"),
            },
        )
        assert resp["Count"] == 0


class TestUndefinedReferencesWin:
    """An undefined value in the key condition is reported, not the size.

    The service words it ``Invalid KeyConditionExpression: An expression
    attribute value used in expression is not defined; attribute value:
    :missing``. ExtendDB omits the leading label (a wording gap that exists
    without this check), so the row compares the text after it.
    """

    def test_an_undefined_value_is_reported_ahead_of_a_2049_byte_hash(
        self, dynamodb_client, base_table
    ):
        refused_ending(
            dynamodb_client,
            "An expression attribute value used in expression is not defined; "
            "attribute value: :missing",
            TableName=base_table,
            KeyConditionExpression="pk = :v AND sk = :missing",
            ExpressionAttributeValues={":v": S(2049, "p")},
        )


class TestHashOnlyTable:
    def test_a_2049_byte_hash_value_is_refused(self, dynamodb_client, hash_only_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=hash_only_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": S(2049, "p")},
        )

    def test_a_2048_byte_hash_value_is_accepted(self, dynamodb_client, hash_only_table):
        resp = dynamodb_client.query(
            TableName=hash_only_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": S(2048, "p")},
        )
        assert resp["Count"] == 0


class TestIndexRangeClauses:
    """The index's own RANGE key is sized: a GSI with one RANGE attribute and an LSI."""

    def test_a_gsi_non_equality_of_1025_bytes_is_refused(
        self, dynamodb_client, index_table
    ):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=index_table,
            IndexName="gsi",
            KeyConditionExpression="g = :g AND gs > :v",
            ExpressionAttributeValues={":g": S(1, "g"), ":v": S(1025, "s")},
        )

    def test_a_gsi_between_with_two_600_byte_bounds_is_accepted(
        self, dynamodb_client, index_table
    ):
        resp = dynamodb_client.query(
            TableName=index_table,
            IndexName="gsi",
            KeyConditionExpression="g = :g AND gs BETWEEN :l AND :u",
            ExpressionAttributeValues={
                ":g": S(1, "g"),
                ":l": S(600, "a"),
                ":u": S(600, "z"),
            },
        )
        assert resp["Count"] == 0

    def test_an_lsi_begins_with_of_1025_bytes_is_refused(
        self, dynamodb_client, index_table
    ):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=index_table,
            IndexName="lsi",
            KeyConditionExpression="pk = :p AND begins_with(l, :v)",
            ExpressionAttributeValues={":p": S(1, "p"), ":v": S(1025, "l")},
        )
