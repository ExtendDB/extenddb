# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query key value size limits.

A Query whose key condition carries a partition key value over 2048 bytes,
partition key values (on an index with several HASH attributes) that add up to
more than 2048 bytes, or range key values that add up to more than 1024 bytes,
is refused with a ValidationException. All limits are inclusive. The single
hash sentence has no space between ``of`` and ``2048``; the aggregated hash
sentence is its own. Every RANGE clause counts whatever its operator; a BETWEEN
counts the larger of its two bounds, so two 600-byte bounds pass and bounds of
1 and 1025 bytes are refused.

Every row below was measured on the service: the GSI hash and range rows on
2026-10-21 (index ``idx`` with HASH ``h`` and RANGE ``a``, ``b``, ``c``, ``d``,
all S), the base table rows on 2026-10-22 (HASH ``pk``, RANGE ``sk``, both S),
and the operator, BETWEEN, binary, LSI, legacy, multi-attribute and two-HASH
rows on 2026-10-23 (``issues/query-parity/service-435-shapes.jsonl``,
``service-435-review.jsonl`` and ``service-435-440-review-2.jsonl``). Rows that
send several RANGE clauses on one index are skipped on a server that refuses
that shape earlier as ``Query condition missed key schema element: h``, and the
``idx`` accept rows are skipped on a backend that answers ``InternalServerError``
to any Query on an index with several RANGE attributes (Cassandra, see
``query_key_size_support.require_multi_range``). Multi-byte
values, ordered BETWEEN bounds, undefined references and a hash-only table are in
``test_query_key_size_values.py``; the order against other refusals is in
``test_query_key_size_order.py``.
"""

from __future__ import annotations

import pytest

from query_key_size_support import (  # noqa: F401  (fixtures are used by name)
    HASH_MESSAGE,
    HASH_TOTAL_MESSAGE,
    RANGE_MESSAGE,
    B,
    S,
    base_table,
    binary_table,
    gsi_table,
    index_table,
    refused,
    require_multi_range,
    two_hash_table,
)


class TestGsiQueryKeySize:
    """Index ``idx`` with HASH h and RANGE a, b, c, d (service, 2026-10-21 and 2026-10-23)."""

    def _query(self, expression: str, values: dict) -> dict:
        return {
            "IndexName": "idx",
            "KeyConditionExpression": expression,
            "ExpressionAttributeValues": values,
        }

    def test_hash_value_of_2049_bytes_is_refused(self, dynamodb_client, gsi_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=gsi_table,
            **self._query("h = :big", {":big": S(2049)}),
        )

    def test_range_value_of_1025_bytes_is_refused(self, dynamodb_client, gsi_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=gsi_table,
            **self._query("h = :h AND a = :big", {":h": S(1, "h"), ":big": S(1025)}),
        )

    def test_one_600_byte_range_value_is_accepted(self, dynamodb_client, gsi_table):
        query = self._query("h = :h AND a = :s600", {":h": S(1, "h"), ":s600": S(600)})
        require_multi_range(dynamodb_client, TableName=gsi_table, **query)
        assert dynamodb_client.query(TableName=gsi_table, **query)["Count"] == 0

    def test_two_600_byte_range_values_are_refused_together(
        self, dynamodb_client, gsi_table
    ):
        query = self._query(
            "h = :h AND a = :s600 AND b = :b AND c = :s600",
            {":h": S(1, "h"), ":s600": S(600), ":b": S(1, "b")},
        )
        require_multi_range(dynamodb_client, TableName=gsi_table, **query)
        refused(dynamodb_client, RANGE_MESSAGE, TableName=gsi_table, **query)

    def test_two_range_values_totalling_1024_bytes_are_accepted(
        self, dynamodb_client, gsi_table
    ):
        query = self._query(
            "h = :h AND a = :a AND b = :b",
            {":h": S(1, "h"), ":a": S(512, "a"), ":b": S(512, "b")},
        )
        require_multi_range(dynamodb_client, TableName=gsi_table, **query)
        assert dynamodb_client.query(TableName=gsi_table, **query)["Count"] == 0

    def test_two_range_values_totalling_1025_bytes_are_refused(
        self, dynamodb_client, gsi_table
    ):
        query = self._query(
            "h = :h AND a = :a AND b = :b",
            {":h": S(1, "h"), ":a": S(512, "a"), ":b": S(513, "b")},
        )
        require_multi_range(dynamodb_client, TableName=gsi_table, **query)
        refused(dynamodb_client, RANGE_MESSAGE, TableName=gsi_table, **query)

    def test_a_1025_byte_non_equality_on_a_later_range_attribute_is_refused(
        self, dynamodb_client, gsi_table
    ):
        query = self._query(
            "h = :h AND a = :a AND b > :big",
            {":h": S(1, "h"), ":a": S(1, "a"), ":big": S(1025, "b")},
        )
        require_multi_range(dynamodb_client, TableName=gsi_table, **query)
        refused(dynamodb_client, RANGE_MESSAGE, TableName=gsi_table, **query)

    def test_between_adds_its_larger_bound_to_the_range_total(
        self, dynamodb_client, gsi_table
    ):
        # a = 600 plus the larger BETWEEN bound: 1 and 600 (1200) is refused,
        # 1 and 424 (1024) is accepted.
        refusing = self._query(
            "h = :h AND a = :a AND b BETWEEN :l AND :u",
            {":h": S(1, "h"), ":a": S(600, "a"), ":l": S(1, "b"), ":u": S(600, "b")},
        )
        require_multi_range(dynamodb_client, TableName=gsi_table, **refusing)
        refused(dynamodb_client, RANGE_MESSAGE, TableName=gsi_table, **refusing)
        accepted = self._query(
            "h = :h AND a = :a AND b BETWEEN :l AND :u",
            {":h": S(1, "h"), ":a": S(600, "a"), ":l": S(1, "b"), ":u": S(424, "b")},
        )
        assert dynamodb_client.query(TableName=gsi_table, **accepted)["Count"] == 0


class TestTwoHashQueryKeySize:
    """Index ``two-hash`` with HASH h1, h2 and RANGE a (service, 2026-10-23)."""

    def _query(self, h1: dict, h2: dict) -> dict:
        return {
            "IndexName": "two-hash",
            "KeyConditionExpression": "h1 = :a AND h2 = :b",
            "ExpressionAttributeValues": {":a": h1, ":b": h2},
        }

    def test_one_value_over_2048_bytes_gets_the_single_sentence(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=two_hash_table,
            **self._query(S(2049, "a"), S(1, "b")),
        )

    def test_values_adding_up_to_2049_bytes_get_the_aggregated_sentence(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            HASH_TOTAL_MESSAGE,
            TableName=two_hash_table,
            **self._query(S(1025, "a"), S(1024, "b")),
        )

    def test_two_2048_byte_values_get_the_aggregated_sentence(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            HASH_TOTAL_MESSAGE,
            TableName=two_hash_table,
            **self._query(S(2048, "a"), S(2048, "b")),
        )

    def test_values_adding_up_to_exactly_2048_bytes_are_accepted(
        self, dynamodb_client, two_hash_table
    ):
        resp = dynamodb_client.query(
            TableName=two_hash_table, **self._query(S(1024, "a"), S(1024, "b"))
        )
        assert resp["Count"] == 0

    def _query_with_range(self, h1: dict, h2: dict, a: dict) -> dict:
        return {
            "IndexName": "two-hash",
            "KeyConditionExpression": "h1 = :a AND h2 = :b AND a = :r",
            "ExpressionAttributeValues": {":a": h1, ":b": h2, ":r": a},
        }

    def test_hash_total_wins_over_an_oversized_range_value(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            HASH_TOTAL_MESSAGE,
            TableName=two_hash_table,
            **self._query_with_range(S(1025, "a"), S(1024, "b"), S(1025, "z")),
        )

    def test_single_hash_sentence_wins_over_an_oversized_range_value(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=two_hash_table,
            **self._query_with_range(S(2049, "a"), S(1, "b"), S(1025, "z")),
        )

    def test_range_sentence_when_the_hash_total_is_within_the_limit(
        self, dynamodb_client, two_hash_table
    ):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=two_hash_table,
            **self._query_with_range(S(1024, "a"), S(1024, "b"), S(1025, "z")),
        )


class TestBaseTableQueryKeySize:
    """Measured 2026-10-22 against a base table with HASH pk and RANGE sk."""

    def test_hash_value_of_2048_bytes_is_accepted(self, dynamodb_client, base_table):
        pk = S(2048, "p")
        dynamodb_client.put_item(
            TableName=base_table, Item={"pk": pk, "sk": {"S": "s"}}
        )
        resp = dynamodb_client.query(
            TableName=base_table,
            ConsistentRead=True,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": pk},
        )
        assert resp["Count"] == 1

    def test_hash_value_of_2049_bytes_isrefused(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": S(2049, "p")},
        )

    def test_range_value_of_1024_bytes_is_accepted(self, dynamodb_client, base_table):
        sk = S(1024, "s")
        dynamodb_client.put_item(
            TableName=base_table, Item={"pk": {"S": "range-at-limit"}, "sk": sk}
        )
        resp = dynamodb_client.query(
            TableName=base_table,
            ConsistentRead=True,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": {"S": "range-at-limit"}, ":v": sk},
        )
        assert resp["Count"] == 1

    def test_range_value_of_1025_bytes_isrefused(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": {"S": "p"}, ":v": S(1025, "s")},
        )

    def test_between_bounds_are_not_added_together(self, dynamodb_client, base_table):
        dynamodb_client.put_item(
            TableName=base_table, Item={"pk": {"S": "between"}, "sk": {"S": "m"}}
        )
        resp = dynamodb_client.query(
            TableName=base_table,
            ConsistentRead=True,
            KeyConditionExpression="pk = :p AND sk BETWEEN :l AND :h",
            ExpressionAttributeValues={
                ":p": {"S": "between"},
                ":l": S(600, "a"),
                ":h": S(600, "z"),
            },
        )
        assert resp["Count"] == 1


class TestRangeOperatorsAreSized:
    """Measured 2026-10-23: every RANGE operator counts its value."""

    @pytest.mark.parametrize(
        "expression",
        [
            "pk = :p AND sk > :v",
            "pk = :p AND sk >= :v",
            "pk = :p AND sk < :v",
            "pk = :p AND sk <= :v",
            "pk = :p AND begins_with(sk, :v)",
        ],
    )
    def test_1025_byte_value_isrefused(self, dynamodb_client, base_table, expression):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditionExpression=expression,
            ExpressionAttributeValues={":p": {"S": "p"}, ":v": S(1025, "s")},
        )

    def test_between_with_a_1025_byte_upper_bound_isrefused(
        self, dynamodb_client, base_table
    ):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk BETWEEN :l AND :h",
            ExpressionAttributeValues={
                ":p": {"S": "p"},
                ":l": S(1, "a"),
                ":h": S(1025, "z"),
            },
        )


class TestBinaryKeySize:
    """Measured 2026-10-23: binary key values are sized by byte length."""

    def test_hash_value_of_2048_bytes_is_accepted(self, dynamodb_client, binary_table):
        resp = dynamodb_client.query(
            TableName=binary_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": B(2048)},
        )
        assert resp["Count"] == 0

    def test_hash_value_of_2049_bytes_isrefused(self, dynamodb_client, binary_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=binary_table,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": B(2049)},
        )

    def test_range_value_of_1024_bytes_is_accepted(self, dynamodb_client, binary_table):
        resp = dynamodb_client.query(
            TableName=binary_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": B(1), ":v": B(1024)},
        )
        assert resp["Count"] == 0

    def test_range_value_of_1025_bytes_isrefused(self, dynamodb_client, binary_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=binary_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": B(1), ":v": B(1025)},
        )


class TestLsiQueryKeySize:
    """Measured 2026-10-23: the index's RANGE key is sized on an LSI query."""

    def test_range_value_of_1024_bytes_is_accepted(self, dynamodb_client, index_table):
        resp = dynamodb_client.query(
            TableName=index_table,
            IndexName="lsi",
            KeyConditionExpression="pk = :p AND l = :v",
            ExpressionAttributeValues={":p": {"S": "p"}, ":v": S(1024, "l")},
        )
        assert resp["Count"] == 0

    def test_range_value_of_1025_bytes_isrefused(self, dynamodb_client, index_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=index_table,
            IndexName="lsi",
            KeyConditionExpression="pk = :p AND l = :v",
            ExpressionAttributeValues={":p": {"S": "p"}, ":v": S(1025, "l")},
        )


class TestLegacyKeyConditionsKeySize:
    """Measured 2026-10-23 with the legacy KeyConditions parameter."""

    def test_hash_value_of_2049_bytes_isrefused(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=base_table,
            KeyConditions={
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [S(2049, "p")]}
            },
        )

    def test_range_value_of_1025_bytes_isrefused(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            RANGE_MESSAGE,
            TableName=base_table,
            KeyConditions={
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "p"}]},
                "sk": {"ComparisonOperator": "EQ", "AttributeValueList": [S(1025, "s")]},
            },
        )


class TestBothLimitsExceeded:
    """Measured 2026-10-23: the hash sentence wins."""

    def test_hash_sentence_wins(self, dynamodb_client, base_table):
        refused(
            dynamodb_client,
            HASH_MESSAGE,
            TableName=base_table,
            KeyConditionExpression="pk = :p AND sk = :v",
            ExpressionAttributeValues={":p": S(2049, "p"), ":v": S(1025, "s")},
        )
