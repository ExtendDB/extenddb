# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query ExclusiveStartKey outside the sort key condition, base table (issue #440).

Every row here was measured on Amazon DynamoDB (2026-10-23 and 2026-10-24,
each row twice with identical results; page ends ten times). The rule:

* The start key's sort value must be one from which "a later key in the scan
  direction can still satisfy the range condition". Otherwise the range
  sentence. Strict operators exclude the bound; `<=` and the BETWEEN upper
  bound refuse the bound going forward, `>=` and the BETWEEN lower bound
  refuse it in reverse. This is checked first, for either partition.
* `sk = m` with the start key's own sort value is "The provided exclusive
  start key is invalid" (same partition, either direction), and
  `begins_with` in reverse at exactly the prefix is "outside query
  boundaries" (same partition).
* A start key from another partition that passes those checks gets "The
  query can return at most one row and cannot be restarted".

Partition rows without a sort key condition are in
test_query_start_key_partition.py, index rows in test_query_start_key_index.py.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

RANGE_PREDICATE = "The provided starting key does not match the range key predicate"
EXCLUSIVE_INVALID = "The provided exclusive start key is invalid"
OUTSIDE_BOUNDARIES = (
    "The provided starting key is outside query boundaries based on provided conditions"
)
AT_MOST_ONE_ROW = "The query can return at most one row and cannot be restarted"

_extenddb_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_VALIDATION_MODE", "").lower() == "true",
    reason="extenddb_only: shape not measured on the service",
)


def _assert_validation(exc_info, message):
    err = exc_info.value.response["Error"]
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Code"] == "ValidationException"
    assert err["Message"] == message


def _run(client, expected, **query):
    """Run a Query; `expected` is a sentence (refused) or an item count (accepted)."""
    if isinstance(expected, str):
        with pytest.raises(ClientError) as exc_info:
            client.query(**query)
        _assert_validation(exc_info, expected)
        return
    resp = client.query(**query)
    assert resp["Count"] == expected


@pytest.fixture(scope="module")
def range_table(dynamodb_client):
    """pk+sk table: partition alice with sk a, aa, ab, abz, m, z, plus bob/a."""
    name = unique_name("esk-range")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    for pk, sk in (
        ("alice", "a"),
        ("alice", "aa"),
        ("alice", "ab"),
        ("alice", "abz"),
        ("alice", "m"),
        ("alice", "z"),
        ("bob", "a"),
    ):
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}})
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass


def _query(table, kce, values, esk, forward=True, **more):
    q = {
        "TableName": table,
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": {
            ":p": {"S": "alice"},
            **{k: {"S": v} for k, v in values.items()},
        },
        "ExclusiveStartKey": {"pk": {"S": esk[0]}, "sk": {"S": esk[1]}},
        "ScanIndexForward": forward,
    }
    q.update(more)
    return q


GE = "pk = :p AND sk >= :v"
GT = "pk = :p AND sk > :v"
LE = "pk = :p AND sk <= :v"
LT = "pk = :p AND sk < :v"
EQ = "pk = :p AND sk = :v"
BETWEEN = "pk = :p AND sk BETWEEN :lo AND :hi"
BEGINS = "pk = :p AND begins_with(sk, :v)"
M = {":v": "m"}
AA_M = {":lo": "aa", ":hi": "m"}

# (kce, bound values, start key sort value, forward, expected: sentence or item count)
SAME_PARTITION_ROWS = [
    # Forward. A lower bound accepts the bound itself; strict operators and
    # <= / BETWEEN upper refuse it; below or above the range is refused.
    (GE, M, "m", True, 1),
    (GE, M, "a", True, RANGE_PREDICATE),
    (GE, {":v": "a"}, "m", True, 1),
    (GT, M, "m", True, RANGE_PREDICATE),
    (LT, M, "m", True, RANGE_PREDICATE),
    (LE, M, "z", True, RANGE_PREDICATE),
    (LE, M, "m", True, RANGE_PREDICATE),
    (BETWEEN, AA_M, "a", True, RANGE_PREDICATE),
    (BETWEEN, AA_M, "z", True, RANGE_PREDICATE),
    (BETWEEN, AA_M, "aa", True, 3),
    (BETWEEN, AA_M, "ab", True, 2),
    (BETWEEN, AA_M, "m", True, RANGE_PREDICATE),
    (EQ, M, "a", True, RANGE_PREDICATE),
    (EQ, M, "m", True, EXCLUSIVE_INVALID),
    (BEGINS, {":v": "ab"}, "a", True, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "aa", True, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "ac", True, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "m", True, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "ab", True, 1),
    (BEGINS, {":v": "ab"}, "abz", True, 0),
    (BEGINS, {":v": "aa"}, "aa", True, 0),
    (BEGINS, {":v": "a"}, "a", True, 3),
    (BEGINS, {":v": "ord-"}, "z", True, RANGE_PREDICATE),
    # Reverse. The far end is the lower bound: >= and BETWEEN lower refuse
    # it, <= and BETWEEN upper accept it; begins_with at the prefix is a
    # different sentence.
    (GE, M, "m", False, RANGE_PREDICATE),
    (GE, M, "z", False, 1),
    (GE, M, "a", False, RANGE_PREDICATE),
    (LE, M, "m", False, 4),
    (GT, {":v": "a"}, "a", False, RANGE_PREDICATE),
    (LT, M, "m", False, RANGE_PREDICATE),
    (BETWEEN, AA_M, "aa", False, RANGE_PREDICATE),
    (BETWEEN, AA_M, "m", False, 3),
    (BETWEEN, AA_M, "ab", False, 1),
    (EQ, M, "m", False, EXCLUSIVE_INVALID),
    (EQ, M, "a", False, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "ab", False, OUTSIDE_BOUNDARIES),
    (BEGINS, {":v": "aa"}, "aa", False, OUTSIDE_BOUNDARIES),
    (BEGINS, {":v": "ab"}, "abz", False, 1),
    (BEGINS, {":v": "ab"}, "aa", False, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "ac", False, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "a", False, RANGE_PREDICATE),
    (BEGINS, {":v": "ab"}, "m", False, RANGE_PREDICATE),
]


def _row_id(row):
    kce, values, esk_sk, forward, _ = row
    operator = kce.split("AND ", 1)[1]
    return f"{operator}|{','.join(values.values())}|esk={esk_sk}|{'fwd' if forward else 'rev'}"


@pytest.mark.parametrize("row", SAME_PARTITION_ROWS, ids=[_row_id(r) for r in SAME_PARTITION_ROWS])
def test_same_partition_start_key(range_table, dynamodb_client, row):
    kce, values, esk_sk, forward, expected = row
    _run(
        dynamodb_client,
        expected,
        **_query(range_table, kce, values, ("alice", esk_sk), forward),
    )


# (kce, bound values, start key (pk, sk), forward, expected sentence)
OTHER_PARTITION_ROWS = [
    # Inside the range for every operator, forward: the fifth sentence.
    (GE, {":v": "a"}, ("bob", "m"), True, AT_MOST_ONE_ROW),
    (GT, {":v": "a"}, ("bob", "m"), True, AT_MOST_ONE_ROW),
    (LE, {":v": "z"}, ("bob", "m"), True, AT_MOST_ONE_ROW),
    (LT, {":v": "z"}, ("bob", "m"), True, AT_MOST_ONE_ROW),
    (BEGINS, {":v": "a"}, ("bob", "aa"), True, AT_MOST_ONE_ROW),
    (BETWEEN, {":lo": "a", ":hi": "z"}, ("bob", "m"), True, AT_MOST_ONE_ROW),
    (BETWEEN, AA_M, ("bob", "ab"), True, AT_MOST_ONE_ROW),
    (EQ, M, ("bob", "m"), True, AT_MOST_ONE_ROW),
    # Reverse.
    (GE, {":v": "a"}, ("bob", "m"), False, AT_MOST_ONE_ROW),
    (GE, M, ("bob", "z"), False, AT_MOST_ONE_ROW),
    (BEGINS, {":v": "a"}, ("bob", "ab"), False, AT_MOST_ONE_ROW),
    (EQ, M, ("bob", "m"), False, AT_MOST_ONE_ROW),
    # Outside the range, or at the far end by sort value: the range sentence wins.
    (GE, M, ("bob", "a"), True, RANGE_PREDICATE),
    (GE, M, ("bob", "a"), False, RANGE_PREDICATE),
    (BETWEEN, AA_M, ("bob", "m"), True, RANGE_PREDICATE),
    (LE, M, ("bob", "m"), True, RANGE_PREDICATE),
    (EQ, M, ("bob", "a"), True, RANGE_PREDICATE),
    (EQ, M, ("bob", "z"), True, RANGE_PREDICATE),
    (EQ, M, ("bob", "a"), False, RANGE_PREDICATE),
    (EQ, M, ("bob", "z"), False, RANGE_PREDICATE),
]


@pytest.mark.parametrize(
    "row",
    OTHER_PARTITION_ROWS,
    ids=[f"{_row_id((r[0], r[1], '%s/%s' % r[2], r[3], r[4]))}" for r in OTHER_PARTITION_ROWS],
)
def test_other_partition_start_key(range_table, dynamodb_client, row):
    kce, values, esk, forward, expected = row
    _run(dynamodb_client, expected, **_query(range_table, kce, values, esk, forward))


def test_other_partition_without_a_sort_key_condition_in_reverse(range_table, dynamodb_client):
    _run(
        dynamodb_client,
        OUTSIDE_BOUNDARIES,
        **_query(range_table, "pk = :p", {}, ("bob", "m"), forward=False),
    )


class TestLegacyKeyConditions:
    """KeyConditions reaches the same check."""

    @staticmethod
    def _legacy(table, operator, values, esk_sk):
        return {
            "TableName": table,
            "KeyConditions": {
                "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "alice"}]},
                "sk": {
                    "ComparisonOperator": operator,
                    "AttributeValueList": [{"S": v} for v in values],
                },
            },
            "ExclusiveStartKey": {"pk": {"S": "alice"}, "sk": {"S": esk_sk}},
        }

    def test_ge(self, range_table, dynamodb_client):
        _run(dynamodb_client, RANGE_PREDICATE, **self._legacy(range_table, "GE", ["m"], "a"))

    def test_between(self, range_table, dynamodb_client):
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            **self._legacy(range_table, "BETWEEN", ["aa", "m"], "z"),
        )

    def test_begins_with(self, range_table, dynamodb_client):
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            **self._legacy(range_table, "BEGINS_WITH", ["ab"], "a"),
        )


class TestKeyTypes:
    """Number, binary and non-ASCII string sort keys."""

    def test_number_sort_key_compares_numerically(self, table_factory, dynamodb_client):
        name = table_factory(hash_type="N", range_key="sk", range_type="N")
        for v in ("1", "2", "10"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"N": "1"}, "sk": {"N": v}})
        base = {
            "TableName": name,
            "KeyConditionExpression": "pk = :p AND sk >= :lo",
            "ExpressionAttributeValues": {":p": {"N": "1"}, ":lo": {"N": "2"}},
        }
        # 10 is above 2 numerically (it sorts below "2" as text).
        _run(
            dynamodb_client,
            0,
            **base,
            ExclusiveStartKey={"pk": {"N": "1"}, "sk": {"N": "10"}},
        )
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            **base,
            ExclusiveStartKey={"pk": {"N": "1"}, "sk": {"N": "1"}},
        )

    def test_binary_sort_key_compares_as_unsigned_bytes(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk", range_type="B")
        for b in (b"\x01", b"\x7f", b"\x80"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "alice"}, "sk": {"B": b}})
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            TableName=name,
            KeyConditionExpression="pk = :p AND sk >= :lo",
            ExpressionAttributeValues={":p": {"S": "alice"}, ":lo": {"B": b"\x80"}},
            ExclusiveStartKey={"pk": {"S": "alice"}, "sk": {"B": b"\x7f"}},
        )
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            TableName=name,
            KeyConditionExpression="pk = :p AND begins_with(sk, :pre)",
            ExpressionAttributeValues={":p": {"S": "alice"}, ":pre": {"B": b"\x01"}},
            ExclusiveStartKey={"pk": {"S": "alice"}, "sk": {"B": b"\x7f"}},
        )

    def test_string_sort_key_compares_in_utf8_byte_order(self, range_table, dynamodb_client):
        # U+00E9 is above "z" in UTF-8 byte order.
        _run(
            dynamodb_client,
            RANGE_PREDICATE,
            **_query(range_table, GE, {":v": "\u00e9"}, ("alice", "z")),
        )
        # U+10000 is above U+FF5E in UTF-8 byte order (UTF-16 order says below).
        _run(
            dynamodb_client,
            0,
            **_query(range_table, GE, {":v": "\uff5e"}, ("alice", "\U00010000")),
        )


class TestPageEndingAtTheEndOfTheRange:
    """A base table page that ends on the last item that can satisfy the range
    condition in the scan direction, with Limit filled exactly, carries no
    LastEvaluatedKey (measured, ten of ten), so feeding a key back never meets
    the refusals above."""

    @pytest.fixture()
    def table(self, table_factory, dynamodb_client):
        name = table_factory(range_key="sk")
        for i in range(1, 6):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "p"}, "sk": {"S": f"s{i}"}}
            )
        return name

    def test_between_with_limit_equal_to_the_matches_returns_no_key(
        self, table, dynamodb_client
    ):
        resp = dynamodb_client.query(
            TableName=table,
            KeyConditionExpression="pk = :p AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":p": {"S": "p"},
                ":lo": {"S": "s1"},
                ":hi": {"S": "s3"},
            },
            Limit=3,
        )
        assert resp["Count"] == 3
        assert "LastEvaluatedKey" not in resp

    def test_equality_with_limit_one_returns_no_key(self, range_table, dynamodb_client):
        resp = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression=EQ,
            ExpressionAttributeValues={":p": {"S": "alice"}, ":v": {"S": "m"}},
            Limit=1,
        )
        assert resp["Count"] == 1
        assert "LastEvaluatedKey" not in resp

    def test_reverse_lower_bound_with_limit_equal_to_the_matches_returns_no_key(
        self, range_table, dynamodb_client
    ):
        # sk >= m in reverse over z, m.
        resp = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression=GE,
            ExpressionAttributeValues={":p": {"S": "alice"}, ":v": {"S": "m"}},
            ScanIndexForward=False,
            Limit=2,
        )
        assert [item["sk"]["S"] for item in resp["Items"]] == ["z", "m"]
        assert "LastEvaluatedKey" not in resp

    @pytest.mark.parametrize("forward", [True, False])
    @pytest.mark.parametrize("limit", [1, 2, 3])
    @pytest.mark.parametrize(
        "kce,values,expected_total",
        [
            ("pk = :p AND sk BETWEEN :lo AND :hi", {":lo": "s2", ":hi": "s4"}, 3),
            ("pk = :p AND sk <= :v", {":v": "s3"}, 3),
            ("pk = :p AND sk >= :v", {":v": "s3"}, 3),
            ("pk = :p AND sk = :v", {":v": "s3"}, 1),
            ("pk = :p AND begins_with(sk, :v)", {":v": "s"}, 5),
        ],
    )
    def test_every_key_returned_is_accepted_back(
        self, table, dynamodb_client, kce, values, expected_total, limit, forward
    ):
        seen, start, pages = [], None, 0
        while True:
            query = {
                "TableName": table,
                "KeyConditionExpression": kce,
                "ExpressionAttributeValues": {
                    ":p": {"S": "p"},
                    **{k: {"S": v} for k, v in values.items()},
                },
                "Limit": limit,
                "ScanIndexForward": forward,
            }
            if start:
                query["ExclusiveStartKey"] = start
            resp = dynamodb_client.query(**query)
            seen.extend(item["sk"]["S"] for item in resp["Items"])
            start = resp.get("LastEvaluatedKey")
            pages += 1
            assert pages < 10
            if not start:
                break
        assert len(seen) == expected_total
        assert len(set(seen)) == expected_total

    @_extenddb_only
    @pytest.mark.parametrize("forward", [True, False])
    def test_hash_only_table_walk_with_limit_one_completes(
        self, table_factory, dynamodb_client, forward
    ):
        """Not measured: the service's page for this shape. A hash-only table
        answers at most one row and refuses every start key, so a page that
        carried a key could never be followed."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "alice"}})
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "alice"}},
            Limit=1,
            ScanIndexForward=forward,
        )
        assert resp["Count"] == 1
        assert "LastEvaluatedKey" not in resp
