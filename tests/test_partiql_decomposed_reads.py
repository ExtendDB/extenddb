# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the number of disjuncts a PartiQL SELECT read
by key may split into.

A SELECT whose every disjunct reads by key splits into at most 50 of them:
each value of an IN on a key attribute and each side of an OR of key
conditions is one, an AND multiplies them, and a disjunct held back for a
refusal made when it is read counts too. A 51st is refused with "Too many
decomposed read operations for a given query." on the first request, on a
table and on an index. The conditions of a filter are not counted, and a
statement with a disjunct that is a scan runs as a scan.

The refusal comes after the operator count, the missing table, the scalar
key refusal, the key type check, the ORDER BY refusals and the overlap
refusal, and before the key size check and the refusal of a key attribute
in a filter.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-03).
"""

from __future__ import annotations

import time
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

TOO_MANY = "Too many decomposed read operations for a given query."
TOO_MANY_OPERATORS = "The expression contains too many operators; operator count: 301"
KEY_TYPE = "Key attribute's data type should match its data type in table's schema: Key {}"
SCALAR = (
    "Key attributes must be scalars; list random access '[]' and map lookup '.' are not "
    "allowed: Key sk"
)
OVERLAP = "Overlapping conditions with range keys are not supported in where clause"
ORDER_BY_HASH = (
    "Must have hash key in ORDER BY clause when more than one hash key condition specified in "
    "WHERE clause."
)
ORDER_BY_KEY = "Attribute n in ORDER BY clause must be part of the primary key"
BIG_HASH = "z" * 2049
FILTER = " AND ".join(f"x{i:02} = 1" for i in range(1, 100))


def _values(n, prefix="v", start=0):
    """`n` string values `<prefix>000`, `<prefix>001`, ... from `start`."""
    return ", ".join(f"'{prefix}{i:03}'" for i in range(start, start + n))


def _ors(values):
    """An OR of equalities of pk with each of `values`."""
    return " OR ".join(f"pk = 'v{i:03}'" for i in values)


def _wait_for_index(client, table, count):
    """Wait until the index gix shows every item written (it fills
    asynchronously on the service)."""
    for _ in range(240):
        scan = client.scan(TableName=table, IndexName="gix", Select="COUNT")
        if scan["Count"] == count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"gix of {table} did not reach {count} items")


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A table keyed on a string pk holding v000 to v029, each with x = 2."""
    with scoped_table(dynamodb_client) as name:
        for i in range(30):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"v{i:03}"}, "x": {"N": "2"}}
            )
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on string pk and sk holding v000/s to v029/s, each with
    n = 1."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        for i in range(30):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"v{i:03}"}, "sk": {"S": "s"}, "n": {"N": "1"}},
            )
        yield name


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """A table keyed on pk with a GSI gix keyed on g, holding p000 to p029
    with g v000 to v029."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gix",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        for i in range(30):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"p{i:03}"}, "g": {"S": f"v{i:03}"}}
            )
        _wait_for_index(dynamodb_client, name, 30)
        yield name


@pytest.fixture(scope="module")
def g2table(dynamodb_client):
    """A table keyed on pk with a GSI gix keyed on g and gs, holding p000 to
    p014 in index partition a with gs w000 to w014."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": name, "AttributeType": "S"} for name in ("pk", "g", "gs")
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gix",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        for i in range(15):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"p{i:03}"}, "g": {"S": "a"}, "gs": {"S": f"w{i:03}"}},
            )
        _wait_for_index(dynamodb_client, name, 15)
        yield name


def _count(client, statement, **kwargs):
    """How many items `statement` returns over all its pages, following
    NextToken, or the message of the ValidationException that refuses it."""
    count, token = 0, None
    for _ in range(40):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        try:
            response = client.execute_statement(Statement=statement, **args)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ValidationException", e.response
            return e.response["Error"]["Message"]
        count += len(response["Items"])
        token = response.get("NextToken")
        if not token:
            return count
    raise AssertionError(f"more than 40 pages: {statement[:80]}")


# ---- what counts -------------------------------------------------------------


@pytest.mark.parametrize(
    ("condition", "result"),
    [
        (f"pk IN [{_values(50)}]", 30),
        (f"pk IN [{_values(51)}]", TOO_MANY),
        (f"pk IN [{_values(100)}]", TOO_MANY),
        (_ors(range(50)), 30),
        (_ors(range(51)), TOO_MANY),
        (_ors(range(150)), TOO_MANY),
        (f"pk IN [{_values(26)}] OR pk IN [{_values(25, start=26)}]", TOO_MANY),
        # The conditions of a filter are not counted.
        (f"pk IN [{_values(50)}] AND {FILTER}", 0),
        (f"pk IN [{_values(99)}] AND x01 = 1", TOO_MANY),
        # A statement with a disjunct that is a scan runs as a scan.
        (f"pk IN [{_values(51)}] OR x = 1", 30),
        (f"x = 1 OR pk IN [{_values(51)}]", 30),
    ],
    ids=[
        "in-50",
        "in-51",
        "in-100",
        "or-50",
        "or-51",
        "or-150",
        "two-in-lists",
        "in-50-and-a-long-filter",
        "in-99-and-a-short-filter",
        "in-51-or-a-scan",
        "a-scan-or-in-51",
    ],
)
def test_a_read_by_partition_key_splits_into_at_most_50_disjuncts(
    dynamodb_client, htable, condition, result
):
    assert _count(dynamodb_client, f'SELECT * FROM "{htable}" WHERE {condition}') == result


@pytest.mark.parametrize(
    ("condition", "result"),
    [
        (f"pk IN [{_values(25)}] AND sk IN [{_values(2, 's')}]", 0),
        (f"pk IN [{_values(10)}] AND sk IN [{_values(5, 's')}]", 0),
        (f"pk IN [{_values(5)}] AND sk IN [{_values(10, 's')}]", 0),
        (f"pk IN [{_values(17)}] AND sk IN [{_values(3, 's')}]", TOO_MANY),
        (f"pk IN [{_values(10)}] AND sk IN [{_values(10, 's')}]", TOO_MANY),
        (f"pk IN [{_values(50)}] AND sk = 's'", 30),
        (f"pk IN [{_values(51)}] AND sk = 's'", TOO_MANY),
        (f"pk IN [{_values(51)}] AND sk > 'a'", TOO_MANY),
        (f"pk IN [{_values(26)}] AND (sk = 's' OR sk = 't')", TOO_MANY),
        (
            f"(pk IN [{_values(26)}] AND sk = 's') OR "
            f"(pk IN [{_values(25, start=26)}] AND sk = 't')",
            TOO_MANY,
        ),
        (f"pk = 'v000' AND sk IN [{_values(50, 's')}]", 0),
        (f"pk = 'v000' AND sk IN [{_values(51, 's')}]", TOO_MANY),
        # Not every disjunct reads by key: a scan.
        (f"pk IN [{_values(51)}] AND sk = 's' AND pk = 'v000'", 1),
    ],
    ids=[
        "25-by-2",
        "10-by-5",
        "5-by-10",
        "17-by-3",
        "10-by-10",
        "items-50",
        "items-51",
        "ranges-51",
        "in-26-by-two-sort-keys",
        "26-or-25",
        "sort-key-in-50",
        "sort-key-in-51",
        "a-second-partition-value",
    ],
)
def test_a_read_by_both_keys_counts_every_disjunct_of_the_product(
    dynamodb_client, ctable, condition, result
):
    assert _count(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}') == result


def test_parameters_are_counted(dynamodb_client, htable):
    parameters = [{"S": f"v{i:03}"} for i in range(51)]

    def statement(n):
        return f"SELECT * FROM \"{htable}\" WHERE pk IN [{', '.join(['?'] * n)}]"

    assert _count(dynamodb_client, statement(51), Parameters=parameters) == TOO_MANY
    assert _count(dynamodb_client, statement(50), Parameters=parameters[:50]) == 30


@pytest.mark.parametrize(
    ("condition", "result"),
    [
        (f"g IN [{_values(50)}]", 30),
        (f"g IN [{_values(51)}]", TOO_MANY),
        (f"g IN [{_values(50)}] AND {FILTER}", 0),
        (f"g IN [{_values(99)}] AND x01 = 1", TOO_MANY),
    ],
    ids=["in-50", "in-51", "in-50-and-a-long-filter", "in-99-and-a-short-filter"],
)
def test_an_index_read_splits_into_at_most_50_disjuncts(
    dynamodb_client, gtable, condition, result
):
    statement = f'SELECT * FROM "{gtable}"."gix" WHERE {condition}'
    assert _count(dynamodb_client, statement) == result


@pytest.mark.parametrize(("n", "result"), [(50, 15), (51, TOO_MANY)])
def test_sort_key_values_of_one_index_partition_are_counted(dynamodb_client, g2table, n, result):
    statement = f"SELECT * FROM \"{g2table}\".\"gix\" WHERE g = 'a' AND gs IN [{_values(n, 'w')}]"
    assert _count(dynamodb_client, statement) == result


# ---- where the refusal comes -------------------------------------------------


@pytest.mark.parametrize(
    ("table", "condition", "result"),
    [
        ("h", _ors(range(200)), TOO_MANY_OPERATORS),
        ("c", f"pk IN [{_values(51)}] AND sk.x = 1", SCALAR),
        ("h", f"pk IN [{_values(50)}, 1]", KEY_TYPE.format("pk")),
        ("h", f"pk IN [1, {_values(50)}]", KEY_TYPE.format("pk")),
        ("c", f"pk IN [{_values(51)}] AND sk = 1", KEY_TYPE.format("sk")),
        ("c", f"pk IN [{_values(51)}] ORDER BY sk DESC", ORDER_BY_HASH),
        ("c", f"pk IN [{_values(51)}] ORDER BY n", ORDER_BY_KEY),
        ("h", "pk IN [" + ", ".join(f"'v{i % 26:03}'" for i in range(51)) + "]", OVERLAP),
        ("h", "pk IN [" + ", ".join(f"'v{i % 51:03}'" for i in range(60)) + "]", OVERLAP),
        ("h", "pk IN [" + ", ".join(["'v000'"] * 100) + "]", OVERLAP),
        ("h", _ors(i % 26 for i in range(51)), OVERLAP),
        ("h", f"pk IN [{_values(26)}] OR pk IN [{_values(26)}]", OVERLAP),
        ("c", f"pk IN [{_values(26)}] AND (sk = 's' OR sk > 'a')", OVERLAP),
    ],
    ids=[
        "operator-count",
        "scalar-key",
        "key-type-last",
        "key-type-first",
        "sort-key-type",
        "order-by-hash-key",
        "order-by-other-attribute",
        "overlap-51-of-26",
        "overlap-60-of-51",
        "overlap-100-copies",
        "overlap-or",
        "overlap-two-in-lists",
        "overlap-sort-key",
    ],
)
def test_the_refusals_checked_before_the_count(
    dynamodb_client, htable, ctable, table, condition, result
):
    name = htable if table == "h" else ctable
    assert _count(dynamodb_client, f'SELECT * FROM "{name}" WHERE {condition}') == result


def test_a_missing_table_is_reported_before_the_count(dynamodb_client):
    missing = f"extenddb-no-such-{uuid.uuid4().hex[:12]}"
    statement = f'SELECT * FROM "{missing}" WHERE pk IN [{_values(51)}]'
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(Statement=statement)
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert e.value.response["Error"]["Message"] == "Requested resource not found"


@pytest.mark.parametrize(
    ("table", "condition"),
    [
        ("h", f"pk IN [{_values(50)}, '{BIG_HASH}']"),
        ("h", f"pk IN ['{BIG_HASH}', {_values(50)}]"),
        ("c", f"pk IN [{_values(51)}] AND sk = a"),
        ("h", f"pk IN [{_values(51)}] ORDER BY pk"),
    ],
    ids=["oversized-last", "oversized-first", "key-attribute-in-the-filter", "order-by-pk"],
)
def test_the_count_comes_before_the_checks_made_when_a_disjunct_is_read(
    dynamodb_client, htable, ctable, table, condition
):
    name = htable if table == "h" else ctable
    assert _count(dynamodb_client, f'SELECT * FROM "{name}" WHERE {condition}') == TOO_MANY


def test_a_write_names_its_item_with_one_value_of_each_key(dynamodb_client, htable):
    statement = f'UPDATE "{htable}" SET q = 1 WHERE pk IN [{_values(2)}]'
    assert _count(dynamodb_client, statement) == (
        "Where clause does not contain a mandatory equality on all key attributes"
    )
