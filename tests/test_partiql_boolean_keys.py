# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for a key condition compared with a boolean in a
PartiQL SELECT.

When a key condition (an equality of the partition key with a value, a key
condition on the sort key, or an AND of them) is compared with a boolean
literal, the service reads by that key condition and the literal alone
decides: compared with true (`= true` or `<> true`) every item read is
returned, compared with false (`= false` or `<> false`) none. Any other
condition compared with a boolean keeps its meaning as a condition.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-03).
"""

from __future__ import annotations

import pytest

from conftest import scoped_table


def _filled(client, keys):
    """A table keyed on string pk and sk holding the items `keys`, each with
    n = 1."""
    with scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        for pk, sk in keys:
            client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": "1"}}
            )
        yield name


@pytest.fixture(scope="module")
def four(dynamodb_client):
    """Items a/x, a/y, b/x and c/x."""
    yield from _filled(dynamodb_client, [("a", "x"), ("a", "y"), ("b", "x"), ("c", "x")])


@pytest.fixture(scope="module")
def five(dynamodb_client):
    """Items a/x, a/y, a/z, b/x and b/y."""
    yield from _filled(
        dynamodb_client, [("a", "x"), ("a", "y"), ("a", "z"), ("b", "x"), ("b", "y")]
    )


def _keys(client, table, condition):
    statement = f'SELECT * FROM "{table}" WHERE {condition}'
    items, token = [], None
    for _ in range(8):
        args = {"NextToken": token} if token else {}
        response = client.execute_statement(Statement=statement, **args)
        items += response["Items"]
        token = response.get("NextToken")
        if not token:
            break
    return sorted(f"{item['pk']['S']}/{item['sk']['S']}" for item in items)


@pytest.mark.parametrize(
    ("condition", "keys"),
    [
        ("pk = 'a' <> true", ["a/x", "a/y", "a/z"]),
        ("pk = 'b' <> true", ["b/x", "b/y"]),
        ("pk = 'a' = true", ["a/x", "a/y", "a/z"]),
        ("pk = 'a' AND sk = 'y' <> true", ["a/y"]),
        ("pk = 'a' AND sk = 'x' = true", ["a/x"]),
        ("pk = 'a' AND sk > 'x' <> true", ["a/y", "a/z"]),
        ("(pk = 'a' AND sk = 'x') <> true", ["a/x"]),
        ("pk = 'a' <> true AND n = 1", ["a/x", "a/y", "a/z"]),
        ("pk = 'a' <> true OR pk = 'b'", ["a/x", "a/y", "a/z", "b/x", "b/y"]),
    ],
)
def test_a_key_condition_compared_with_true_returns_what_it_reads(
    dynamodb_client, five, condition, keys
):
    assert _keys(dynamodb_client, five, condition) == keys


@pytest.mark.parametrize(
    "condition",
    [
        "pk = 'a' <> false",
        "pk = 'a' AND sk > 'x' = false",
        "(pk = 'a' AND sk = 'x') = false",
    ],
)
def test_a_key_condition_compared_with_false_returns_nothing(dynamodb_client, five, condition):
    assert _keys(dynamodb_client, five, condition) == []


@pytest.mark.parametrize(
    ("condition", "keys"),
    [
        ("pk = 'a' = false", []),
        ("(pk = 'a') = false", []),
        ("pk = 'a' = false AND n = 1", []),
        ("pk = 'a' AND sk = 'x' = false", []),
        ("pk = 'a' AND (sk = 'x') = false", []),
        ("pk = 'a' AND sk = 'x' <> true", ["a/x"]),
        ("pk = 'a' <> true", ["a/x", "a/y"]),
        ("pk = 'a' = false OR pk = 'b'", ["b/x"]),
        ("pk = 'b' OR pk = 'a' = false", ["b/x"]),
    ],
)
def test_the_literal_alone_decides_beside_other_disjuncts(dynamodb_client, four, condition, keys):
    assert _keys(dynamodb_client, four, condition) == keys


@pytest.mark.parametrize(
    ("condition", "keys"),
    [
        ("pk = 'a' AND n = 1 <> true", []),
        ("pk = 'a' AND n = 1 = false", []),
        ("pk IN ['a'] <> true", ["b/x", "b/y"]),
        ("pk IN ['a'] = false", ["b/x", "b/y"]),
    ],
)
def test_another_condition_compared_with_a_boolean_keeps_its_meaning(
    dynamodb_client, five, condition, keys
):
    assert _keys(dynamodb_client, five, condition) == keys


def test_a_not_over_a_key_condition_is_a_scan(dynamodb_client, four):
    assert _keys(dynamodb_client, four, "NOT pk = 'a'") == ["b/x", "c/x"]
