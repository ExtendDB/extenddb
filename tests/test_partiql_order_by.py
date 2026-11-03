# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for ORDER BY in a PartiQL SELECT.

Every path in the list must be a key attribute of what is read, the table's
key or the index's; the statement must have a WHERE, and the WHERE must read
by key. Over more than one partition key value the list must name the
partition key somewhere. Each path takes ASC or DESC.

The partitions ascend unless the path that names the partition key says
DESC. Within a partition the items follow the path that names the sort key,
and descend when no path names it: `ORDER BY pk` reads the items of a
partition in descending order.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-03).
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "
NO_WHERE = "Must have WHERE clause in the statement when using ORDER BY clause."
NOT_KEYED = (
    "Must have at least one non-optional hash key condition in WHERE clause when using "
    "ORDER BY clause."
)
HASH_KEY = (
    "Must have hash key in ORDER BY clause when more than one hash key condition specified in "
    "WHERE clause."
)
OVERLAP = "Overlapping conditions with range keys are not supported in where clause"


def _attribute(name):
    return f"Attribute {name} in ORDER BY clause must be part of the primary key"


def _keyed_table(client, sort_type="S"):
    """A table keyed on a string pk and a sort key sk of type `sort_type`."""
    return scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": sort_type},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    )


@pytest.fixture(scope="module")
def five(dynamodb_client):
    """Items a/x, a/y, a/z, b/x and b/y, each with n = 1 and m = 'x'."""
    with _keyed_table(dynamodb_client) as name:
        for pk, sk in [("a", "x"), ("a", "y"), ("a", "z"), ("b", "x"), ("b", "y")]:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": "1"}, "m": {"S": "x"}},
            )
        yield name


@pytest.fixture(scope="module")
def four(dynamodb_client):
    """Items a/x, a/y, b/x and c/x, each with n = 1."""
    with _keyed_table(dynamodb_client) as name:
        for pk, sk in [("a", "x"), ("a", "y"), ("b", "x"), ("c", "x")]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": "1"}}
            )
        yield name


@pytest.fixture(scope="module")
def numbers(dynamodb_client):
    """Partition a with number sort keys 1, 2 and 10."""
    with _keyed_table(dynamodb_client, sort_type="N") as name:
        for sk in ["1", "2", "10"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "sk": {"N": sk}})
        yield name


@pytest.fixture(scope="module")
def hashed(dynamodb_client):
    """A table keyed on pk alone holding a, b and c, each with n = 1."""
    with scoped_table(dynamodb_client) as name:
        for pk in ["a", "b", "c"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": pk}, "n": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def indexed(dynamodb_client):
    """A table keyed on pk with a GSI gix keyed on g and gs, holding p1 (a/x),
    p2 (a/y) and p3 (a/z)."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": n, "AttributeType": "S"} for n in ("pk", "g", "gs")
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
        for pk, gs in [("p1", "x"), ("p2", "y"), ("p3", "z")]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "g": {"S": "a"}, "gs": {"S": gs}}
            )
        for _ in range(240):
            count = dynamodb_client.scan(TableName=name, IndexName="gix", Select="COUNT")["Count"]
            if count == 3:
                break
            time.sleep(0.5)
        yield name


def _key(item):
    if "gs" in item:
        return item["pk"]["S"]
    sort = item.get("sk")
    return item["pk"]["S"] + ("" if sort is None else "/" + (sort.get("S") or sort.get("N")))


def _read(client, statement, **kwargs):
    """The keys every page of `statement` holds, in order, following
    NextToken, or the refusal's message."""
    keys, token = [], None
    for _ in range(12):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        try:
            response = client.execute_statement(Statement=statement, **args)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ValidationException", e.response
            return e.response["Error"]["Message"]
        keys += [_key(item) for item in response["Items"]]
        token = response.get("NextToken")
        if not token:
            break
    return keys


DOWN = ["a/z", "a/y", "a/x"]
UP = ["a/x", "a/y", "a/z"]


@pytest.mark.parametrize("where", ["pk = 'a'", "pk = 'a' AND sk > 'a'", "pk = 'a' AND n = 1"])
@pytest.mark.parametrize(
    ("order", "keys"),
    [
        ("pk", DOWN),
        ("pk ASC", DOWN),
        ("pk DESC", DOWN),
        ("sk", UP),
        ("sk DESC", DOWN),
        ("pk, sk", UP),
        ("pk, pk", DOWN),
        ("sk, sk", UP),
        ('"pk"', DOWN),
        ("n", _attribute("n")),
        ("PK", _attribute("PK")),
    ],
)
def test_the_items_of_a_partition_follow_the_sort_key_or_descend(
    dynamodb_client, five, where, order, keys
):
    statement = f'SELECT * FROM "{five}" WHERE {where} ORDER BY {order}'
    assert _read(dynamodb_client, statement) == keys


@pytest.mark.parametrize(
    ("where", "order", "keys"),
    [
        ("pk = 'a'", "pk", ["a/10", "a/2", "a/1"]),
        ("pk = 'a'", "sk", ["a/1", "a/2", "a/10"]),
        ("pk = 'a'", "sk DESC", ["a/10", "a/2", "a/1"]),
        ("pk = 'a' AND sk > 0", "pk", ["a/10", "a/2", "a/1"]),
        ("pk = 'a' AND sk > 0", "pk, sk", ["a/1", "a/2", "a/10"]),
        ("pk = 'a' AND sk <> 5", "pk", NOT_KEYED),
        ("pk = 'a' AND sk <> 5", "n", _attribute("n")),
    ],
)
def test_a_number_sort_key_orders_by_value(dynamodb_client, numbers, where, order, keys):
    statement = f'SELECT * FROM "{numbers}" WHERE {where} ORDER BY {order}'
    assert _read(dynamodb_client, statement) == keys


@pytest.mark.parametrize(
    ("order", "keys"),
    [
        ("pk", ["a/z", "a/y", "a/x", "b/y", "b/x"]),
        ("pk ASC", ["a/z", "a/y", "a/x", "b/y", "b/x"]),
        ("sk, pk", ["a/x", "a/y", "a/z", "b/x", "b/y"]),
        ("sk DESC, pk", ["a/z", "a/y", "a/x", "b/y", "b/x"]),
        ("pk DESC, sk DESC", ["b/y", "b/x", "a/z", "a/y", "a/x"]),
    ],
)
def test_partitions_follow_the_partition_key(dynamodb_client, five, order, keys):
    statement = f"SELECT * FROM \"{five}\" WHERE pk IN ['a', 'b'] ORDER BY {order}"
    assert _read(dynamodb_client, statement) == keys


@pytest.mark.parametrize(
    ("order", "keys"),
    [
        ("sk, pk", ["a/x", "a/y", "b/x"]),
        ("pk DESC, sk", ["b/x", "a/x", "a/y"]),
        ("pk ASC, sk", ["a/x", "a/y", "b/x"]),
        ("pk, sk DESC", ["a/y", "a/x", "b/x"]),
        ("pk, sk ASC", ["a/x", "a/y", "b/x"]),
        ("pk", ["a/y", "a/x", "b/x"]),
        ("pk, sk", ["a/x", "a/y", "b/x"]),
        ("sk", HASH_KEY),
        ("pk, n", _attribute("n")),
    ],
)
def test_each_path_takes_a_direction_one_item_a_page(dynamodb_client, four, order, keys):
    statement = f"SELECT * FROM \"{four}\" WHERE pk IN ['a', 'b'] ORDER BY {order}"
    assert _read(dynamodb_client, statement, Limit=1) == keys


def test_a_list_that_ends_in_a_comma_is_an_unexpected_term(dynamodb_client, four):
    statement = f"SELECT * FROM \"{four}\" WHERE pk = 'a' ORDER BY pk,"
    assert _read(dynamodb_client, statement) == WF + "Unexpected term"


@pytest.mark.parametrize(
    ("where", "order", "result"),
    [
        (None, "pk", NO_WHERE),
        ("n = 1", "pk", NOT_KEYED),
        ("n = 1", "sk", NOT_KEYED),
        ("pk = 'a' OR n = 1", "pk", NOT_KEYED),
        ("pk > 'a'", "pk", NOT_KEYED),
        ("pk = 'a' AND sk <> 'x'", "sk", NOT_KEYED),
        ("pk = 'a' AND NOT sk = 'x'", "sk", NOT_KEYED),
        ("pk = 'a' AND contains(sk, 'x')", "sk", NOT_KEYED),
        ("pk = 'a' AND sk IS NOT MISSING", "sk", NOT_KEYED),
        ("pk = 'a' AND size(sk) = 1", "sk", NOT_KEYED),
        ("pk = 'a' AND sk = 'x' AND sk = 'y'", "sk", NOT_KEYED),
        ("pk = 'a' AND sk > 'a' AND sk < 'z'", "sk", NOT_KEYED),
        ("pk IN ['a', 'b'] AND sk <> 'x'", "pk", NOT_KEYED),
        ("pk = 'a' AND sk <> 'x'", "pk", NOT_KEYED),
        ("pk = 'a' AND sk IN ['x', 'y']", "sk", ["a/x", "a/y"]),
        ("pk = 'a' AND (sk = 'x' OR sk = 'y')", "sk", ["a/x", "a/y"]),
        ("pk = 'a' AND m = 'x'", "sk", UP),
        ("pk = 'a' AND NOT m = 'y'", "sk", UP),
        ("pk = 'a' AND (sk = 'x' OR m = 'x')", "sk", OVERLAP),
    ],
)
def test_an_order_by_needs_a_where_that_reads_by_key(dynamodb_client, five, where, order, result):
    clause = f" WHERE {where}" if where else ""
    statement = f'SELECT * FROM "{five}"{clause} ORDER BY {order}'
    assert _read(dynamodb_client, statement) == result


@pytest.mark.parametrize(
    ("where", "order", "result"),
    [
        ("pk = 'a'", "pk", ["a"]),
        ("pk IN ['a', 'b']", "pk", ["a", "b"]),
        ("pk IN ['a', 'b']", "pk DESC", ["b", "a"]),
        ("pk IN ['c', 'a']", "pk", ["a", "c"]),
        ("n = 1", "pk", NOT_KEYED),
        ("pk = 'a' AND n = 1", "pk", ["a"]),
        ("pk = 'a'", "n", _attribute("n")),
    ],
)
def test_an_order_by_on_a_table_keyed_on_its_partition_key(
    dynamodb_client, hashed, where, order, result
):
    statement = f'SELECT * FROM "{hashed}" WHERE {where} ORDER BY {order}'
    assert _read(dynamodb_client, statement) == result


@pytest.mark.parametrize(
    ("order", "result"),
    [
        ("g", ["p3", "p2", "p1"]),
        ("g DESC", ["p3", "p2", "p1"]),
        ("gs", ["p1", "p2", "p3"]),
        ("g, gs", ["p1", "p2", "p3"]),
        ("pk", _attribute("pk")),
    ],
)
def test_an_order_by_on_an_index_takes_the_index_keys(dynamodb_client, indexed, order, result):
    statement = f"SELECT * FROM \"{indexed}\".\"gix\" WHERE g = 'a' ORDER BY {order}"
    assert _read(dynamodb_client, statement) == result
