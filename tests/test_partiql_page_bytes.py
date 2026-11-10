# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the size of a page of a PartiQL SELECT that
reads several partitions by key.

The disjuncts of a page share one 1 MB budget of the item bytes they read. A
Query of a partition reads until the item that takes the page past 1 MB,
keeps that item, and ends the page there with that item's key as the
LastEvaluatedKey; the next page goes on inside that Query. Whether an empty
page follows a page that ends on the last item of a partition depends on
where the partition sits in the table's storage, so these tests do not assert
what follows such a page.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. The expected values were measured on the
service (2026-11-03), except the read of a partition past 1 MB in
`test_a_partition_past_1_mb_ends_the_page_on_the_item_that_passes_it`, which
follows the rule the other shapes measured.
"""

from __future__ import annotations

import contextlib

import pytest

from conftest import scoped_table

BIG = "x" * (380 * 1024)


@pytest.fixture(scope="module")
def big_table(dynamodb_client):
    """Yield a function that fills a fresh table keyed on string pk and sk
    with items of about 380 KB, `{partition: [sort keys]}`, plus small items
    `small`, and returns its name."""
    with contextlib.ExitStack() as stack:

        def make(big, small=()):
            name = stack.enter_context(
                scoped_table(
                    dynamodb_client,
                    attribute_definitions=[
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"},
                    ],
                    key_schema=[
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"},
                    ],
                )
            )
            for pk, sorts in big.items():
                for sk in sorts:
                    dynamodb_client.put_item(
                        TableName=name,
                        Item={"pk": {"S": pk}, "sk": {"S": sk}, "v": {"S": BIG}},
                    )
            for pk, sk in small:
                dynamodb_client.put_item(TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}})
            return name

        yield make


@pytest.fixture(scope="module")
def one_each(big_table):
    """Partitions a, b and c, each holding one item of about 380 KB."""
    return big_table({"a": ["1"], "b": ["1"], "c": ["1"]})


@pytest.fixture(scope="module")
def two_each(big_table):
    """Partitions a and b, each holding two items of about 380 KB."""
    return big_table({"a": ["1", "2"], "b": ["1", "2"]})


@pytest.fixture(scope="module")
def four_in_a(big_table):
    """Partition a holding four items of about 380 KB, partition b one small item."""
    return big_table({"a": ["1", "2", "3", "4"]}, small=[("b", "1")])


def _key(item):
    return f"{item['pk']['S']}/{item['sk']['S']}"


def _pages(client, statement):
    """Every page of `statement`, following NextToken: its keys, its
    LastEvaluatedKey (None when it has none) and whether it has a NextToken."""
    pages, token = [], None
    for _ in range(8):
        args = {"NextToken": token} if token else {}
        response = client.execute_statement(Statement=statement, **args)
        token = response.get("NextToken")
        keys = [_key(item) for item in response["Items"]]
        pages.append((keys, response.get("LastEvaluatedKey"), token is not None))
        if not token:
            break
    return pages


def _sk(pk, sk):
    return {"pk": {"S": pk}, "sk": {"S": sk}}


@pytest.mark.parametrize("projection", ["*", "pk, sk"])
def test_a_page_ends_inside_the_query_that_takes_it_past_1_mb(
    dynamodb_client, two_each, projection
):
    statement = f"SELECT {projection} FROM \"{two_each}\" WHERE pk IN ['a', 'b']"
    assert _pages(dynamodb_client, statement) == [
        (["a/1", "a/2", "b/1"], _sk("b", "1"), True),
        (["b/2"], None, False),
    ]


def test_a_page_within_1_mb_reads_every_partition(dynamodb_client, one_each):
    statement = f"SELECT * FROM \"{one_each}\" WHERE pk IN ['a', 'b']"
    assert _pages(dynamodb_client, statement) == [(["a/1", "b/1"], None, False)]


@pytest.mark.parametrize(
    "condition",
    ["pk IN ['a', 'b', 'c']", "pk = 'a' OR pk = 'b' OR pk = 'c'"],
    ids=["in", "or"],
)
def test_a_page_keeps_the_item_that_takes_it_past_1_mb(dynamodb_client, one_each, condition):
    pages = _pages(dynamodb_client, f'SELECT * FROM "{one_each}" WHERE {condition}')
    # The page ends on the last item of partition c: whether it has a key and
    # whether an empty page follows depend on the table's storage.
    assert pages[0][0] == ["a/1", "b/1", "c/1"]
    assert all(not keys for keys, _, _ in pages[1:])


def test_a_page_of_an_item_and_a_partition_shares_the_budget(dynamodb_client, two_each):
    statement = f"SELECT * FROM \"{two_each}\" WHERE (pk = 'a' AND sk = '1') OR pk = 'b'"
    pages = _pages(dynamodb_client, statement)
    assert pages[0][0] == ["a/1", "b/1", "b/2"]
    assert all(not keys for keys, _, _ in pages[1:])


def test_a_partition_past_1_mb_ends_the_page_on_the_item_that_passes_it(
    dynamodb_client, four_in_a
):
    statement = f"SELECT * FROM \"{four_in_a}\" WHERE pk IN ['a', 'b']"
    assert _pages(dynamodb_client, statement) == [
        (["a/1", "a/2", "a/3"], _sk("a", "3"), True),
        (["a/4", "b/1"], None, False),
    ]
