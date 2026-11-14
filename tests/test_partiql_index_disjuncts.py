# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for a PartiQL read of an index by several
disjuncts, one of which fails a check made when it is read.

The service reads the other disjuncts first and refuses the request with the
NextToken of their last page, on an index as on a table (measured
2026-11-03: `g IN ['a', <2049 bytes>]` on a GSI keyed on g gives the items of
partition a and a NextToken, and the request with that token gets the hash
key size refusal). Beside a long filter, 50 disjuncts read 10 a page, the
held-back one in its place, and more than 50 are refused on the first
request, the held-back one among them (measured 2026-11-03).

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

HASH_SIZE = (
    "One or more parameter values were invalid: Size of hashkey has exceeded the maximum "
    "size limit of2048 bytes"
)
TOO_MANY = "Too many decomposed read operations for a given query."
BIG_HASH = "z" * 2049
FILTER = [f"x{i:02}" for i in range(1, 100)]


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """A table keyed on pk with a GSI gix keyed on g. p1 (g a00), p2 (g a50)
    and p3 (g a98) sit in partitions the statements read, p4 (g b) in one
    they do not; p3 lacks x05."""
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
        ones = {attribute: {"N": "1"} for attribute in FILTER}
        for pk, g in [("p1", "a00"), ("p2", "a50"), ("p3", "a98"), ("p4", "b")]:
            item = {"pk": {"S": pk}, "g": {"S": g}, **ones}
            if pk == "p3":
                del item["x05"]
            dynamodb_client.put_item(TableName=name, Item=item)
        for _ in range(240):
            scan = dynamodb_client.scan(TableName=name, IndexName="gix", Select="COUNT")
            if scan["Count"] == 4:
                break
            time.sleep(0.5)
        yield name


def _pages(client, statement):
    """The sorted keys of every page of `statement`, following NextToken, and
    the refusal that ends the paging when there is one."""
    pages, token = [], None
    for _ in range(8):
        args = {"NextToken": token} if token else {}
        try:
            response = client.execute_statement(Statement=statement, **args)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ValidationException", e.response
            pages.append(e.response["Error"]["Message"])
            break
        pages.append(sorted(item["pk"]["S"] for item in response["Items"]))
        token = response.get("NextToken")
        if not token:
            break
    return pages


def test_an_oversized_disjunct_on_an_index_is_refused_after_the_others(dynamodb_client, gtable):
    statement = f"SELECT pk FROM \"{gtable}\".\"gix\" WHERE g IN ['a00', '{BIG_HASH}']"
    assert _pages(dynamodb_client, statement) == [["p1"], HASH_SIZE]


@pytest.mark.parametrize(
    ("held_back", "pages"),
    [
        (True, [["p1"], [], [], [], ["p2"], HASH_SIZE]),
        (False, [["p1"], [], [], [], ["p2"]]),
    ],
    ids=["50-disjuncts", "49-disjuncts"],
)
def test_50_disjuncts_beside_a_long_filter_on_an_index_read_10_a_page(
    dynamodb_client, gtable, held_back, pages
):
    # a00 to a46, then the partitions of p2 and p3: p3 lacks x05.
    values = [f"'a{i:02}'" for i in range(47)] + ["'a50'", "'a98'"]
    if held_back:
        values.append(f"'{BIG_HASH}'")
    condition = " AND ".join(f"{attribute} = 1" for attribute in FILTER)
    statement = (
        f"SELECT pk FROM \"{gtable}\".\"gix\" WHERE g IN [{', '.join(values)}] AND {condition}"
    )
    assert _pages(dynamodb_client, statement) == pages


@pytest.mark.parametrize("count", [99, 50], ids=["100-disjuncts", "51-disjuncts"])
def test_more_than_50_disjuncts_beside_a_long_filter_on_an_index_are_refused(
    dynamodb_client, gtable, count
):
    values = ", ".join(f"'a{i:02}'" for i in range(count))
    condition = " AND ".join(f"{attribute} = 1" for attribute in FILTER)
    statement = (
        f"SELECT pk FROM \"{gtable}\".\"gix\" WHERE g IN [{values}, '{BIG_HASH}'] AND {condition}"
    )
    assert _pages(dynamodb_client, statement) == [TOO_MANY]
