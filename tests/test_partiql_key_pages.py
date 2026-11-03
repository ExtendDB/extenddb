# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the pages of a PartiQL SELECT over several
disjuncts read by key.

When every disjunct gives the full primary key of the table, the service
reads the items in key order, `Limit` keys a page (a key with no item counts
as one), and returns no NextToken once the last key is read, so a read of one
item never has one. Disjuncts that read a whole partition page as a Query and
Scan do.

A statement mixing reads of whole items and reads of partitions takes its
disjuncts in key order, one after another: a whole item takes one unit of
`Limit`, a partition is a Query with the units left. A page of such a read
that holds `Limit` items has a LastEvaluatedKey, the table key of its last
item, with or without a NextToken; any other page has none. A page of whole
items stops before the item that would take it past 1 MB.

A disjunct that fails a check made when it is read (a key value past its size
limit, or a key attribute compared with a path in its filter) does not fail
the statement while another disjunct is fine: the others are read first, and
the request with the NextToken of their last page gets the refusal of the
failing disjunct last in key order. When every disjunct fails, the first
request is refused.

An ORDER BY of a read of more than one partition key value that does not
name the partition key is refused. ORDER BY takes a list of the keys, and
reads in key order with it.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-02).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

FILTER_KEY = (
    "Filter Expression can only contain non-primary key attributes: Primary key attribute: {}"
)
HASH_SIZE = (
    "One or more parameter values were invalid: Size of hashkey has exceeded the maximum "
    "size limit of2048 bytes"
)
RANGE_SIZE = (
    "One or more parameter values were invalid: Aggregated size of all range keys has "
    "exceeded the size limit of 1024 bytes"
)
BIG_HASH = "z" * 2049
BIG_RANGE = "z" * 1025
ORDER_BY_HASH = (
    "Must have hash key in ORDER BY clause when more than one hash key condition specified in "
    "WHERE clause."
)


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A table keyed on a string pk, holding a, b and c, each with n = 1."""
    with scoped_table(dynamodb_client) as name:
        for key in ["a", "b", "c"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": key}, "n": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on string pk and sk, holding a/x, a/y, b/x and c/x."""
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
        for pk, sk in [("a", "x"), ("a", "y"), ("b", "x"), ("c", "x")]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": "1"}}
            )
        yield name


@pytest.fixture(scope="module")
def qtable(dynamodb_client):
    """A table keyed on string pk and sk, holding a/x, a/y, a/z and b/x."""
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
        for pk, sk in [("a", "x"), ("a", "y"), ("a", "z"), ("b", "x")]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "sk": {"S": sk}, "n": {"N": "1"}}
            )
        yield name


@pytest.fixture(scope="module")
def bigtable(dynamodb_client):
    """A table keyed on a string pk, holding a, b and c of about 380 KB each,
    and a small item d."""
    with scoped_table(dynamodb_client) as name:
        for key in ["a", "b", "c"]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": key}, "blob": {"S": "q" * 380_000}}
            )
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "d"}})
        yield name


@pytest.fixture(scope="module")
def ntable(dynamodb_client):
    """A table keyed on a number pk, holding 9, 10 and 100."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[{"AttributeName": "pk", "AttributeType": "N"}],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    ) as name:
        for key in ["9", "10", "100"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"N": key}})
        yield name


def _key(item):
    return "/".join(item[k]["S"] for k in ("pk", "sk") if k in item)


def _last_keys(client, statement, **kwargs):
    """Every page of `statement`, following NextToken: its items, its
    LastEvaluatedKey (None when it has none) and whether it has a NextToken."""
    pages, token = [], None
    for _ in range(12):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        response = client.execute_statement(Statement=statement, **args)
        token = response.get("NextToken")
        pages.append((response["Items"], response.get("LastEvaluatedKey"), token is not None))
        if not token:
            break
    return pages


def _s(**key):
    return {name: {"S": value} for name, value in key.items()}


class _KeyOrNone:
    """The LastEvaluatedKey of a page that ends on the last item of a
    partition, read by a Query that stopped at Limit. The service returns the
    item's key for some partitions and none for others, by where the partition
    sits in the table's storage (measured 2026-11-02), so either is accepted."""

    def __init__(self, key):
        self.key = key

    def __eq__(self, other):
        return other is None or other == self.key

    def __repr__(self):
        return f"{self.key!r} or None"


def _pages(client, table, condition, ordered=True, **kwargs):
    """Every page of a SELECT on `table`, following NextToken: the keys of each
    page (sorted unless `ordered`), and the refusal that ends the paging when
    there is one. Every page but the last carries a NextToken."""
    statement = f'SELECT * FROM "{table}" WHERE {condition}'
    pages, token = [], None
    for _ in range(12):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        try:
            response = client.execute_statement(Statement=statement, **args)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ValidationException", e.response
            pages.append(e.response["Error"]["Message"])
            break
        keys = [_key(item) for item in response["Items"]]
        pages.append(keys if ordered else sorted(keys))
        token = response.get("NextToken")
        if not token:
            break
    return pages


# ---- whole items, in key order -----------------------------------------------


@pytest.mark.parametrize(
    ("condition", "limit", "pages"),
    [
        ("pk IN ['c', 'a', 'b']", 1, [["a"], ["b"], ["c"]]),
        ("pk IN ['c', 'a', 'b']", 2, [["a", "b"], ["c"]]),
        ("pk IN ['c', 'a', 'b']", None, [["a", "b", "c"]]),
        ("pk = 'c' OR pk = 'a' OR pk = 'b'", 1, [["a"], ["b"], ["c"]]),
        ("pk IN ['a', 'b']", 1, [["a"], ["b"]]),
        ("pk IN ['a', 'zz']", 1, [["a"], []]),
        ("pk IN ['zz', 'a']", 1, [["a"], []]),
        ("pk = 'a'", 1, [["a"]]),
    ],
)
def test_whole_items_are_read_in_key_order_limit_keys_a_page(
    dynamodb_client, htable, condition, limit, pages
):
    kwargs = {"Limit": limit} if limit else {}
    assert _pages(dynamodb_client, htable, condition, **kwargs) == pages


@pytest.mark.parametrize(
    ("condition", "limit", "pages"),
    [
        ("(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk = 'x')", 1, [["a/x"], ["b/x"]]),
        ("(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk = 'x')", None, [["a/x", "b/x"]]),
        ("(pk = 'a' AND sk = 'x') OR (pk = 'c' AND sk = 'x')", 1, [["a/x"], ["c/x"]]),
        ("pk = 'a' AND sk = 'x'", 1, [["a/x"]]),
    ],
)
def test_whole_items_by_partition_and_sort_key_are_read_in_key_order(
    dynamodb_client, ctable, condition, limit, pages
):
    kwargs = {"Limit": limit} if limit else {}
    assert _pages(dynamodb_client, ctable, condition, **kwargs) == pages


def test_a_partition_read_pages_as_a_query(dynamodb_client, ctable):
    # The Query stops at Limit on the partition's last item: whether an empty
    # page follows depends on where the partition sits in the table's storage.
    pages = _pages(dynamodb_client, ctable, "pk = 'a'", Limit=2)
    assert pages in ([["a/x", "a/y"]], [["a/x", "a/y"], []])


@pytest.mark.parametrize(
    ("condition", "limit", "pages"),
    [
        (
            "(pk = 'a' AND sk = 'y') OR pk = 'b' OR (pk = 'c' AND sk = 'x')",
            1,
            [["a/y"], ["b/x"], ["c/x"]],
        ),
        (
            "(pk = 'a' AND sk = 'y') OR pk = 'b' OR (pk = 'c' AND sk = 'x')",
            None,
            [["a/y", "b/x", "c/x"]],
        ),
        ("(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk > 'a')", 1, [["a/x"], ["a/y"], ["b/x"]]),
        ("(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk > 'a')", None, [["a/x", "a/y", "b/x"]]),
        ("(pk = 'a' AND sk = 'x') OR pk = 'b'", None, [["a/x", "b/x"]]),
        ("pk = 'b' OR (pk = 'a' AND sk = 'x')", None, [["a/x", "b/x"]]),
    ],
)
def test_reads_of_items_and_partitions_are_taken_in_key_order(
    dynamodb_client, ctable, condition, limit, pages
):
    kwargs = {"Limit": limit} if limit else {}
    assert _pages(dynamodb_client, ctable, condition, **kwargs) == pages


def test_reads_of_several_partitions_page_one_item_at_a_time(dynamodb_client, ctable):
    pages = _pages(dynamodb_client, ctable, "pk IN ['c', 'a', 'b']", Limit=1)
    # Whether an empty page follows the last item depends on where partition
    # 'c' sits in the table's storage.
    assert [len(page) for page in pages] in ([1, 1, 1, 1], [1, 1, 1, 1, 0])
    assert sorted(key for page in pages for key in page) == ["a/x", "a/y", "b/x", "c/x"]
    assert _pages(dynamodb_client, ctable, "pk IN ['c', 'a', 'b']", ordered=False) == [
        ["a/x", "a/y", "b/x", "c/x"]
    ]


# ---- the LastEvaluatedKey of a page ------------------------------------------


@pytest.mark.parametrize(
    ("condition", "limit", "pages"),
    [
        ("pk IN ['a', 'b']", 1, [(["a"], _s(pk="a"), True), (["b"], _s(pk="b"), False)]),
        ("pk IN ['a', 'zz']", 1, [(["a"], _s(pk="a"), True), ([], None, False)]),
        ("pk IN ['zz', 'a']", 1, [(["a"], _s(pk="a"), True), ([], None, False)]),
        ("pk = 'a'", 1, [(["a"], _s(pk="a"), False)]),
        (
            "pk IN ['a', 'b', 'c']",
            2,
            [(["a", "b"], _s(pk="b"), True), (["c"], None, False)],
        ),
        ("pk IN ['a', 'b']", None, [(["a", "b"], None, False)]),
        ("pk IN ['a', 'b'] AND n = 2", 1, [([], None, True), ([], None, False)]),
    ],
    ids=["two", "missing-second", "missing-first", "one", "limit-two", "no-limit", "filtered"],
)
def test_a_page_of_whole_items_holding_limit_items_has_the_last_key(
    dynamodb_client, htable, condition, limit, pages
):
    kwargs = {"Limit": limit} if limit else {}
    statement = f'SELECT * FROM "{htable}" WHERE {condition}'
    got = _last_keys(dynamodb_client, statement, **kwargs)
    assert [([_key(i) for i in items], lek, more) for items, lek, more in got] == pages


@pytest.mark.parametrize(
    ("condition", "pages"),
    [
        (
            "(pk = 'b' AND sk = 'x') OR (pk = 'a' AND sk > 'a')",
            [
                (["a/x"], _s(pk="a", sk="x"), True),
                (["a/y"], _KeyOrNone(_s(pk="a", sk="y")), True),
                (["b/x"], _s(pk="b", sk="x"), False),
            ],
        ),
        (
            "(pk = 'a' AND sk = 'x') OR (pk = 'b' AND sk = 'x')",
            [(["a/x"], _s(pk="a", sk="x"), True), (["b/x"], _s(pk="b", sk="x"), False)],
        ),
        (
            "pk = 'a' AND sk IN ['x', 'y'] ORDER BY sk DESC",
            [(["a/y"], _s(pk="a", sk="y"), True), (["a/x"], _s(pk="a", sk="x"), False)],
        ),
    ],
    ids=["item-and-partition", "two-items", "descending"],
)
def test_a_page_of_items_and_partitions_holding_limit_items_has_the_last_key(
    dynamodb_client, ctable, condition, pages
):
    statement = f'SELECT * FROM "{ctable}" WHERE {condition}'
    got = _last_keys(dynamodb_client, statement, Limit=1)
    assert [([_key(i) for i in items], lek, more) for items, lek, more in got] == pages


def test_a_page_that_ends_in_a_partition_read_takes_its_last_key(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE (pk = 'a' AND sk = 'x') OR pk = 'b'"
    got = _last_keys(dynamodb_client, statement, Limit=1)
    # The second page ends on the last item of partition 'b': its key, and
    # whether an empty page follows, depend on where the partition sits in the
    # table's storage, so only the first two pages are compared.
    assert [([_key(i) for i in items], lek) for items, lek, _ in got[:2]] == [
        (["a/x"], _s(pk="a", sk="x")),
        (["b/x"], _KeyOrNone(_s(pk="b", sk="x"))),
    ]
    assert got[0][2] is True
    assert all(not items for items, _, _ in got[2:])


def test_the_last_key_of_a_page_is_the_key_without_the_projection(dynamodb_client, htable):
    statement = f"SELECT n FROM \"{htable}\" WHERE pk IN ['a', 'b']"
    assert _last_keys(dynamodb_client, statement, Limit=1) == [
        ([{"n": {"N": "1"}}], _s(pk="a"), True),
        ([{"n": {"N": "1"}}], _s(pk="b"), False),
    ]


def test_the_last_key_of_a_page_holds_a_number_key(dynamodb_client, ntable):
    statement = f'SELECT * FROM "{ntable}" WHERE pk IN [10, 9, 100]'
    got = _last_keys(dynamodb_client, statement, Limit=1)
    assert [(lek, more) for _, lek, more in got] == [
        ({"pk": {"N": "9"}}, True),
        ({"pk": {"N": "10"}}, True),
        ({"pk": {"N": "100"}}, False),
    ]


# ---- the size of a page ------------------------------------------------------


def test_a_page_of_whole_items_stops_before_it_passes_1_mb(dynamodb_client, bigtable):
    statement = f"SELECT * FROM \"{bigtable}\" WHERE pk IN ['a', 'b', 'c', 'd']"
    got = _last_keys(dynamodb_client, statement)
    assert [([_key(i) for i in items], lek, more) for items, lek, more in got] == [
        (["a", "b"], None, True),
        (["c", "d"], None, False),
    ]


# ---- ORDER BY over several partitions ----------------------------------------


@pytest.mark.parametrize(
    "condition",
    ["pk IN ['a', 'b']", "(pk = 'a' AND sk = 'x') OR pk = 'b'"],
    ids=["in", "item-and-partition"],
)
def test_an_order_by_over_two_partitions_must_name_the_partition_key(
    dynamodb_client, ctable, condition
):
    assert _pages(dynamodb_client, ctable, f"{condition} ORDER BY sk DESC", Limit=1) == [
        ORDER_BY_HASH
    ]


@pytest.mark.parametrize(
    ("condition", "pages"),
    [
        (
            "pk IN ['a', 'b']",
            [
                (["a/x"], _s(pk="a", sk="x")),
                (["a/y"], _s(pk="a", sk="y")),
                (["a/z"], _KeyOrNone(_s(pk="a", sk="z"))),
                (["b/x"], _KeyOrNone(_s(pk="b", sk="x"))),
            ],
        ),
        (
            "(pk = 'a' AND sk = 'x') OR pk = 'b'",
            [(["a/x"], _s(pk="a", sk="x")), (["b/x"], _KeyOrNone(_s(pk="b", sk="x")))],
        ),
    ],
    ids=["in", "item-and-partition"],
)
def test_an_order_by_list_of_the_keys_reads_in_key_order(
    dynamodb_client, qtable, condition, pages
):
    statement = f'SELECT * FROM "{qtable}" WHERE {condition} ORDER BY pk, sk'
    got = _last_keys(dynamodb_client, statement, Limit=1)
    # Whether an empty page follows the last item depends on where partition
    # 'b' sits in the table's storage, so a page after it is only checked to
    # hold nothing.
    assert [([_key(i) for i in items], lek) for items, lek, _ in got[: len(pages)]] == pages
    assert all(more for _, _, more in got[: len(pages) - 1])
    assert all(not items for items, _, _ in got[len(pages) :])


# ---- a disjunct that fails when it is read -----------------------------------


@pytest.mark.parametrize(
    ("condition", "limit", "pages"),
    [
        (f"pk IN ['a', '{BIG_HASH}']", None, [["a"], HASH_SIZE]),
        (f"pk IN ['{BIG_HASH}', 'a']", None, [["a"], HASH_SIZE]),
        (f"pk = 'a' OR pk = '{BIG_HASH}' OR pk = 'b'", None, [["a", "b"], HASH_SIZE]),
        (f"pk IN ['a', '{BIG_HASH}', 'b']", 1, [["a"], ["b"], HASH_SIZE]),
    ],
    ids=["in", "in-reversed", "or", "limit"],
)
def test_an_oversized_partition_value_is_refused_after_the_other_items(
    dynamodb_client, htable, condition, limit, pages
):
    kwargs = {"Limit": limit} if limit else {}
    assert _pages(dynamodb_client, htable, condition, **kwargs) == pages


@pytest.mark.parametrize(
    ("condition", "pages"),
    [
        (f"pk IN ['a', '{BIG_HASH}']", [["a/x", "a/y"], HASH_SIZE]),
        (
            f"(pk = 'a' AND sk = 'x') OR (pk = 'b' AND sk = '{BIG_RANGE}')",
            [["a/x"], RANGE_SIZE],
        ),
        (f"pk = 'a' OR (pk = 'b' AND sk = '{BIG_RANGE}')", [["a/x", "a/y"], RANGE_SIZE]),
        (f"pk = 'a' AND sk IN ['x', '{BIG_RANGE}']", [["a/x"], RANGE_SIZE]),
    ],
    ids=["partition", "point", "partition-and-point", "sort-in"],
)
def test_an_oversized_key_value_is_refused_after_the_other_disjuncts(
    dynamodb_client, ctable, condition, pages
):
    assert _pages(dynamodb_client, ctable, condition) == pages


@pytest.mark.parametrize(
    ("condition", "pages"),
    [
        ("(pk = 'a' AND sk = a) OR pk = 'b'", [["b/x"], FILTER_KEY.format("sk")]),
        ("pk = 'b' OR (pk = 'a' AND sk = a)", [["b/x"], FILTER_KEY.format("sk")]),
        ("(pk = 'c' AND sk = a) OR pk = 'b'", [["b/x"], FILTER_KEY.format("sk")]),
        (
            "(pk = 'a' AND sk = a) OR (pk = 'b' AND sk = 'x')",
            [["b/x"], FILTER_KEY.format("sk")],
        ),
        (
            "(pk = 'a' AND sk = a) OR pk = 'b' OR pk = 'c'",
            [["b/x", "c/x"], FILTER_KEY.format("sk")],
        ),
        ("(pk = 'b' AND sk = a) OR pk = 'a'", [["a/x", "a/y"], FILTER_KEY.format("sk")]),
        ("(pk = 'a' AND sk = a) OR pk = 'zz'", [[], FILTER_KEY.format("sk")]),
        (f"(pk = 'a' AND sk = a) OR pk = '{BIG_HASH}' OR pk = 'b'", [["b/x"], HASH_SIZE]),
    ],
    ids=[
        "first",
        "last",
        "other-partition",
        "beside-a-point",
        "two-others",
        "valid-partition-sorts-first",
        "nothing-read",
        "with-a-size-refusal",
    ],
)
def test_a_key_attribute_in_one_disjunct_filter_is_refused_after_the_others(
    dynamodb_client, ctable, condition, pages
):
    assert _pages(dynamodb_client, ctable, condition, ordered=False) == pages


def test_a_filter_every_disjunct_shares_is_refused_at_once(dynamodb_client, ctable):
    assert _pages(dynamodb_client, ctable, "pk IN ['a', 'b'] AND sk = a") == [
        FILTER_KEY.format("sk")
    ]


@pytest.mark.parametrize(
    ("condition", "pages"),
    [
        (f"pk = '{BIG_HASH}' OR (pk = 'a' AND sk = a) OR pk = 'b'", [["b/x"], HASH_SIZE]),
        (
            f"(pk = 'b' AND sk = '{BIG_RANGE}') OR pk = '{BIG_HASH}' OR pk = 'a'",
            [["a/x", "a/y"], HASH_SIZE],
        ),
        (
            f"pk = '{BIG_HASH}' OR (pk = 'b' AND sk = '{BIG_RANGE}') OR pk = 'a'",
            [["a/x", "a/y"], HASH_SIZE],
        ),
        (
            "(pk = 'a' AND pk = b) OR (pk = 'b' AND sk = a) OR pk = 'c'",
            [["c/x"], FILTER_KEY.format("sk")],
        ),
        (
            "(pk = 'b' AND sk = a) OR (pk = 'a' AND pk = b) OR pk = 'c'",
            [["c/x"], FILTER_KEY.format("sk")],
        ),
    ],
    ids=["size-last", "two-sizes", "two-sizes-reversed", "two-filters", "two-filters-reversed"],
)
def test_of_several_failing_disjuncts_the_last_in_key_order_is_reported(
    dynamodb_client, ctable, condition, pages
):
    assert _pages(dynamodb_client, ctable, condition) == pages
