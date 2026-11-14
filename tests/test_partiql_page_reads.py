# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the pages of a PartiQL SELECT read by key over
many disjuncts.

A page of a SELECT whose every disjunct reads by key reads at most 10 of its
disjuncts, whatever its Limit: the first page in read order (key order, or
the order an ORDER BY gives), later pages in the order that
tests/test_partiql_visit_order.py describes. Within that cap the rules of a
page hold: a disjunct
that names one item takes one unit of Limit whether the item exists or not,
any other disjunct is a Query with the units left. A page has a
LastEvaluatedKey only when it returned Limit items, so a page the cap ends
with fewer has none. There is no NextToken after the page that reads the
last disjunct, and the capacity a page reports is that of its reads.

A disjunct held back for the refusal it gets when it is read takes its
place among the 10 and reads nothing; a request whose page would visit only
held-back disjuncts gets the refusal, so the request after the page that
reads the last of the others does.

An index read takes one Query for each disjunct, also when several name one
partition of the index: each Query is one of the 10 of a page and reports
its own capacity. Whether a page that ends in one of these Queries has a
LastEvaluatedKey, and whether an empty page follows the last one, is the
answer a Query gives at the end of a partition, which differs on the service
by where the partition sits in its storage, so the tests assert neither.

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

HASH_SIZE = (
    "One or more parameter values were invalid: Size of hashkey has exceeded the maximum "
    "size limit of2048 bytes"
)
FILTER_KEY = (
    "Filter Expression can only contain non-primary key attributes: Primary key attribute: sk"
)
BIG = "z" * 2049


def _values(n, prefix="v", start=0):
    """`n` string values `<prefix>000`, `<prefix>001`, ... from `start`."""
    return ", ".join(f"'{prefix}{i:03}'" for i in range(start, start + n))


def _keyed_table(client):
    """A table keyed on string pk and sk."""
    return scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    )


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A table keyed on a string pk holding v000 to v029."""
    with scoped_table(dynamodb_client) as name:
        for i in range(30):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": f"v{i:03}"}})
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on string pk and sk holding v000/s to v029/s, each with
    a = 's'."""
    with _keyed_table(dynamodb_client) as name:
        for i in range(30):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"v{i:03}"}, "sk": {"S": "s"}, "a": {"S": "s"}},
            )
        yield name


@pytest.fixture(scope="module")
def c3table(dynamodb_client):
    """A table keyed on string pk and sk holding partitions v000 to v010 of
    three items each, s0, s1 and s2."""
    with _keyed_table(dynamodb_client) as name:
        for i in range(11):
            for j in range(3):
                dynamodb_client.put_item(
                    TableName=name, Item={"pk": {"S": f"v{i:03}"}, "sk": {"S": f"s{j}"}}
                )
        yield name


def _indexed_table(client, *sort):
    """A table keyed on a string pk with a GSI gix keyed on a string g and,
    when `sort` names it, a string sort key."""
    schema = [{"AttributeName": "g", "KeyType": "HASH"}]
    schema += [{"AttributeName": name, "KeyType": "RANGE"} for name in sort]
    return scoped_table(
        client,
        attribute_definitions=[
            {"AttributeName": name, "AttributeType": "S"} for name in ("pk", "g", *sort)
        ],
        GlobalSecondaryIndexes=[
            {"IndexName": "gix", "KeySchema": schema, "Projection": {"ProjectionType": "ALL"}}
        ],
    )


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
def gtable(dynamodb_client):
    """A table with a GSI gix keyed on g, holding p000 to p029 with g v000 to
    v029."""
    with _indexed_table(dynamodb_client) as name:
        for i in range(30):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"p{i:03}"}, "g": {"S": f"v{i:03}"}}
            )
        _wait_for_index(dynamodb_client, name, 30)
        yield name


@pytest.fixture(scope="module")
def g2table(dynamodb_client):
    """A table with a GSI gix keyed on g and gs, holding p000 to p014 in index
    partition a with gs w000 to w014."""
    with _indexed_table(dynamodb_client, "gs") as name:
        for i in range(15):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"p{i:03}"}, "g": {"S": "a"}, "gs": {"S": f"w{i:03}"}},
            )
        _wait_for_index(dynamodb_client, name, 15)
        yield name


@pytest.fixture(scope="module")
def xyz(dynamodb_client):
    """A table with a GSI gix keyed on g and gs, holding p1 (a/x), p2 (a/y)
    and p3 (a/z)."""
    with _indexed_table(dynamodb_client, "gs") as name:
        for pk, gs in [("p1", "x"), ("p2", "y"), ("p3", "z")]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": pk}, "g": {"S": "a"}, "gs": {"S": gs}}
            )
        _wait_for_index(dynamodb_client, name, 3)
        yield name


def _key(item):
    return "/".join(item[k]["S"] for k in ("pk", "sk") if k in item)


def _gs(item):
    return item["gs"]["S"]


def _read(client, statement, key=_key, **kwargs):
    """Every page of `statement`, following NextToken: the `key` of each of
    its items in order, its LastEvaluatedKey (None when it has none), whether
    it has a NextToken and the capacity it reports; and the message of the
    ValidationException that ends the paging when there is one."""
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
        token = response.get("NextToken")
        capacity = response.get("ConsumedCapacity", {}).get("CapacityUnits")
        keys = [key(item) for item in response["Items"]]
        pages.append((keys, response.get("LastEvaluatedKey"), token is not None, capacity))
        if not token:
            break
    return pages


def _sizes(client, statement, **kwargs):
    """How many items each page of `statement` holds, and the refusal that
    ends the paging when there is one."""
    return [
        page if isinstance(page, str) else len(page[0])
        for page in _read(client, statement, **kwargs)
    ]


def _sorted_keys(client, statement, **kwargs):
    """The sorted keys of each page of `statement`, and the refusal that ends
    the paging when there is one."""
    return [
        page if isinstance(page, str) else sorted(page[0])
        for page in _read(client, statement, **kwargs)
    ]


# ---- the cap of 10 disjuncts a page ------------------------------------------


@pytest.mark.parametrize(
    ("count", "sizes"),
    [
        (4, [4]),
        (9, [9]),
        (10, [10]),
        (11, [10, 1]),
        (12, [10, 2]),
        (15, [10, 5]),
        (20, [10, 10]),
        (24, [10, 10, 4]),
        (25, [10, 10, 5]),
        (30, [10, 10, 10]),
        (50, [10, 10, 10, 0, 0]),
    ],
)
def test_a_page_reads_at_most_10_items_by_key(dynamodb_client, htable, count, sizes):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_values(count)}]'
    assert _sizes(dynamodb_client, statement) == sizes


@pytest.mark.parametrize(("count", "sizes"), [(10, [0]), (11, [0, 0]), (25, [0, 0, 0])])
def test_a_key_with_no_item_takes_its_place_in_a_page(dynamodb_client, htable, count, sizes):
    statement = f"SELECT * FROM \"{htable}\" WHERE pk IN [{_values(count, 'zz')}]"
    assert _sizes(dynamodb_client, statement) == sizes


@pytest.mark.parametrize(("count", "sizes"), [(11, [10, 1]), (25, [10, 10, 5])])
def test_an_or_of_keys_pages_as_an_in_list(dynamodb_client, htable, count, sizes):
    condition = " OR ".join(f"pk = 'v{i:03}'" for i in range(count))
    assert _sizes(dynamodb_client, f'SELECT * FROM "{htable}" WHERE {condition}') == sizes


@pytest.mark.parametrize(
    ("count", "limit", "sizes"),
    [
        (25, 100, [10, 10, 5]),
        (25, 15, [10, 10, 5]),
        (25, 11, [10, 10, 5]),
        (25, 10, [10, 10, 5]),
        (25, 7, [7, 7, 7, 4]),
        (21, 10, [10, 10, 1]),
    ],
)
def test_the_cap_holds_whatever_the_limit(dynamodb_client, htable, count, limit, sizes):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_values(count)}]'
    assert _sizes(dynamodb_client, statement, Limit=limit) == sizes


@pytest.mark.parametrize(
    ("limit", "last_keys"),
    [
        (None, [None, None]),
        (15, [None, None, None]),
        (11, [None, None, None]),
        (10, [{"pk": {"S": "v009"}}, {"pk": {"S": "v019"}}, None]),
    ],
    ids=["no-limit", "limit-15", "limit-11", "limit-10"],
)
def test_a_page_has_a_last_key_only_when_it_holds_limit_items(
    dynamodb_client, htable, limit, last_keys
):
    count = 11 if limit is None else 25
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_values(count)}]'
    kwargs = {"Limit": limit} if limit else {}
    pages = _read(dynamodb_client, statement, **kwargs)
    assert [lek for _, lek, _, _ in pages] == last_keys
    assert [more for _, _, more, _ in pages] == [True] * (len(last_keys) - 1) + [False]


def test_a_page_reports_the_capacity_of_its_reads(dynamodb_client, htable):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_values(11)}]'
    pages = _read(dynamodb_client, statement, ReturnConsumedCapacity="TOTAL")
    assert [(len(keys), capacity) for keys, _, _, capacity in pages] == [(10, 5.0), (1, 0.5)]


def test_an_order_by_pages_in_its_order(dynamodb_client, htable):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_values(25)}] ORDER BY pk DESC'
    pages = _read(dynamodb_client, statement)
    assert [keys for keys, _, _, _ in pages] == [
        [f"v{i:03}" for i in range(24, 14, -1)],
        [f"v{i:03}" for i in range(14, 4, -1)],
        [f"v{i:03}" for i in range(4, -1, -1)],
    ]


@pytest.mark.parametrize(
    ("condition", "sizes"),
    [
        (f"pk IN [{_values(11)}]", [10, 1]),
        (f"pk IN [{_values(11)}] AND sk = 's'", [10, 1]),
        (f"pk IN [{_values(25)}]", [10, 10, 5]),
        (f"pk IN [{_values(25)}] AND sk = 's'", [10, 10, 5]),
    ],
    ids=["partitions-11", "items-11", "partitions-25", "items-25"],
)
def test_a_partition_read_is_one_of_the_10(dynamodb_client, ctable, condition, sizes):
    assert _sizes(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}') == sizes


def test_items_and_partitions_share_the_10(dynamodb_client, ctable):
    condition = f"(pk = 'v000' AND sk = 's') OR pk IN [{_values(10, start=1)}]"
    pages = _read(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}')
    assert [keys for keys, _, _, _ in pages] == [
        [f"v{i:03}/s" for i in range(10)],
        ["v010/s"],
    ]


@pytest.mark.parametrize(
    ("limit", "sizes"), [(None, [30, 3]), (5, [5, 5, 5, 5, 5, 5, 3])], ids=["no-limit", "limit-5"]
)
def test_a_page_reads_10_partitions_of_several_items(dynamodb_client, c3table, limit, sizes):
    statement = f'SELECT * FROM "{c3table}" WHERE pk IN [{_values(11)}]'
    kwargs = {"Limit": limit} if limit else {}
    assert _sizes(dynamodb_client, statement, **kwargs) == sizes


def test_sort_key_values_of_one_partition_take_a_place_each(dynamodb_client, c3table):
    statement = f"SELECT * FROM \"{c3table}\" WHERE pk = 'v000' AND sk IN [{_values(11, 's')}]"
    assert _sizes(dynamodb_client, statement) == [0, 0]


# ---- a disjunct held back for a refusal --------------------------------------


@pytest.mark.parametrize(
    ("held_back", "limit"),
    [(f"v005{BIG}", None), (f"v005{BIG}", 15), (f"v000{BIG}", None)],
    ids=["after-v005", "after-v005-limit-15", "after-v000"],
)
def test_a_held_back_key_takes_its_place_in_a_page(dynamodb_client, htable, held_back, limit):
    statement = f"SELECT * FROM \"{htable}\" WHERE pk IN [{_values(11)}, '{held_back}']"
    kwargs = {"Limit": limit} if limit else {}
    assert _sorted_keys(dynamodb_client, statement, **kwargs) == [
        [f"v{i:03}" for i in range(9)],
        ["v009", "v010"],
        HASH_SIZE,
    ]


def test_a_held_back_key_read_last_is_refused_after_the_last_page(dynamodb_client, htable):
    statement = f"SELECT * FROM \"{htable}\" WHERE pk IN [{_values(49)}, '{BIG}']"
    assert _sizes(dynamodb_client, statement) == [10, 10, 10, 0, 0, HASH_SIZE]


def test_a_held_back_filter_takes_its_place_in_a_page(dynamodb_client, ctable):
    others = ", ".join(f"'v{i:03}'" for i in range(12) if i != 3)
    condition = f"(pk = 'v003' AND sk = a) OR pk IN [{others}]"
    pages = _sorted_keys(dynamodb_client, f'SELECT * FROM "{ctable}" WHERE {condition}')
    assert pages == [
        [f"v{i:03}/s" for i in range(10) if i != 3],
        ["v010/s", "v011/s"],
        FILTER_KEY,
    ]


# ---- an index read by several disjuncts --------------------------------------


@pytest.mark.parametrize(("count", "sizes"), [(11, [10, 1]), (25, [10, 10, 5])])
def test_an_index_page_reads_at_most_10_partitions(dynamodb_client, gtable, count, sizes):
    statement = f'SELECT * FROM "{gtable}"."gix" WHERE g IN [{_values(count)}]'
    assert _sizes(dynamodb_client, statement) == sizes


@pytest.mark.parametrize(
    ("condition", "keys", "capacity"),
    [
        ("g = 'a' AND gs IN ['w000', 'qqq']", ["w000"], 1.0),
        ("g = 'a' AND gs IN ['w000', 'w001', 'w002']", ["w000", "w001", "w002"], 1.5),
        ("(g = 'a' AND gs = 'w000') OR (g = 'a' AND gs = 'w001')", ["w000", "w001"], 1.0),
        ("g = 'a' AND (gs = 'w000' OR gs > 'w012')", ["w000", "w013", "w014"], 1.0),
        ("g = 'a' AND gs IN ['w000', 'w001'] AND pk = 'zz'", [], 1.0),
        # One key condition is one Query.
        ("g = 'a'", [f"w{i:03}" for i in range(15)], 0.5),
        ("g = 'a' AND gs > 'w005'", [f"w{i:03}" for i in range(6, 15)], 0.5),
    ],
    ids=["in-two", "in-three", "or", "equality-or-range", "filtered", "partition", "range"],
)
def test_each_disjunct_of_an_index_partition_is_a_query_of_its_own(
    dynamodb_client, g2table, condition, keys, capacity
):
    statement = f'SELECT * FROM "{g2table}"."gix" WHERE {condition}'
    pages = _read(dynamodb_client, statement, key=_gs, ReturnConsumedCapacity="TOTAL")
    assert [(found, more, units) for found, _, more, units in pages] == [(keys, False, capacity)]


def test_each_query_of_an_index_partition_is_one_of_the_10(dynamodb_client, g2table):
    statement = (
        f"SELECT * FROM \"{g2table}\".\"gix\" WHERE g = 'a' AND gs IN [{_values(11, 'w')}]"
    )
    pages = _read(dynamodb_client, statement, key=_gs, ReturnConsumedCapacity="TOTAL")
    assert [(found, more, units) for found, _, more, units in pages] == [
        ([f"w{i:03}" for i in range(10)], True, 5.0),
        (["w010"], False, 0.5),
    ]


def test_a_held_back_value_takes_its_place_on_an_index(dynamodb_client, gtable):
    statement = (
        f"SELECT pk FROM \"{gtable}\".\"gix\" WHERE g IN [{_values(11)}, 'v005{BIG}']"
    )
    pages = _read(dynamodb_client, statement, key=lambda item: item["pk"]["S"])
    assert [page if isinstance(page, str) else sorted(page[0]) for page in pages] == [
        [f"p{i:03}" for i in range(9)],
        ["p009", "p010"],
        HASH_SIZE,
    ]


@pytest.mark.parametrize(
    ("condition", "first_pages"),
    [
        ("g = 'a' AND gs IN ['x', 'z']", [["x"], ["z"]]),
        ("(g = 'a' AND gs = 'x') OR (g = 'a' AND gs > 'y')", [["x"], ["z"]]),
        ("g = 'a' AND gs IN ['x', 'z'] ORDER BY gs DESC", [["z"], ["x"]]),
        ("g = 'a' AND gs IN ['x', 'z'] AND pk = 'p3'", [[], ["z"]]),
    ],
    ids=["in", "or", "descending", "filtered"],
)
def test_queries_of_one_index_partition_take_the_limit_in_turn(
    dynamodb_client, xyz, condition, first_pages
):
    statement = f'SELECT * FROM "{xyz}"."gix" WHERE {condition}'
    pages = _read(dynamodb_client, statement, key=_gs, Limit=1)
    # The second page ends in the Query of the last disjunct: whether an
    # empty page follows depends on where the partition sits in storage.
    assert [found for found, _, _, _ in pages[:2]] == first_pages
    assert pages[0][2] is True
    assert all(not found for found, _, _, _ in pages[2:])


@pytest.mark.parametrize(
    ("condition", "keys"),
    [
        ("g = 'a' AND gs IN ['x', 'z']", ["x", "z"]),
        ("(g = 'a' AND gs = 'x') OR (g = 'a' AND gs > 'y')", ["x", "z"]),
        ("g = 'a' AND gs IN ['x', 'z'] ORDER BY gs DESC", ["z", "x"]),
        ("g = 'a' AND gs IN ['x', 'z'] AND pk = 'p3'", ["z"]),
    ],
    ids=["in", "or", "descending", "filtered"],
)
def test_queries_of_one_index_partition_add_up_their_capacity(
    dynamodb_client, xyz, condition, keys
):
    statement = f'SELECT * FROM "{xyz}"."gix" WHERE {condition}'
    pages = _read(dynamodb_client, statement, key=_gs, ReturnConsumedCapacity="TOTAL")
    assert [(found, more, units) for found, _, more, units in pages] == [(keys, False, 1.0)]
