# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the order in which the pages of a PartiQL
SELECT read by key visit its disjuncts.

Number the disjuncts 0 to N-1 in read order (key order, or the order an
ORDER BY gives), the ones held back for a refusal included. The first page
takes them in that order. Each later page takes the list the page before it
left and visits it by position modulo a bucket count, in list order within
one bucket; the bucket count is 16, doubled while the list holds more than
three quarters of it. A page visits at most 10 disjuncts and stops when its
Limit is spent. The list it leaves holds the disjuncts it visited and did
not read to their end, in the order it visited them (a disjunct held back
for a refusal, which takes a visit on every page that reaches it, and a
Query the page stopped inside, which goes on from where it stopped), then
the ones it did not visit. The pages return their items in visit order, and
a page with Limit items has the key of its last item as its
LastEvaluatedKey. A request whose page visits only disjuncts held back gets
their refusal instead of a page, also when other disjuncts are left.

The tests write each page as the positions of the items it returns, in
order, and the hash key size refusal that ends a read as "R".

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


def _values(n, prefix="v"):
    """The `n` string values `<prefix>000` to `<prefix><n - 1>`."""
    return [f"{prefix}{i:03}" for i in range(n)]


def _listed(values):
    return ", ".join(f"'{value}'" for value in values)


def _expected(*pages):
    """Pages written as positions, `a..b` for the positions a to b, and "R"
    for the refusal."""
    out = []
    for page in pages:
        if page == "R":
            out.append("R")
            continue
        positions = []
        for token in page.split():
            low, _, high = token.partition("..")
            positions.extend(range(int(low), int(high or low) + 1))
        out.append(positions)
    return out


def _number(attribute):
    """The position of an item whose `attribute` ends in three digits."""
    return lambda item: int(item[attribute]["S"][-3:])


def _read(client, statement, position, **kwargs):
    """Every page of `statement`, following NextToken: the positions of its
    items in the order it returns them, and its LastEvaluatedKey (None when it
    has none). The hash key size refusal ends the paging as "R"."""
    pages, keys, token = [], [], None
    for _ in range(40):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        try:
            response = client.execute_statement(Statement=statement, **args)
        except ClientError as e:
            assert e.response["Error"]["Code"] == "ValidationException", e.response
            message = e.response["Error"]["Message"]
            pages.append("R" if message == HASH_SIZE else message)
            break
        pages.append([position(item) for item in response["Items"]])
        keys.append(response.get("LastEvaluatedKey"))
        token = response.get("NextToken")
        if not token:
            break
    return pages, keys


def _wait_for_index(client, table, count):
    """Wait until the index gix shows every item written (it fills
    asynchronously on the service)."""
    for _ in range(240):
        scan = client.scan(TableName=table, IndexName="gix", Select="COUNT")
        if scan["Count"] == count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"gix of {table} did not reach {count} items")


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


@pytest.fixture(scope="module")
def htable(dynamodb_client):
    """A table keyed on a string pk holding v000 to v049."""
    with scoped_table(dynamodb_client) as name:
        for value in _values(50):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": value}})
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on pk and sk holding the partitions v000 to v039 of one
    item each (sk s) and the partition p of the items s000 to s039."""
    with _keyed_table(dynamodb_client) as name:
        for value in _values(40):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": value}, "sk": {"S": "s"}})
        for value in _values(40, "s"):
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "p"}, "sk": {"S": value}})
        yield name


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """A table with a GSI gix keyed on g, holding p000 to p039 with g v000 to
    v039."""
    with _indexed_table(dynamodb_client) as name:
        for i, value in enumerate(_values(40)):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"p{i:03}"}, "g": {"S": value}}
            )
        _wait_for_index(dynamodb_client, name, 40)
        yield name


@pytest.fixture(scope="module")
def g2table(dynamodb_client):
    """A table with a GSI gix keyed on g and gs, holding q000 to q039 in index
    partition a with gs w000 to w039."""
    with _indexed_table(dynamodb_client, "gs") as name:
        for i, value in enumerate(_values(40, "w")):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"q{i:03}"}, "g": {"S": "a"}, "gs": {"S": value}},
            )
        _wait_for_index(dynamodb_client, name, 40)
        yield name


@pytest.fixture(scope="module")
def qtable(dynamodb_client):
    """A table keyed on pk and sk holding the partitions p00 to p18 of one
    item each (sk s), except p10, which holds the items s00 to s20."""
    with _keyed_table(dynamodb_client) as name:
        for i in range(19):
            sorts = [f"s{j:02}" for j in range(21)] if i == 10 else ["s"]
            for sort in sorts:
                dynamodb_client.put_item(
                    TableName=name, Item={"pk": {"S": f"p{i:02}"}, "sk": {"S": sort}}
                )
        yield name


# Pages of point reads, as measured on N disjuncts.
POINT_READS = {
    17: _expected("0..9", "16 10..15"),
    21: _expected("0..9", "16..20 10..14", "15"),
    22: _expected("0..9", "16..21 10..13", "14 15"),
    23: _expected("0..9", "10..19", "20..22"),
    33: _expected("0..9", "32 10..18", "19..28", "29..31"),
    37: _expected("0..9", "10..19", "32..36 20..24", "25..31"),
    41: _expected("0..9", "10..19", "32..40 20", "21..30", "31"),
    50: _expected("0..9", "10..19", "20..29", "32..41", "48 49 42..46 30 47 31"),
}


@pytest.mark.parametrize("count", sorted(POINT_READS))
def test_later_pages_visit_the_disjuncts_left_by_position_modulo_the_buckets(
    dynamodb_client, htable, count
):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_listed(_values(count))}]'
    pages, _ = _read(dynamodb_client, statement, _number("pk"))
    assert pages == POINT_READS[count]


@pytest.mark.parametrize(
    ("count", "limit", "pages"),
    [
        (21, 3, _expected("0..2", "3..5", "6..8", "16..18", "19 20 9", "10..12", "13..15")),
        (
            33,
            4,
            _expected(
                "0..3", "4..7", "8..11", "32 12..14", "15..18", "19..22", "23..26", "27..30", "31"
            ),
        ),
    ],
)
def test_pages_with_a_limit_visit_in_the_same_order(
    dynamodb_client, htable, count, limit, pages
):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_listed(_values(count))}]'
    got, _ = _read(dynamodb_client, statement, _number("pk"), Limit=limit)
    assert got == pages


@pytest.mark.parametrize(
    ("count", "limit", "pages", "last_keys"),
    [
        (21, 10, _expected("0..9", "16..20 10..14", "15"), ["v009", "v014", None]),
        (
            33,
            7,
            _expected("0..6", "7..13", "32 14..19", "20..26", "27..31"),
            ["v006", "v013", "v019", "v026", None],
        ),
    ],
)
def test_the_last_key_of_a_full_page_is_its_last_item_in_visit_order(
    dynamodb_client, htable, count, limit, pages, last_keys
):
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{_listed(_values(count))}]'
    got, keys = _read(dynamodb_client, statement, _number("pk"), Limit=limit)
    assert got == pages
    assert keys == [None if key is None else {"pk": {"S": key}} for key in last_keys]


def test_an_order_by_numbers_the_disjuncts_in_its_order(dynamodb_client, htable):
    statement = (
        f'SELECT * FROM "{htable}" WHERE pk IN [{_listed(_values(33))}] ORDER BY pk DESC'
    )
    pages, _ = _read(dynamodb_client, statement, lambda item: 32 - _number("pk")(item))
    assert pages == POINT_READS[33]


@pytest.mark.parametrize("count", [21, 33])
def test_an_index_read_visits_in_the_same_order(dynamodb_client, gtable, count):
    statement = f'SELECT * FROM "{gtable}"."gix" WHERE g IN [{_listed(_values(count))}]'
    pages, _ = _read(dynamodb_client, statement, _number("g"))
    assert pages == POINT_READS[count]


@pytest.mark.parametrize("count", [21, 33])
def test_queries_of_partitions_visit_in_the_same_order(dynamodb_client, ctable, count):
    statement = f'SELECT * FROM "{ctable}" WHERE pk IN [{_listed(_values(count))}]'
    pages, _ = _read(dynamodb_client, statement, _number("pk"))
    assert pages == POINT_READS[count]


@pytest.mark.parametrize("count", [21, 33])
def test_sort_key_values_of_one_partition_visit_in_the_same_order(
    dynamodb_client, ctable, count
):
    statement = (
        f"SELECT * FROM \"{ctable}\" WHERE pk = 'p' AND sk IN [{_listed(_values(count, 's'))}]"
    )
    pages, _ = _read(dynamodb_client, statement, _number("sk"))
    assert pages == POINT_READS[count]


@pytest.mark.parametrize("count", [21, 33])
def test_sort_key_values_of_one_index_partition_visit_in_the_same_order(
    dynamodb_client, g2table, count
):
    values = _listed(_values(count, "w"))
    statement = f"SELECT * FROM \"{g2table}\".\"gix\" WHERE g = 'a' AND gs IN [{values}]"
    pages, _ = _read(dynamodb_client, statement, _number("gs"))
    assert pages == POINT_READS[count]


def _with_oversized(values, oversized):
    """The statement on `values` and the values past the key size limit
    `oversized`, and the position of each value in key order."""
    ordered = sorted(values + oversized)
    return _listed(values + oversized), {value: i for i, value in enumerate(ordered)}


@pytest.mark.parametrize(
    ("oversized", "pages"),
    [
        ("a" * 2049, _expected("1..9", "32 10..17", "18..26", "27..31", "R")),
        ("v004" + "z" * 2049, _expected("0..4 6..9", "32 10..17", "18..26", "27..31", "R")),
        ("v011" + "z" * 2049, _expected("0..9", "32 10 11 13..18", "19..27", "28..31", "R")),
        ("v019" + "z" * 2049, _expected("0..9", "32 10..18", "19 21..28", "29..31", "R")),
        ("v031" + "z" * 2049, _expected("0..9", "10..18", "19..27", "28..31", "R")),
    ],
    ids=["at-0", "at-5", "at-12", "at-20", "at-32"],
)
def test_a_held_back_value_takes_a_visit_on_every_page_that_reaches_it(
    dynamodb_client, htable, oversized, pages
):
    listed, position = _with_oversized(_values(32), [oversized])
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{listed}]'
    got, _ = _read(dynamodb_client, statement, lambda item: position[item["pk"]["S"]])
    assert got == pages


def test_a_held_back_value_that_sorts_last_is_visited_again(dynamodb_client, htable):
    listed, position = _with_oversized(_values(20), ["z" * 2049])
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{listed}]'
    got, _ = _read(dynamodb_client, statement, lambda item: position[item["pk"]["S"]])
    assert got == _expected("0..9", "16..19 10..14", "15", "R")


def test_two_held_back_values_with_a_limit(dynamodb_client, htable):
    listed, position = _with_oversized(
        _values(21), ["v004" + "z" * 2049, "v015" + "z" * 2049]
    )
    statement = f'SELECT * FROM "{htable}" WHERE pk IN [{listed}]'
    got, _ = _read(dynamodb_client, statement, lambda item: position[item["pk"]["S"]], Limit=4)
    assert got == _expected(
        "0..3", "4 6 7 8", "9..12", "16 18 19 20", "21 22 13 14", "15", "R"
    )


def _partitions(first, last):
    """The partitions v<first> to v<last> of ctable, listed."""
    return _listed(f"v{i:03}" for i in range(first, last + 1))


@pytest.mark.parametrize("limit", [None, 3], ids=["no-limit", "limit-3"])
def test_a_request_whose_page_visits_only_held_back_disjuncts_is_refused(
    dynamodb_client, ctable, limit
):
    # Positions 0 to 9 are held back for the filter refusal and sort first;
    # positions 10 to 20 read partitions.
    statement = (
        f'SELECT * FROM "{ctable}" WHERE (pk IN [{_partitions(0, 9)}] AND sk = a) '
        f"OR pk IN [{_partitions(10, 20)}]"
    )
    kwargs = {"Limit": limit} if limit else {}
    pages, _ = _read(dynamodb_client, statement, _number("pk"), **kwargs)
    assert pages == [FILTER_KEY]


def test_a_later_request_whose_page_visits_only_held_back_disjuncts_is_refused(
    dynamodb_client, ctable
):
    # The second page's ten visits go to positions 10 to 19, all held back,
    # while 20 to 24 are left unvisited.
    statement = (
        f'SELECT * FROM "{ctable}" WHERE pk IN [{_partitions(0, 9)}] '
        f"OR (pk IN [{_partitions(10, 19)}] AND sk = a) OR pk IN [{_partitions(20, 24)}]"
    )
    pages, _ = _read(dynamodb_client, statement, _number("pk"))
    assert pages == _expected("0..9") + [FILTER_KEY]


def test_a_page_of_nine_held_back_visits_and_one_read_is_a_page(dynamodb_client, ctable):
    statement = (
        f'SELECT * FROM "{ctable}" WHERE (pk IN [{_partitions(0, 8)}] AND sk = a) '
        f"OR pk IN [{_partitions(9, 20)}]"
    )
    pages, _ = _read(dynamodb_client, statement, _number("pk"))
    assert pages == _expected(*[str(p) for p in range(9, 18)], "18..20") + [FILTER_KEY]


def test_a_query_stopped_at_the_limit_goes_on_at_its_place(dynamodb_client, qtable):
    conditions = [
        f"pk = 'p{i:02}'" if i == 10 else f"(pk = 'p{i:02}' AND sk = 's')" for i in range(19)
    ]
    statement = f'SELECT * FROM "{qtable}" WHERE ' + " OR ".join(conditions)
    pages, _ = _read(dynamodb_client, statement, lambda item: int(item["pk"]["S"][1:]), Limit=6)

    def runs(page):
        """The page with each run of items of partition p10 written as
        10x<count>."""
        out = []
        for value in page:
            if value == 10 and out and out[-1].startswith("10x"):
                out[-1] = f"10x{int(out[-1][3:]) + 1}"
            else:
                out.append("10x1" if value == 10 else str(value))
        return " ".join(out)

    assert [runs(page) for page in pages] == [
        "0 1 2 3 4 5",
        "6 7 8 9 10x2",
        "16 17 18 10x3",
        "10x6",
        "10x6",
        "10x4 11 12",
        "13 14 15",
    ]
