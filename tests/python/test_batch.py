# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Batch operations: BatchGetItem, BatchWriteItem.

Covers single-table and cross-table batches, error paths, limits,
unprocessed items, and composite key tables.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name

# The clause of a count outside its bound: the requests or keys of a table, or
# the tables of RequestItems. Each test gives the path and the bound. In every
# answer measured on 2026-11-20, DynamoDB listed the table count clause first
# when it was over the limit, then the table clauses in an order that changed
# from one send to the next, at most MAX_CLAUSES in all. In some BatchWriteItem
# answers of 2026-11-14 and 2026-11-16, and in every one of 2026-11-25, it
# quoted the request map instead, in a message that holds QUOTED. The
# BatchWriteItem test accepts that from DynamoDB.
CLAUSE = "Value at '{}' failed to satisfy constraint: Member must have length {}"
MAX_CLAUSES = 10
QUOTED = "' at 'requestItems' failed to satisfy constraint: "


def _assert_clauses(message: str, clauses: list[str], count: str | None = None) -> None:
    """The message lists `count` first when given, then clauses of `clauses` in
    any order, each once, MAX_CLAUSES in all at most."""
    n = min(len(clauses) + (count is not None), MAX_CLAUSES)
    head, _, rest = message.partition(": ")
    assert head == f"{n} validation error{'s' if n > 1 else ''} detected", message
    listed = rest.split("; ")
    assert len(listed) == n, message
    if count is not None:
        assert listed.pop(0) == count, message
    assert len(set(listed)) == len(listed) and set(listed) <= set(clauses), message


def _puts(n: int) -> list[dict]:
    return [{"PutRequest": {"Item": {"pk": {"S": f"k{i}"}}}} for i in range(n)]


def _counts(n: int, empty: range | set[int], big: int | None = None, over: int = 101) -> list[int]:
    """n counts of one, except none at the indexes of `empty` and `over` at `big`."""
    return [0 if i in empty else over if i == big else 1 for i in range(n)]


def _writes(n: int, empty: range | set[int], big: int | None = None) -> list[tuple]:
    """n tables of one put, except none at `empty` and 26 at `big`."""
    return [("tbl", _puts(c)) for c in _counts(n, empty, big, 26)]


# The start of a table ARN in an account that is not the caller's.
OTHER_ACCOUNT = "arn:aws:dynamodb:us-east-1:000000000000:table/"


NESTED_PUT = {
    "PutRequest": {
        "Item": {
            "pk": {"S": "nested"},
            "doc": {"M": {"inner": {"L": [{"S": "a"}, {"NULL": True}]}}},
        }
    }
}
DELETES = [{"DeleteRequest": {"Key": {"pk": {"S": f"d{i}"}}}} for i in range(26)]


class TestBatchWriteItem:
    """BatchWriteItem API tests."""

    def test_batch_write_puts(self, table_factory, dynamodb_client):
        """Put multiple items in a single batch."""
        name = table_factory()
        dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {"PutRequest": {"Item": {"pk": {"S": f"k{i}"}, "v": {"N": str(i)}}}}
                    for i in range(5)
                ]
            }
        )
        for i in range(5):
            resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": f"k{i}"}})
            assert resp["Item"]["v"]["N"] == str(i)

    def test_batch_write_deletes(self, table_factory, dynamodb_client):
        """Delete multiple items in a single batch."""
        name = table_factory()
        for i in range(3):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"k{i}"}, "v": {"S": "x"}}
            )
        dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {"DeleteRequest": {"Key": {"pk": {"S": f"k{i}"}}}} for i in range(3)
                ]
            }
        )
        for i in range(3):
            resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": f"k{i}"}})
            assert "Item" not in resp

    def test_batch_write_mixed(self, table_factory, dynamodb_client):
        """Mix puts and deletes in a single batch."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "del1"}, "v": {"S": "old"}}
        )
        dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {"PutRequest": {"Item": {"pk": {"S": "put1"}, "v": {"S": "new"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "del1"}}}},
                ]
            }
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "put1"}})
        assert resp["Item"]["v"]["S"] == "new"
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "del1"}})
        assert "Item" not in resp

    def test_batch_write_cross_table(self, table_factory, dynamodb_client):
        """Write to multiple tables in a single batch."""
        t1 = table_factory()
        t2 = table_factory()
        dynamodb_client.batch_write_item(
            RequestItems={
                t1: [{"PutRequest": {"Item": {"pk": {"S": "a"}, "v": {"S": "1"}}}}],
                t2: [{"PutRequest": {"Item": {"pk": {"S": "b"}, "v": {"S": "2"}}}}],
            }
        )
        r1 = dynamodb_client.get_item(TableName=t1, Key={"pk": {"S": "a"}})
        r2 = dynamodb_client.get_item(TableName=t2, Key={"pk": {"S": "b"}})
        assert r1["Item"]["v"]["S"] == "1"
        assert r2["Item"]["v"]["S"] == "2"

    def test_batch_write_empty_request(self, dynamodb_client):
        """Empty RequestItems is rejected."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_write_item(RequestItems={})
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    @pytest.mark.parametrize(
        ("tables", "named"),
        [
            pytest.param([("tbl", _puts(25) + [NESTED_PUT])], [0], id="25_puts_and_a_nested_26th"),
            pytest.param([("tbl", DELETES)], [0], id="26_deletes"),
            pytest.param([("tbl", _puts(26)), ("tbl", _puts(26))], [0, 1], id="two_tables_of_26"),
            pytest.param([("tbl", _puts(1)), ("tbl", _puts(26))], [1], id="1_and_26"),
            pytest.param([("tbl.dot", _puts(26))], [0], id="a_dot_in_the_table_name"),
            pytest.param([("tbl", _puts(1)), ("tbl", [])], [1], id="1_and_none"),
            pytest.param([("tbl", _puts(26)), ("tbl", [])], [0, 1], id="26_and_none"),
            pytest.param(
                [("tbl", _puts(20)), ("tbl", _puts(10)), ("tbl", [])], [2], id="20_10_and_none"
            ),
            pytest.param([("tbl", _puts(1))] * 26, [], id="26_tables"),
            pytest.param(_writes(26, {13}), [13], id="26_tables_1_none"),
            pytest.param(_writes(30, {13}), [13], id="30_tables_1_none"),
            pytest.param(_writes(26, range(1, 10)), range(1, 10), id="26_tables_9_none"),
            pytest.param(_writes(26, range(1, 11)), range(1, 11), id="26_tables_10_none"),
            pytest.param(_writes(26, {5}, 9), [5, 9], id="26_tables_1_none_1_of_26"),
            pytest.param(
                [
                    (OTHER_ACCOUNT + p if i == 13 else p, r)
                    for i, (p, r) in enumerate(_writes(26, {5}))
                ],
                [5],
                id="26_tables_1_none_1_arn_of_another_account",
            ),
        ],
    )
    def test_batch_write_too_many_items(
        self, table_factory, raw_client, is_extenddb, tables, named
    ):
        """A table with more than 25 requests or none, or more than 25 tables,
        is rejected. The refusal names the table count when it is over 25, then
        each such table at its member path."""
        # A row of more than 25 tables names tables that do not exist, as measured.
        over = len(tables) > 25
        names = [
            unique_name(prefix) if over else table_factory(unique_name(prefix))
            for prefix, _ in tables
        ]
        with pytest.raises(ClientError) as exc:
            raw_client.batch_write_item(
                RequestItems={name: reqs for name, (_, reqs) in zip(names, tables)}
            )
        error = exc.value.response["Error"]
        assert error["Code"] == "ValidationException"
        if not is_extenddb and QUOTED in error["Message"]:
            return
        clauses = [
            CLAUSE.format(
                f"RequestItems.{names[i]}.member",
                "less than or equal to 25" if tables[i][1] else "greater than or equal to 1",
            )
            for i in named
        ]
        _assert_clauses(
            error["Message"],
            clauses,
            CLAUSE.format("RequestItems", "less than or equal to 25") if over else None,
        )

    def test_batch_write_unprocessed_empty(self, table_factory, dynamodb_client):
        """Small batch returns empty UnprocessedItems."""
        name = table_factory()
        resp = dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {"PutRequest": {"Item": {"pk": {"S": "k1"}, "v": {"S": "v1"}}}}
                ]
            }
        )
        assert resp.get("UnprocessedItems", {}) == {}

    def test_batch_write_composite_key(self, table_factory, dynamodb_client):
        """Batch write to a hash+range table."""
        name = table_factory(range_key="sk")
        dynamodb_client.batch_write_item(
            RequestItems={
                name: [
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": "p1"},
                                "sk": {"S": f"s{i}"},
                                "v": {"N": str(i)},
                            }
                        }
                    }
                    for i in range(3)
                ]
            }
        )
        for i in range(3):
            resp = dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}}
            )
            assert resp["Item"]["v"]["N"] == str(i)


class TestBatchGetItem:
    """BatchGetItem API tests."""

    def test_batch_get_single_table(self, table_factory, dynamodb_client):
        """Get multiple items from a single table."""
        name = table_factory()
        for i in range(5):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"k{i}"}, "v": {"N": str(i)}}
            )
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                name: {"Keys": [{"pk": {"S": f"k{i}"}} for i in range(5)]}
            }
        )
        items = resp["Responses"][name]
        assert len(items) == 5
        pks = {item["pk"]["S"] for item in items}
        assert pks == {f"k{i}" for i in range(5)}

    def test_batch_get_cross_table(self, table_factory, dynamodb_client):
        """Get items from multiple tables in a single batch."""
        t1 = table_factory()
        t2 = table_factory()
        dynamodb_client.put_item(TableName=t1, Item={"pk": {"S": "a"}, "v": {"S": "1"}})
        dynamodb_client.put_item(TableName=t2, Item={"pk": {"S": "b"}, "v": {"S": "2"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                t1: {"Keys": [{"pk": {"S": "a"}}]},
                t2: {"Keys": [{"pk": {"S": "b"}}]},
            }
        )
        assert len(resp["Responses"][t1]) == 1
        assert len(resp["Responses"][t2]) == 1

    def test_batch_get_missing_items(self, table_factory, dynamodb_client):
        """Missing items are silently omitted from results."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "exists"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                name: {"Keys": [{"pk": {"S": "exists"}}, {"pk": {"S": "missing"}}]}
            }
        )
        items = resp["Responses"][name]
        assert len(items) == 1
        assert items[0]["pk"]["S"] == "exists"

    def test_batch_get_composite_key(self, table_factory, dynamodb_client):
        """Batch get from a hash+range table."""
        name = table_factory(range_key="sk")
        for i in range(3):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}, "v": {"N": str(i)}},
            )
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                name: {
                    "Keys": [
                        {"pk": {"S": "p1"}, "sk": {"S": f"s{i}"}} for i in range(3)
                    ]
                }
            }
        )
        assert len(resp["Responses"][name]) == 3

    def test_batch_get_with_projection(self, table_factory, dynamodb_client):
        """Batch get with ProjectionExpression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}}
        )
        resp = dynamodb_client.batch_get_item(
            RequestItems={
                name: {
                    "Keys": [{"pk": {"S": "k1"}}],
                    "ProjectionExpression": "pk, a",
                }
            }
        )
        item = resp["Responses"][name][0]
        assert "a" in item
        assert "b" not in item

    def test_batch_get_empty_request(self, dynamodb_client):
        """Empty RequestItems is rejected."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_get_item(RequestItems={})
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    @pytest.mark.parametrize(
        ("counts", "named"),
        [
            pytest.param([101], [0], id="101"),
            pytest.param([101, 101], [0, 1], id="101_and_101"),
            pytest.param([101, 0], [0, 1], id="101_and_none"),
            pytest.param([60, 41, 0], [2], id="60_41_and_none"),
            pytest.param(_counts(101, {50}), [50], id="101_tables_1_none"),
            pytest.param(_counts(101, range(1, 10)), range(1, 10), id="101_tables_9_none"),
            pytest.param(_counts(101, range(1, 11)), range(1, 11), id="101_tables_10_none"),
            pytest.param(_counts(101, {5}, 9), [5, 9], id="101_tables_1_none_1_of_101"),
        ],
    )
    def test_batch_get_too_many_keys(self, table_factory, raw_client, counts, named):
        """A table with more than 100 keys or none, or more than 100 tables, is
        rejected. The refusal names the table count when it is over 100, then
        each such table at its member path."""
        # A row of more than 100 tables names tables that do not exist, as measured.
        over = len(counts) > 100
        names = [unique_name() if over else table_factory() for _ in counts]
        with pytest.raises(ClientError) as exc:
            raw_client.batch_get_item(
                RequestItems={
                    name: {"Keys": [{"pk": {"S": f"k{i}"}} for i in range(n)]}
                    for name, n in zip(names, counts)
                }
            )
        error = exc.value.response["Error"]
        assert error["Code"] == "ValidationException"
        clauses = [
            CLAUSE.format(
                f"RequestItems.{names[i]}.member.Keys",
                "less than or equal to 100" if counts[i] else "greater than or equal to 1",
            )
            for i in named
        ]
        _assert_clauses(
            error["Message"],
            clauses,
            CLAUSE.format("RequestItems", "less than or equal to 100") if over else None,
        )

    def test_batch_get_unprocessed_empty(self, table_factory, dynamodb_client):
        """Small batch returns empty UnprocessedKeys."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={name: {"Keys": [{"pk": {"S": "k1"}}]}}
        )
        assert resp.get("UnprocessedKeys", {}) == {}
