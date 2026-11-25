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

# The clause of a table with more than 25 requests. DynamoDB gave each such
# table this clause, two tables in either order, in every answer measured on
# 2026-11-20. It quoted the request map instead in some answers of 2026-11-14
# and 2026-11-16 and in every answer of 2026-11-25, in a message that holds
# QUOTED_MAP, so the test accepts that from DynamoDB.
OVER_LIMIT = (
    "Value at 'RequestItems.{}.member' failed to satisfy constraint: "
    "Member must have length less than or equal to 25"
)
QUOTED_MAP = (
    "' at 'requestItems' failed to satisfy constraint: Map value must satisfy "
    "constraint: [Member must have length less than or equal to 25, "
    "Member must have length greater than or equal to 1]"
)


def _puts(n: int) -> list[dict]:
    return [{"PutRequest": {"Item": {"pk": {"S": f"k{i}"}}}} for i in range(n)]


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
        ],
    )
    def test_batch_write_too_many_items(
        self, table_factory, dynamodb_client, is_extenddb, tables, named
    ):
        """More than 25 requests for a table is rejected, with each such table
        named at its member path."""
        names = [table_factory(unique_name(prefix)) for prefix, _ in tables]
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_write_item(
                RequestItems={name: reqs for name, (_, reqs) in zip(names, tables)}
            )
        error = exc.value.response["Error"]
        assert error["Code"] == "ValidationException"
        if not is_extenddb and QUOTED_MAP in error["Message"]:
            return
        clauses = [OVER_LIMIT.format(names[i]) for i in named]
        detected = f"{len(clauses)} validation error{'s' if len(clauses) > 1 else ''} detected: "
        assert error["Message"] in {detected + "; ".join(c) for c in (clauses, clauses[::-1])}

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

    def test_batch_get_too_many_keys(self, table_factory, dynamodb_client):
        """More than 100 keys is rejected."""
        name = table_factory()
        keys = [{"pk": {"S": f"k{i}"}} for i in range(101)]
        with pytest.raises(ClientError) as exc:
            dynamodb_client.batch_get_item(RequestItems={name: {"Keys": keys}})
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_get_unprocessed_empty(self, table_factory, dynamodb_client):
        """Small batch returns empty UnprocessedKeys."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}})
        resp = dynamodb_client.batch_get_item(
            RequestItems={name: {"Keys": [{"pk": {"S": "k1"}}]}}
        )
        assert resp.get("UnprocessedKeys", {}) == {}
