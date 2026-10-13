# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Read paging contract tests for Query and Scan.

Locks the client-visible pagination contract for reads that cross the 1 MB
page boundary: page size, LastEvaluatedKey placement, exactly-once pagination,
parallel scan segment union, Limit interaction, and ConsumedCapacity metering.

Two of these pin behavior at the page boundary specifically:
  - a FilterExpression that matches nothing still ends the page at 1 MB and
    returns Count 0 with a LastEvaluatedKey;
  - ConsumedCapacity is computed from the evaluated page, not from every row
    the storage layer could have returned.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import scoped_table, wait_for_gsi_items

PAGE_LIMIT_BYTES = 1_048_576

MB_ITEM_COUNT = 1_500
MB_PAYLOAD = "x" * 1_015  # item size ~1,038 bytes with pk + gsi_pk + payload

PARTITION_ITEM_COUNT = 3_000
PARTITION_PAYLOAD = "q" * 990

LARGE_PAYLOAD = "y" * 307_200  # ~300 KB
SMALL_PAYLOAD = "z" * 85  # ~100 B item


def _item_bytes(item: dict) -> int:
    """DynamoDB item size for string-only items: name bytes + value bytes."""
    total = 0
    for name, value in item.items():
        total += len(name.encode())
        if "S" in value:
            total += len(value["S"].encode())
        elif "N" in value:
            # Approximate; only byte-asserted tables are string-only.
            total += len(value["N"])
        else:
            raise NotImplementedError(f"unsupported type in {name}")
    return total


def _batch_write(client, table: str, items: list[dict], threads: int = 8) -> None:
    """Write items with batch_write_item, 25 per batch, retrying unprocessed."""
    batches = [items[i : i + 25] for i in range(0, len(items), 25)]

    def _write(batch: list[dict]) -> None:
        request = {table: [{"PutRequest": {"Item": it}} for it in batch]}
        for _ in range(20):
            resp = client.batch_write_item(RequestItems=request)
            unprocessed = resp.get("UnprocessedItems") or {}
            if not unprocessed.get(table):
                return
            request = unprocessed
        raise RuntimeError(f"unprocessed items remained after retries in {table}")

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(_write, batches))


def _paginate(client, operation: str, max_pages: int = 100, **kwargs) -> list[dict]:
    """Run a Scan or Query to exhaustion, returning the list of raw pages.

    Bounded so a LastEvaluatedKey that fails to advance surfaces as a short
    result instead of spinning forever.
    """
    pages: list[dict] = []
    call = getattr(client, operation)
    request = dict(kwargs)
    for _ in range(max_pages):
        resp = call(**request)
        pages.append(resp)
        if "LastEvaluatedKey" not in resp:
            break
        request["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return pages


def _all_items(pages: list[dict]) -> list[dict]:
    items: list[dict] = []
    for page in pages:
        items.extend(page.get("Items") or [])
    return items


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mb_scan_table(dynamodb_client):
    """Hash-only table, 1,500 items of ~1 KB (total ~1.5 MB), with a GSI."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "PayloadGSI",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        items = [
            {
                "pk": {"S": f"k{i:05d}"},
                "gsi_pk": {"S": f"g{i % 4}"},
                "payload": {"S": MB_PAYLOAD},
            }
            for i in range(MB_ITEM_COUNT)
        ]
        _batch_write(dynamodb_client, name, items)
        yield name


@pytest.fixture(scope="module")
def mixed_size_table(dynamodb_client):
    """Items alternating ~300 KB and ~100 B in key order, total ~4.3 MB."""
    with scoped_table(dynamodb_client) as name:
        for i in range(28):
            payload = LARGE_PAYLOAD if i % 2 == 0 else SMALL_PAYLOAD
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"m{i:03d}"}, "payload": {"S": payload}},
            )
        yield name


@pytest.fixture(scope="module")
def small_scan_table(dynamodb_client):
    """Hash-only table with exactly 20 small items for Limit tests."""
    with scoped_table(dynamodb_client) as name:
        for i in range(20):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"s{i:02d}"}, "v": {"S": "small"}},
            )
        yield name


@pytest.fixture(scope="module")
def partition_table(dynamodb_client):
    """Composite-key table with one 3,000 item ~3 MB partition and a GSI.

    Partition "big" holds 3,000 items of ~1 KB, mirrored into PosGSI.
    Partition "lim" holds exactly 20 small items (sparse, not in the GSI)
    for the Query Limit tests.
    """
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "PosGSI",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        items = [
            {
                "pk": {"S": "big"},
                "sk": {"N": str(i)},
                "gsi_pk": {"S": "g"},
                "gsi_sk": {"N": str(i)},
                "payload": {"S": PARTITION_PAYLOAD},
            }
            for i in range(PARTITION_ITEM_COUNT)
        ]
        items.extend(
            {"pk": {"S": "lim"}, "sk": {"N": str(i)}, "v": {"S": "small"}}
            for i in range(20)
        )
        _batch_write(dynamodb_client, name, items)
        yield name


# ---------------------------------------------------------------------------
# Case 1: no Limit over a table larger than 1 MB
# ---------------------------------------------------------------------------


class TestNoLimitPaging:
    """Scan without Limit over >1 MB of data pages at the 1 MB boundary."""

    def test_scan_first_page_cut_at_1mb(self, dynamodb_client, mb_scan_table):
        resp = dynamodb_client.scan(TableName=mb_scan_table)
        assert "LastEvaluatedKey" in resp, "page 1 of a >1 MB table must have a LEK"
        assert resp["Count"] < MB_ITEM_COUNT, "page 1 must not contain the whole table"
        page_bytes = sum(_item_bytes(it) for it in resp["Items"])
        max_item = max(_item_bytes(it) for it in resp["Items"])
        assert page_bytes <= PAGE_LIMIT_BYTES + max_item, (
            f"page 1 carries {page_bytes} bytes, over the 1 MB page budget"
        )

    def test_scan_pagination_exactly_once(self, dynamodb_client, mb_scan_table):
        pages = _paginate(dynamodb_client, "scan", TableName=mb_scan_table)
        assert len(pages) >= 2, "a 1.5 MB table must take more than one page"
        assert "LastEvaluatedKey" not in pages[-1]
        pks = [it["pk"]["S"] for it in _all_items(pages)]
        assert len(pks) == MB_ITEM_COUNT
        assert len(set(pks)) == MB_ITEM_COUNT, "pagination returned duplicates"
        assert sorted(pks) == [f"k{i:05d}" for i in range(MB_ITEM_COUNT)]
        for page in pages:
            page_items = page.get("Items") or []
            if not page_items:
                continue
            page_bytes = sum(_item_bytes(it) for it in page_items)
            max_item = max(_item_bytes(it) for it in page_items)
            assert page_bytes <= PAGE_LIMIT_BYTES + max_item

    def test_parallel_scan_segments_union(self, dynamodb_client, mb_scan_table):
        total_segments = 4
        pks: list[str] = []
        for segment in range(total_segments):
            pages = _paginate(
                dynamodb_client,
                "scan",
                TableName=mb_scan_table,
                Segment=segment,
                TotalSegments=total_segments,
            )
            pks.extend(it["pk"]["S"] for it in _all_items(pages))
        assert len(pks) == MB_ITEM_COUNT, "segments must cover the table exactly"
        assert len(set(pks)) == MB_ITEM_COUNT, "segments overlapped"
        assert sorted(pks) == [f"k{i:05d}" for i in range(MB_ITEM_COUNT)]

    def test_gsi_scan_pagination_exactly_once(self, dynamodb_client, mb_scan_table):
        def _collect():
            pages = _paginate(
                dynamodb_client,
                "scan",
                TableName=mb_scan_table,
                IndexName="PayloadGSI",
            )
            return [it["pk"]["S"] for it in _all_items(pages)]

        pks = wait_for_gsi_items(_collect, MB_ITEM_COUNT, timeout=120.0)
        assert len(pks) == MB_ITEM_COUNT
        assert len(set(pks)) == MB_ITEM_COUNT, "GSI pagination returned duplicates"


# ---------------------------------------------------------------------------
# Case 2: FilterExpression that matches nothing still pages at 1 MB
# ---------------------------------------------------------------------------


class TestFilterNoMatchPaging:
    def test_scan_filter_matches_nothing_pages_with_count_zero(
        self, dynamodb_client, mb_scan_table
    ):
        first = dynamodb_client.scan(
            TableName=mb_scan_table,
            FilterExpression="payload = :v",
            ExpressionAttributeValues={":v": {"S": "no-item-has-this"}},
        )
        assert first["Count"] == 0
        assert "LastEvaluatedKey" in first, (
            "a >1 MB scan with a never-matching filter must end page 1 at the "
            "1 MB boundary with a LastEvaluatedKey"
        )
        pages = _paginate(
            dynamodb_client,
            "scan",
            TableName=mb_scan_table,
            FilterExpression="payload = :v",
            ExpressionAttributeValues={":v": {"S": "no-item-has-this"}},
        )
        assert all(page["Count"] == 0 for page in pages)
        assert "LastEvaluatedKey" not in pages[-1]


# ---------------------------------------------------------------------------
# Case 3: Limit interaction with page boundaries
# ---------------------------------------------------------------------------


class TestLimitPaging:
    def test_scan_limit_7_over_20(self, dynamodb_client, small_scan_table):
        pages = _paginate(
            dynamodb_client, "scan", TableName=small_scan_table, Limit=7
        )
        assert [p["Count"] for p in pages] == [7, 7, 6]
        assert "LastEvaluatedKey" in pages[0]
        assert "LastEvaluatedKey" in pages[1]
        assert "LastEvaluatedKey" not in pages[2]
        pks = [it["pk"]["S"] for it in _all_items(pages)]
        assert sorted(pks) == [f"s{i:02d}" for i in range(20)]

    def test_query_limit_7_over_20(self, dynamodb_client, partition_table):
        pages = _paginate(
            dynamodb_client,
            "query",
            TableName=partition_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "lim"}},
            Limit=7,
        )
        assert [p["Count"] for p in pages] == [7, 7, 6]
        assert "LastEvaluatedKey" not in pages[2]
        sks = [int(it["sk"]["N"]) for it in _all_items(pages)]
        assert sks == list(range(20))

    def test_scan_limit_20_over_exactly_20(self, dynamodb_client, small_scan_table):
        """Limit reached exactly at the end of the data: the service returns a
        LastEvaluatedKey (it stops at Limit without looking past it) and the
        next page is empty with none. Measured 2026-09-16."""
        resp = dynamodb_client.scan(TableName=small_scan_table, Limit=20)
        assert resp["Count"] == 20
        assert "LastEvaluatedKey" in resp
        tail = dynamodb_client.scan(
            TableName=small_scan_table, Limit=20, ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        assert tail["Count"] == 0
        assert "LastEvaluatedKey" not in tail

    def test_query_limit_20_over_exactly_20(self, dynamodb_client, partition_table):
        resp = dynamodb_client.query(
            TableName=partition_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "lim"}},
            Limit=20,
        )
        assert resp["Count"] == 20
        assert "LastEvaluatedKey" in resp
        tail = dynamodb_client.query(
            TableName=partition_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "lim"}},
            Limit=20,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        assert tail["Count"] == 0
        assert "LastEvaluatedKey" not in tail


# ---------------------------------------------------------------------------
# Case 4: widely varying item sizes
# ---------------------------------------------------------------------------


class TestMixedSizePaging:
    def test_scan_mixed_sizes_exactly_once_and_page_budget(
        self, dynamodb_client, mixed_size_table
    ):
        pages = _paginate(dynamodb_client, "scan", TableName=mixed_size_table)
        pks = [it["pk"]["S"] for it in _all_items(pages)]
        assert len(pks) == 28
        assert len(set(pks)) == 28, "pagination returned duplicates"
        assert sorted(pks) == [f"m{i:03d}" for i in range(28)]
        for page in pages:
            page_items = page.get("Items") or []
            if not page_items:
                continue
            page_bytes = sum(_item_bytes(it) for it in page_items)
            max_item = max(_item_bytes(it) for it in page_items)
            assert page_bytes <= PAGE_LIMIT_BYTES + max_item, (
                f"page carries {page_bytes} bytes with max item {max_item}"
            )


# ---------------------------------------------------------------------------
# Case 5: ConsumedCapacity reflects the page, not the fetch
# ---------------------------------------------------------------------------


class TestScanConsumedCapacity:
    def test_scan_consumed_capacity_matches_page_bytes(
        self, dynamodb_client, mb_scan_table
    ):
        resp = dynamodb_client.scan(
            TableName=mb_scan_table, ReturnConsumedCapacity="TOTAL"
        )
        assert "LastEvaluatedKey" in resp, "precondition: table larger than one page"
        page_bytes = sum(_item_bytes(it) for it in resp["Items"])
        # Eventually consistent read: 0.5 RCU per 4 KB of evaluated data.
        expected = math.ceil(page_bytes / 4096) * 0.5
        cu = resp["ConsumedCapacity"]["CapacityUnits"]
        assert abs(cu - expected) <= max(2.0, expected * 0.1), (
            f"CapacityUnits {cu} does not correspond to page bytes {page_bytes} "
            f"(expected about {expected}); metering the whole fetch would give "
            "about 1.5x this page"
        )


# ---------------------------------------------------------------------------
# Case 6: Query over a 3 MB partition, both directions, base and GSI
# ---------------------------------------------------------------------------


class TestQueryPartitionPaging:
    def test_query_forward_exactly_once_in_order(self, dynamodb_client, partition_table):
        pages = _paginate(
            dynamodb_client,
            "query",
            TableName=partition_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "big"}},
        )
        assert len(pages) >= 2, "a 3 MB partition must take more than one page"
        sks = [int(it["sk"]["N"]) for it in _all_items(pages)]
        assert sks == list(range(PARTITION_ITEM_COUNT))

    def test_query_reverse_exactly_once_in_order(self, dynamodb_client, partition_table):
        pages = _paginate(
            dynamodb_client,
            "query",
            TableName=partition_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "big"}},
            ScanIndexForward=False,
        )
        assert len(pages) >= 2
        sks = [int(it["sk"]["N"]) for it in _all_items(pages)]
        assert sks == list(range(PARTITION_ITEM_COUNT - 1, -1, -1))

    def test_gsi_query_forward_exactly_once_in_order(
        self, dynamodb_client, partition_table
    ):
        def _collect():
            pages = _paginate(
                dynamodb_client,
                "query",
                TableName=partition_table,
                IndexName="PosGSI",
                KeyConditionExpression="gsi_pk = :g",
                ExpressionAttributeValues={":g": {"S": "g"}},
            )
            return [int(it["gsi_sk"]["N"]) for it in _all_items(pages)]

        sks = wait_for_gsi_items(_collect, PARTITION_ITEM_COUNT, timeout=120.0)
        assert sks == list(range(PARTITION_ITEM_COUNT))

    def test_gsi_query_reverse_exactly_once_in_order(
        self, dynamodb_client, partition_table
    ):
        def _collect():
            pages = _paginate(
                dynamodb_client,
                "query",
                TableName=partition_table,
                IndexName="PosGSI",
                KeyConditionExpression="gsi_pk = :g",
                ExpressionAttributeValues={":g": {"S": "g"}},
                ScanIndexForward=False,
            )
            return [int(it["gsi_sk"]["N"]) for it in _all_items(pages)]

        sks = wait_for_gsi_items(_collect, PARTITION_ITEM_COUNT, timeout=120.0)
        assert sks == list(range(PARTITION_ITEM_COUNT - 1, -1, -1))
