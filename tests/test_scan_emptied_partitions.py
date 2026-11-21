# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan of a table whose items sit among partitions whose items were deleted.

Amazon DynamoDB answers such a Scan with every live item and counts only
live items in ScannedCount, with no Limit, with a Limit, across parallel
segments, with Select COUNT and with a FilterExpression. With Limit 1, a
Scan of 59 live items among 100 partitions whose items were deleted took 60
pages: 59 with one item and a last one with none. Measured on 2026-10-26 at
15:29 UTC (us-east-1), each case twice. A table whose 50 items have all been
deleted, with a sort key or hash only, answers Count 0, ScannedCount 0 and
no LastEvaluatedKey with no Limit and with Limit 1 and 7. Measured at 16:39
UTC, each case twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from conftest import scoped_table

LIVE = [f"live-{i:02d}" for i in range(30)]
GONE = [f"gone-{i:03d}" for i in range(100)]


def _key(pk, sort_key):
    key = {"pk": {"S": pk}}
    if sort_key:
        key["sk"] = {"S": "a"}
    return key


def _fill(client, name, sort_key):
    for pk in LIVE:
        client.put_item(TableName=name, Item={**_key(pk, sort_key), "x": {"S": "x"}})
    for pk in GONE:
        client.put_item(TableName=name, Item={**_key(pk, sort_key), "x": {"S": "x"}})
        client.delete_item(TableName=name, Key=_key(pk, sort_key))


SORT_KEY_TABLE = {
    "attribute_definitions": [
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
    ],
    "key_schema": [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ],
}


@pytest.fixture(scope="module", params=["sort_key", "hash_only"])
def table(request, dynamodb_client):
    sort_key = request.param == "sort_key"
    with scoped_table(dynamodb_client, **(SORT_KEY_TABLE if sort_key else {})) as name:
        _fill(dynamodb_client, name, sort_key)
        yield name


def _page_all(client, name, **kw):
    pks, pages, start = [], [], None
    for _ in range(500):
        args = {"TableName": name, "ConsistentRead": True, **kw}
        if start:
            args["ExclusiveStartKey"] = start
        resp = client.scan(**args)
        pages.append(resp)
        pks += [item["pk"]["S"] for item in resp.get("Items", [])]
        start = resp.get("LastEvaluatedKey")
        if not start:
            return pks, pages
    raise AssertionError("more than 500 pages")


def test_scan_with_no_limit(dynamodb_client, table):
    pks, pages = _page_all(dynamodb_client, table)
    assert len(pages) == 1
    assert (pages[0]["Count"], pages[0]["ScannedCount"]) == (30, 30)
    assert sorted(pks) == LIVE


@pytest.mark.parametrize("limit", [1, 7, 50])
def test_scan_pages_through_every_item(dynamodb_client, table, limit):
    pks, pages = _page_all(dynamodb_client, table, Limit=limit)
    assert sorted(pks) == LIVE
    first = pages[0]
    expected = min(limit, 30)
    assert (first["Count"], first["ScannedCount"]) == (expected, expected)


def test_parallel_scan_reads_every_item(dynamodb_client, table):
    pks = []
    for segment in (0, 1):
        seg_pks, _ = _page_all(dynamodb_client, table, Limit=3, Segment=segment, TotalSegments=2)
        pks += seg_pks
    assert sorted(pks) == LIVE


def test_scan_count(dynamodb_client, table):
    _, pages = _page_all(dynamodb_client, table, Select="COUNT")
    assert sum(p["Count"] for p in pages) == 30


def test_scan_with_a_filter(dynamodb_client, table):
    flt = {"FilterExpression": "begins_with(pk, :p)", "ExpressionAttributeValues": {":p": {"S": "live-0"}}}
    pks, pages = _page_all(dynamodb_client, table, **flt)
    assert (pages[0]["Count"], pages[0]["ScannedCount"]) == (10, 30)
    assert sorted(pks) == LIVE[:10]
    pks, _ = _page_all(dynamodb_client, table, Limit=7, **flt)
    assert sorted(pks) == LIVE[:10]


@pytest.mark.parametrize("sort_key", [True, False], ids=["sort_key", "hash_only"])
def test_scan_of_a_table_with_no_item_left(dynamodb_client, sort_key):
    with scoped_table(dynamodb_client, **(SORT_KEY_TABLE if sort_key else {})) as name:
        for pk in GONE[:50]:
            dynamodb_client.put_item(TableName=name, Item={**_key(pk, sort_key), "x": {"S": "x"}})
            dynamodb_client.delete_item(TableName=name, Key=_key(pk, sort_key))
        for kw in ({}, {"Limit": 1}, {"Limit": 7}):
            resp = dynamodb_client.scan(TableName=name, ConsistentRead=True, **kw)
            assert (resp["Count"], resp["ScannedCount"]) == (0, 0), kw
            assert "LastEvaluatedKey" not in resp, kw
