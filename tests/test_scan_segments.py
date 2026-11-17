# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Parallel Scan segments, dual-target against DynamoDB and ExtendDB.

DynamoDB returns all items of a partition key from one segment, on a table and
on a global secondary index, and every item from exactly one segment. That
holds for every segment count, with a FilterExpression that every item
matches, and across the pages of a segment read with a Limit (measured in
us-east-1, 13 and 14 Nov 2026).
"""

from __future__ import annotations

import pytest

from conftest import _paginate, scoped_table, wait_for_gsi_items

# Items per partition key of the table: p0 holds 5, p1 3, p2 2 and p3 1.
TABLE_PARTITIONS = {"p0": 5, "p1": 3, "p2": 2, "p3": 1}
TABLE_SEGMENT_COUNTS = (1, 2, 3, 4, 11, 13)

# The GSI table holds p0 to p5, with gpk g0 for the even ones and g1 for the odd.
GSI_NAME = "byGroup"
GSI_ITEM_COUNT = 6
GSI_SEGMENT_COUNTS = (1, 2, 3, 6, 8)

# A filter every item matches: v is set on every item and never equals :absent.
FILTER = {
    "FilterExpression": "#v <> :absent",
    "ExpressionAttributeNames": {"#v": "v"},
    "ExpressionAttributeValues": {":absent": {"S": "absent"}},
}


@pytest.fixture(scope="module")
def partitions_table(dynamodb_client):
    """A table keyed on pk S HASH and sk S RANGE with 11 items in four partitions."""
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
        n = 0
        for pk, count in TABLE_PARTITIONS.items():
            for i in range(count):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={"pk": {"S": pk}, "sk": {"S": f"s{i}"}, "v": {"S": str(n)}},
                )
                n += 1
        yield name


@pytest.fixture(scope="module")
def grouped_gsi_table(dynamodb_client):
    """A table keyed on pk S HASH and sk N RANGE with a GSI on gpk S, projection ALL."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "gpk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": GSI_NAME,
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        for i in range(GSI_ITEM_COUNT):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": f"p{i}"},
                    "sk": {"N": str(i)},
                    "gpk": {"S": f"g{i % 2}"},
                    "v": {"S": str(i)},
                },
            )

        def _index_items():
            pages = _paginate(dynamodb_client, "scan", TableName=name, IndexName=GSI_NAME)
            return [item for page in pages for item in page["Items"]]

        wait_for_gsi_items(_index_items, GSI_ITEM_COUNT, timeout=120.0)
        yield name


def _base_key(item: dict) -> tuple[str, str]:
    """The item's base key; sk is S on one table and N on the other."""
    return item["pk"]["S"], next(iter(item["sk"].values()))


CASES = [
    pytest.param("table", total, filtered, limit, id=f"table-{total}-{filtered}-{limit}")
    for total in TABLE_SEGMENT_COUNTS
    for filtered in ("plain", "filter")
    for limit in (None, 2)
] + [
    pytest.param("gsi", total, "plain", limit, id=f"gsi-{total}-plain-{limit}")
    for total in GSI_SEGMENT_COUNTS
    for limit in (None, 2)
]


@pytest.mark.parametrize(("target", "total_segments", "filtered", "limit"), CASES)
def test_every_partition_key_comes_back_from_one_segment(
    dynamodb_client, partitions_table, grouped_gsi_table, target, total_segments, filtered, limit
):
    if target == "table":
        request = {"TableName": partitions_table}
        pk_attr = "pk"
        expected_items = sum(TABLE_PARTITIONS.values())
    else:
        request = {"TableName": grouped_gsi_table, "IndexName": GSI_NAME}
        pk_attr = "gpk"
        expected_items = GSI_ITEM_COUNT
    if filtered == "filter":
        request.update(FILTER)
    if limit is not None:
        request["Limit"] = limit

    # Every segment paged to its end, with the Scan parameters every page shares.
    segments: list[list[dict]] = []
    for segment in range(total_segments):
        pages = _paginate(
            dynamodb_client, "scan", Segment=segment, TotalSegments=total_segments, **request
        )
        segments.append([item for page in pages for item in page["Items"]])

    # Every item once across the segments.
    keys = [_base_key(item) for items in segments for item in items]
    assert len(keys) == expected_items, f"{len(keys)} items came back, not {expected_items}"
    assert len(set(keys)) == expected_items, "an item came back from two segments"

    # Every partition key value from one segment.
    segments_of: dict[str, set[int]] = {}
    for segment, items in enumerate(segments):
        for item in items:
            segments_of.setdefault(item[pk_attr]["S"], set()).add(segment)
    split = {pk: sorted(segs) for pk, segs in segments_of.items() if len(segs) > 1}
    assert not split, f"{pk_attr} values came back from more than one segment: {split}"
