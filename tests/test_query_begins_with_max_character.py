# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""begins_with on a string sort key whose next character is at or near U+10FFFF.

Amazon DynamoDB returns every sort key that starts with the prefix, in the
order of their UTF-8 bytes, including keys that continue with U+10FFFF
(measured 2026-10-25 at 22:31 UTC, us-east-1, each request twice, both scan
directions; Limit 1 pages over the prefixes a, a + U+10FFFF and U+10FFFF at
22:35 UTC).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from conftest import scoped_table

MAX = "\U0010ffff"

# Every sort key the table holds, in the service's order (UTF-8 byte order).
KEYS = [
    "a",
    "ab",
    "a\ud7ff",
    "a\ue000",
    "a\uffff",
    "a\U00010000",
    "a" + MAX,
    "a" + MAX + "b",
    "a" + MAX + MAX,
    "b",
    "z",
    MAX,
    MAX + "x",
    MAX + MAX,
]

PK = "begins-with-max"


@pytest.fixture(scope="module")
def table(dynamodb_client):
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
        for sk in KEYS:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": PK}, "sk": {"S": sk}})
        yield name


def _sort_keys(dynamodb_client, table, key_condition, values, forward):
    items = []
    pages = 0
    kwargs = {
        "TableName": table,
        "KeyConditionExpression": key_condition,
        "ExpressionAttributeValues": {":p": {"S": PK}, **values},
        "ScanIndexForward": forward,
        "ConsistentRead": True,
    }
    while True:
        pages += 1
        assert pages <= len(KEYS) + 1, f"no last page after {pages - 1} pages: {items}"
        resp = dynamodb_client.query(**kwargs)
        items.extend(i["sk"]["S"] for i in resp["Items"])
        if "LastEvaluatedKey" not in resp:
            return items
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


@pytest.mark.parametrize("forward", [True, False], ids=["forward", "reverse"])
@pytest.mark.parametrize(
    "prefix",
    ["a", "a" + MAX, "a" + MAX + MAX, MAX, MAX + MAX, "a\ud7ff", "a\uffff"],
    ids=["a", "a_max", "a_max_max", "max", "max_max", "a_d7ff", "a_ffff"],
)
def test_begins_with_returns_every_key_with_the_prefix(dynamodb_client, table, prefix, forward):
    got = _sort_keys(
        dynamodb_client,
        table,
        "pk = :p AND begins_with(sk, :s)",
        {":s": {"S": prefix}},
        forward,
    )
    want = [k for k in KEYS if k.startswith(prefix)]
    assert got == (want if forward else want[::-1])


@pytest.mark.parametrize("prefix", ["a", "a" + MAX, MAX], ids=["a", "a_max", "max"])
def test_begins_with_pages_through_keys_after_the_prefix(dynamodb_client, table, prefix):
    """Limit 1 makes every page end inside the range, so each page's start key
    is folded into the same upper bound. U+10FFFF alone has no upper bound."""
    want = [k for k in KEYS if k.startswith(prefix)]
    for forward in (True, False):
        items = []
        kwargs = {
            "TableName": table,
            "KeyConditionExpression": "pk = :p AND begins_with(sk, :s)",
            "ExpressionAttributeValues": {":p": {"S": PK}, ":s": {"S": prefix}},
            "ScanIndexForward": forward,
            "ConsistentRead": True,
            "Limit": 1,
        }
        for _ in range(len(KEYS) + 1):
            resp = dynamodb_client.query(**kwargs)
            items.extend(i["sk"]["S"] for i in resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        else:
            pytest.fail(f"no last page after {len(KEYS) + 1} pages: {items}")
        assert items == (want if forward else want[::-1])


@pytest.mark.parametrize(
    ("low", "high"),
    [("a", "a" + MAX), ("a" + MAX, "b")],
    ids=["a_to_a_max", "a_max_to_b"],
)
def test_between_includes_both_bounds(dynamodb_client, table, low, high):
    got = _sort_keys(
        dynamodb_client,
        table,
        "pk = :p AND sk BETWEEN :l AND :u",
        {":l": {"S": low}, ":u": {"S": high}},
        True,
    )
    lo, hi = low.encode(), high.encode()
    assert got == [k for k in KEYS if lo <= k.encode() <= hi]
