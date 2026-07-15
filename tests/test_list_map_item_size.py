# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""List/Map write sizing: ConsumedCapacity and the 400 KB item-size limit.

DynamoDB charges a per-element overhead for every List element and a per-entry
overhead for every Map entry, on top of the 3-byte container overhead. Because
that per-element overhead feeds both write ConsumedCapacity and the 400 KB
item-size limit, an undercount makes dense list/map items report too little
capacity and lets some oversized nested items slip past the size limit.

All expected values verified against real DynamoDB (us-east-1) with
ReturnConsumedCapacity=TOTAL.
"""

from __future__ import annotations

import botocore.exceptions
import pytest


@pytest.fixture()
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]["TableName"]


def _capacity_units(resp: dict) -> float:
    return resp["ConsumedCapacity"]["CapacityUnits"]


# --- Write ConsumedCapacity for dense lists ------------------------------------
# Each element of {"S": "aa"} is 2 value bytes + 1 byte per-element overhead.


@pytest.mark.parametrize(
    ("count", "expected_cu"),
    [(500, 2.0), (1500, 5.0), (3000, 9.0)],
)
def test_put_item_dense_list_capacity(dynamodb_client, table, count, expected_cu):
    resp = dynamodb_client.put_item(
        TableName=table,
        Item={"pk": {"S": f"l{count}"}, "data": {"L": [{"S": "aa"}] * count}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert _capacity_units(resp) == expected_cu


def test_put_item_dense_map_capacity(dynamodb_client, table):
    # 800 entries "k0".."k799", each value {"S": "v"} (1 value byte),
    # plus 1 byte per-entry overhead. Real DynamoDB reports 5.0.
    item = {
        "pk": {"S": "m800"},
        "data": {"M": {f"k{i}": {"S": "v"} for i in range(800)}},
    }
    resp = dynamodb_client.put_item(
        TableName=table, Item=item, ReturnConsumedCapacity="TOTAL"
    )
    assert _capacity_units(resp) == 5.0


def test_put_item_nested_list_of_maps_capacity(dynamodb_client, table):
    # Overhead compounds recursively: inner map {"a":{"S":"b"}} = 3 + (1+1+1) = 6,
    # each list element = 6 + 1 = 7, list = 3 + 500*7. Real DynamoDB reports 4.0.
    item = {
        "pk": {"S": "nest"},
        "data": {"L": [{"M": {"a": {"S": "b"}}} for _ in range(500)]},
    }
    resp = dynamodb_client.put_item(
        TableName=table, Item=item, ReturnConsumedCapacity="TOTAL"
    )
    assert _capacity_units(resp) == 4.0


def test_put_item_nested_map_of_lists_capacity(dynamodb_client, table):
    # Map entries each hold a one-element list; per-entry and per-element
    # overhead both apply. Real DynamoDB reports 5.0.
    item = {
        "pk": {"S": "mapl"},
        "data": {"M": {f"k{i}": {"L": [{"S": "b"}]} for i in range(500)}},
    }
    resp = dynamodb_client.put_item(
        TableName=table, Item=item, ReturnConsumedCapacity="TOTAL"
    )
    assert _capacity_units(resp) == 5.0


def test_update_item_set_list_capacity(dynamodb_client, table):
    # The write-path plumbing (UpdateItem) derives capacity from the same
    # sizing as PutItem: SET of a 1500-element list is 5.0 on both.
    resp = dynamodb_client.update_item(
        TableName=table,
        Key={"pk": {"S": "upd"}},
        UpdateExpression="SET #d = :v",
        ExpressionAttributeNames={"#d": "data"},
        ExpressionAttributeValues={":v": {"L": [{"S": "aa"}] * 1500}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert _capacity_units(resp) == 5.0


def test_put_item_scalar_capacity_unchanged(dynamodb_client, table):
    # Control: an item with no list/map is one WCU and must stay one WCU.
    resp = dynamodb_client.put_item(
        TableName=table,
        Item={"pk": {"S": "scalar"}, "n": {"N": "1"}, "s": {"S": "hello"}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert _capacity_units(resp) == 1.0


# --- 400 KB item-size limit driven by the same per-element sizing --------------


def test_large_nested_list_under_limit_accepted(dynamodb_client, table):
    # ~390 KB with per-element overhead counted: just under 400 KB, accepted,
    # and 381 WCU. Guards against the fix over-rejecting at the boundary.
    resp = dynamodb_client.put_item(
        TableName=table,
        Item={"pk": {"S": "under"}, "data": {"L": [{"S": "aa"}] * 130_000}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert _capacity_units(resp) == 381.0


def test_oversized_nested_list_rejected(dynamodb_client, table):
    # ~450 KB once per-element overhead is counted. Real DynamoDB rejects this
    # with an item-size ValidationException; ExtendDB must too. Without the
    # per-element overhead the summed size is only ~300 KB and slips past.
    with pytest.raises(botocore.exceptions.ClientError) as exc:
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": "over"}, "data": {"L": [{"S": "aa"}] * 150_000}},
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "Item size has exceeded the maximum allowed size" in err["Message"]
