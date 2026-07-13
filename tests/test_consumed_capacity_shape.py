# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""ConsumedCapacity response shape.

Real DynamoDB returns only ``CapacityUnits`` (top-level and in the nested
``Table`` breakdown) for these single-item and batch operations at both TOTAL
and INDEXES granularity — it does NOT emit granular ``ReadCapacityUnits`` /
``WriteCapacityUnits`` sub-fields. Verified against real DynamoDB (us-east-1).
"""

from __future__ import annotations

import pytest


@pytest.fixture()
def table(create_and_cleanup_table):
    return create_and_cleanup_table()["TableDescription"]["TableName"]


def _assert_only_capacity_units(cc: dict):
    assert "CapacityUnits" in cc, cc
    assert "ReadCapacityUnits" not in cc, cc
    assert "WriteCapacityUnits" not in cc, cc
    if "Table" in cc:
        tbl = cc["Table"]
        assert "CapacityUnits" in tbl, tbl
        assert "ReadCapacityUnits" not in tbl, tbl
        assert "WriteCapacityUnits" not in tbl, tbl


@pytest.mark.parametrize("granularity", ["TOTAL", "INDEXES"])
def test_get_item_consumed_capacity_shape(dynamodb_client, table, granularity):
    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "k1"}})
    resp = dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": "k1"}}, ReturnConsumedCapacity=granularity
    )
    _assert_only_capacity_units(resp["ConsumedCapacity"])


@pytest.mark.parametrize("granularity", ["TOTAL", "INDEXES"])
def test_put_item_consumed_capacity_shape(dynamodb_client, table, granularity):
    resp = dynamodb_client.put_item(
        TableName=table, Item={"pk": {"S": "k2"}}, ReturnConsumedCapacity=granularity
    )
    _assert_only_capacity_units(resp["ConsumedCapacity"])


def test_batch_get_consumed_capacity_shape(dynamodb_client, table):
    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "b1"}})
    resp = dynamodb_client.batch_get_item(
        RequestItems={table: {"Keys": [{"pk": {"S": "b1"}}]}},
        ReturnConsumedCapacity="TOTAL",
    )
    _assert_only_capacity_units(resp["ConsumedCapacity"][0])


def test_batch_write_consumed_capacity_shape(dynamodb_client, table):
    resp = dynamodb_client.batch_write_item(
        RequestItems={table: [{"PutRequest": {"Item": {"pk": {"S": "bw1"}}}}]},
        ReturnConsumedCapacity="TOTAL",
    )
    _assert_only_capacity_units(resp["ConsumedCapacity"][0])
