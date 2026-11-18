# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the consumed capacity a PartiQL SELECT reports
when it reads several sort-key values or partitions by key.

Each sort-key value of an IN is a read of one item, billed whether the item
exists or not; each partition is a Query.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-03).
"""

from __future__ import annotations

import pytest

from conftest import scoped_table


@pytest.fixture(scope="module")
def xyz(dynamodb_client):
    """A table keyed on string pk and sk holding a/x, a/y and a/z."""
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
        for sk in ["x", "y", "z"]:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "a"}, "sk": {"S": sk}})
        yield name


@pytest.mark.parametrize(
    ("condition", "consistent", "units"),
    [
        ("pk = 'a' AND sk IN ['x', 'y']", False, 1.0),
        ("pk = 'a' AND sk IN ['x', 'y']", True, 2.0),
        ("pk = 'a' AND sk IN ['x', 'q']", False, 1.0),
        ("pk = 'a' AND sk IN ['x', 'y', 'z']", False, 1.5),
        ("pk = 'a'", False, 0.5),
        ("pk = 'a' AND sk = 'x'", False, 0.5),
        ("(pk = 'a' AND sk = 'x') OR (pk = 'a' AND sk = 'y')", False, 1.0),
        ("pk IN ['a', 'b']", False, 1.0),
    ],
)
def test_a_read_by_key_reports_the_capacity_of_its_reads(
    dynamodb_client, xyz, condition, consistent, units
):
    response = dynamodb_client.execute_statement(
        Statement=f'SELECT * FROM "{xyz}" WHERE {condition}',
        ReturnConsumedCapacity="TOTAL",
        ConsistentRead=consistent,
    )
    assert response["ConsumedCapacity"]["CapacityUnits"] == units
