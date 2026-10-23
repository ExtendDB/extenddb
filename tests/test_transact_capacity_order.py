# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Order of the ConsumedCapacity entries of a multi-table transaction.

Amazon DynamoDB documents no order for the ``ConsumedCapacity`` array of
``TransactGetItems`` and ``TransactWriteItems``. Measured on 2026-10-23 with
three single-hash-key tables whose names sort opposite to their creation order:
both operations listed the tables oldest first, for every request order tried
([newest, middle, oldest], [middle, newest, oldest], [oldest, newest, middle]).
The order follows neither the request nor the names.

The fixture below creates its three tables so that the name order is the
reverse of the creation order, waiting for ACTIVE between them, so a server
that ordered by name, or by first appearance in the request, fails these
tests. Dual-target: runs against the service and against ExtendDB on every
backend.
"""

from __future__ import annotations

import uuid

import pytest

from conftest import wait_for_active, wait_for_deleted


@pytest.fixture(scope="module")
def tables_oldest_first(dynamodb_client) -> list[str]:
    """Three tables, returned oldest first; their names sort newest first."""
    suffix = uuid.uuid4().hex[:12]
    names = [f"extenddb-test-c-{suffix}", f"extenddb-test-b-{suffix}", f"extenddb-test-a-{suffix}"]
    created: list[str] = []
    try:
        for name in names:
            dynamodb_client.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            created.append(name)
            # Each table is ACTIVE before the next is created, so the creation
            # times are distinct and in list order.
            wait_for_active(dynamodb_client, name)
        yield created
    finally:
        for name in created:
            try:
                dynamodb_client.delete_table(TableName=name)
            except dynamodb_client.exceptions.ResourceNotFoundException:
                continue
            wait_for_deleted(dynamodb_client, name)


def _request_orders(oldest_first: list[str]) -> list[list[str]]:
    oldest, middle, newest = oldest_first
    return [
        [newest, middle, oldest],
        [middle, newest, oldest],
        [oldest, newest, middle],
    ]


def _table_names(resp: dict) -> list[str]:
    return [c["TableName"] for c in resp["ConsumedCapacity"]]


class TestTransactionConsumedCapacityOrder:
    """Measured against Amazon DynamoDB on 2026-10-23."""

    def test_fixture_names_sort_opposite_to_creation(self, tables_oldest_first):
        assert sorted(tables_oldest_first) == list(reversed(tables_oldest_first))

    @pytest.mark.parametrize("granularity", ["TOTAL", "INDEXES"])
    def test_transact_get_items_lists_tables_oldest_first(
        self, dynamodb_client, tables_oldest_first, granularity
    ):
        for order in _request_orders(tables_oldest_first):
            resp = dynamodb_client.transact_get_items(
                ReturnConsumedCapacity=granularity,
                TransactItems=[{"Get": {"TableName": t, "Key": {"pk": {"S": "k"}}}} for t in order],
            )
            assert _table_names(resp) == tables_oldest_first, order

    @pytest.mark.parametrize("granularity", ["TOTAL", "INDEXES"])
    def test_transact_write_items_lists_tables_oldest_first(
        self, dynamodb_client, tables_oldest_first, granularity
    ):
        for n, order in enumerate(_request_orders(tables_oldest_first)):
            resp = dynamodb_client.transact_write_items(
                ReturnConsumedCapacity=granularity,
                TransactItems=[
                    {"Put": {"TableName": t, "Item": {"pk": {"S": f"{granularity}-{n}"}}}} for t in order
                ],
            )
            assert _table_names(resp) == tables_oldest_first, order

    def test_a_table_named_twice_has_one_entry_in_the_same_order(
        self, dynamodb_client, tables_oldest_first
    ):
        oldest, middle, newest = tables_oldest_first
        resp = dynamodb_client.transact_get_items(
            ReturnConsumedCapacity="TOTAL",
            TransactItems=[
                {"Get": {"TableName": newest, "Key": {"pk": {"S": "k1"}}}},
                {"Get": {"TableName": oldest, "Key": {"pk": {"S": "k1"}}}},
                {"Get": {"TableName": newest, "Key": {"pk": {"S": "k2"}}}},
                {"Get": {"TableName": middle, "Key": {"pk": {"S": "k1"}}}},
            ],
        )
        assert _table_names(resp) == tables_oldest_first
