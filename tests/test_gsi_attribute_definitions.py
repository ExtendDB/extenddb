# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for issue #259.

Creating a GSI via UpdateTable must not corrupt the base table's stored
AttributeDefinitions. UpdateTable's AttributeDefinitions only describes the
attributes referenced by the new index; replacing the stored definitions
wholesale dropped the table's own key attribute definitions, after which
base-table GetItem/UpdateItem/DeleteItem lost the sort key and targeted an
arbitrary item under the same partition key.
"""

from __future__ import annotations

import time

from conftest import wait_for_active


def wait_for_gsi_active(client, table_name: str, index_name: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = client.describe_table(TableName=table_name)["Table"]
        indexes = {i["IndexName"]: i for i in desc.get("GlobalSecondaryIndexes", [])}
        if (
            desc["TableStatus"] == "ACTIVE"
            and indexes.get(index_name, {}).get("IndexStatus") == "ACTIVE"
        ):
            return
        time.sleep(0.2)
    raise TimeoutError(f"GSI {index_name} on {table_name} did not become ACTIVE within {timeout}s")


class TestGsiCreatePreservesAttributeDefinitions:
    def test_get_item_after_gsi_create_on_composite_table(
        self, create_and_cleanup_table, dynamodb_client, unique_table_name
    ):
        """Issue #259: base-table GetItem must honor the sort key after a GSI is added."""
        create_and_cleanup_table(
            unique_table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        )

        items = []
        for i in range(10):
            item = {
                "pk": {"S": "shared"},
                "sk": {"S": f"sk-{i:03d}"},
                "gpk": {"S": f"gpk-{i % 3}"},
                "gsk": {"S": f"gsk-{i:03d}"},
                "payload": {"S": f"payload-{i:03d}"},
            }
            dynamodb_client.put_item(TableName=unique_table_name, Item=item)
            items.append(item)

        # UpdateTable carries only the new index key attributes, per DynamoDB
        # semantics — the stored pk/sk definitions must survive the merge.
        dynamodb_client.update_table(
            TableName=unique_table_name,
            AttributeDefinitions=[
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "gsi01",
                        "KeySchema": [
                            {"AttributeName": "gpk", "KeyType": "HASH"},
                            {"AttributeName": "gsk", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )
        wait_for_gsi_active(dynamodb_client, unique_table_name, "gsi01")

        # DescribeTable must report the union of old and new definitions.
        desc = dynamodb_client.describe_table(TableName=unique_table_name)["Table"]
        defined = {ad["AttributeName"] for ad in desc["AttributeDefinitions"]}
        assert {"pk", "sk", "gpk", "gsk"} <= defined

        # Every base-table read must return exactly the requested item.
        for item in items:
            got = dynamodb_client.get_item(
                TableName=unique_table_name,
                Key={"pk": item["pk"], "sk": item["sk"]},
                ConsistentRead=True,
            ).get("Item")
            assert got == item

        # DeleteItem must also target the exact key, not another row in the
        # same partition.
        victim = items[5]
        dynamodb_client.delete_item(
            TableName=unique_table_name,
            Key={"pk": victim["pk"], "sk": victim["sk"]},
        )
        for item in items:
            got = dynamodb_client.get_item(
                TableName=unique_table_name,
                Key={"pk": item["pk"], "sk": item["sk"]},
                ConsistentRead=True,
            ).get("Item")
            assert got == (None if item is victim else item)
        wait_for_active(dynamodb_client, unique_table_name)
