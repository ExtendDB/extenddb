# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query and Scan operations.

Covers key conditions, filter expressions, pagination, consistent reads,
index queries, and parallel scan.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted


class TestQuery:
    """Query API tests."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_table(self, dynamodb_client, request):
        """Create a hash+range table with test data (class-scoped, read-only tests)."""
        name = unique_name("query")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)
        # Insert test data: 3 partitions, 5 items each
        for p in range(3):
            for s in range(5):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={
                        "pk": {"S": f"partition-{p}"},
                        "sk": {"S": f"sort-{s:03d}"},
                        "val": {"N": str(p * 10 + s)},
                        "category": {"S": "even" if s % 2 == 0 else "odd"},
                    },
                )
        request.cls.table = name
        request.cls.client = dynamodb_client
        yield
        try:
            dynamodb_client.delete_table(TableName=name)
        except Exception:
            pass
        try:
            wait_for_deleted(dynamodb_client, name)
        except Exception:
            pass

    def test_query_partition(self):
        """Query all items in a partition."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
        )
        assert resp["Count"] == 5
        assert len(resp["Items"]) == 5

    def test_query_key_condition_begins_with(self):
        """Query with begins_with on sort key."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": "partition-0"},
                ":prefix": {"S": "sort-00"},
            },
        )
        # sort-000 through sort-004 all start with "sort-00"
        assert resp["Count"] == 5
        for item in resp["Items"]:
            assert item["sk"]["S"].startswith("sort-00")

    def test_query_key_condition_between(self):
        """Query with BETWEEN on sort key."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "partition-0"},
                ":lo": {"S": "sort-001"},
                ":hi": {"S": "sort-003"},
            },
        )
        assert resp["Count"] == 3
        sks = [item["sk"]["S"] for item in resp["Items"]]
        assert sks == ["sort-001", "sort-002", "sort-003"]

    def test_query_key_condition_reversed_sort_key_comparison(self):
        """Query accepts a value placeholder on the left side of a sort-key comparison."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk AND :lo <= sk",
            ExpressionAttributeValues={
                ":pk": {"S": "partition-0"},
                ":lo": {"S": "sort-002"},
            },
        )
        assert resp["Count"] == 3
        sks = [item["sk"]["S"] for item in resp["Items"]]
        assert sks == ["sort-002", "sort-003", "sort-004"]

    def test_query_scan_index_forward_false(self):
        """Query with ScanIndexForward=False returns items in reverse order."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
            ScanIndexForward=False,
        )
        sks = [item["sk"]["S"] for item in resp["Items"]]
        assert sks == sorted(sks, reverse=True)

    def test_query_with_filter(self):
        """Query with FilterExpression."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="category = :cat",
            ExpressionAttributeValues={
                ":pk": {"S": "partition-0"},
                ":cat": {"S": "even"},
            },
        )
        # sort-000, sort-002, sort-004 are even
        assert resp["Count"] == 3
        for item in resp["Items"]:
            assert item["category"]["S"] == "even"

    def test_query_with_limit(self):
        """Query with Limit returns at most Limit items."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
            Limit=2,
        )
        assert len(resp["Items"]) == 2
        assert "LastEvaluatedKey" in resp

    def test_query_pagination(self):
        """Query with pagination collects all items."""
        all_items = []
        kwargs = {
            "TableName": self.table,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "partition-0"}},
            "Limit": 2,
        }
        while True:
            resp = self.client.query(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 5

    def test_query_consistent_read(self):
        """Query with ConsistentRead=True."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
            ConsistentRead=True,
        )
        assert resp["Count"] == 5

    def test_query_projection(self):
        """Query with ProjectionExpression."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
            ProjectionExpression="pk, sk",
        )
        for item in resp["Items"]:
            assert "pk" in item
            assert "sk" in item
            assert "val" not in item
            assert "category" not in item

    def test_query_select_count(self):
        """Query with Select=COUNT returns count without items."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
            Select="COUNT",
        )
        assert resp["Count"] == 5
        assert len(resp.get("Items", [])) == 0

    def test_query_empty_partition(self):
        """Query a partition with no items returns empty."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "nonexistent"}},
        )
        assert resp["Count"] == 0
        assert len(resp["Items"]) == 0

    def test_query_nonexistent_table(self):
        """Query a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            self.client.query(
                TableName=f"nonexistent-{unique_name()}",
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "x"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_query_expression_attribute_names(self):
        """Query using ExpressionAttributeNames for reserved words."""
        resp = self.client.query(
            TableName=self.table,
            KeyConditionExpression="#p = :pk",
            ExpressionAttributeNames={"#p": "pk"},
            ExpressionAttributeValues={":pk": {"S": "partition-0"}},
        )
        assert resp["Count"] == 5


class TestScan:
    """Scan API tests."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_table(self, dynamodb_client, request):
        """Create a table with test data for scan tests (class-scoped, read-only tests)."""
        name = unique_name("scan")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": f"item-{i:03d}"},
                    "val": {"N": str(i)},
                    "category": {"S": "even" if i % 2 == 0 else "odd"},
                },
            )
        request.cls.table = name
        request.cls.client = dynamodb_client
        yield
        try:
            dynamodb_client.delete_table(TableName=name)
        except Exception:
            pass
        try:
            wait_for_deleted(dynamodb_client, name)
        except Exception:
            pass

    def test_scan_all(self):
        """Scan returns all items."""
        resp = self.client.scan(TableName=self.table)
        assert resp["Count"] == 10

    def test_scan_with_filter(self):
        """Scan with FilterExpression."""
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="category = :cat",
            ExpressionAttributeValues={":cat": {"S": "even"}},
        )
        assert resp["Count"] == 5
        for item in resp["Items"]:
            assert item["category"]["S"] == "even"

    def test_scan_with_limit(self):
        """Scan with Limit."""
        resp = self.client.scan(TableName=self.table, Limit=3)
        assert len(resp["Items"]) == 3
        assert "LastEvaluatedKey" in resp

    def test_scan_pagination(self):
        """Scan with pagination collects all items."""
        all_items = []
        kwargs = {"TableName": self.table, "Limit": 3}
        while True:
            resp = self.client.scan(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 10

    def test_scan_projection(self):
        """Scan with ProjectionExpression."""
        resp = self.client.scan(
            TableName=self.table,
            ProjectionExpression="pk",
        )
        for item in resp["Items"]:
            assert "pk" in item
            assert "val" not in item

    def test_scan_select_count(self):
        """Scan with Select=COUNT."""
        resp = self.client.scan(TableName=self.table, Select="COUNT")
        assert resp["Count"] == 10
        assert len(resp.get("Items", [])) == 0

    def test_scan_consistent_read(self):
        """Scan with ConsistentRead=True."""
        resp = self.client.scan(TableName=self.table, ConsistentRead=True)
        assert resp["Count"] == 10

    def test_parallel_scan(self):
        """Parallel scan with TotalSegments/Segment."""
        total_segments = 3
        all_items = []
        for seg in range(total_segments):
            resp = self.client.scan(
                TableName=self.table,
                TotalSegments=total_segments,
                Segment=seg,
            )
            all_items.extend(resp["Items"])
        # All items should be covered across segments
        pks = {item["pk"]["S"] for item in all_items}
        assert len(pks) == 10

    def test_scan_filter_comparison_operators(self):
        """Scan with various comparison operators in FilterExpression."""
        # Greater than
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="val > :threshold",
            ExpressionAttributeValues={":threshold": {"N": "7"}},
        )
        assert resp["Count"] == 2  # items 8, 9

        # Less than or equal
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="val <= :threshold",
            ExpressionAttributeValues={":threshold": {"N": "2"}},
        )
        assert resp["Count"] == 3  # items 0, 1, 2

    def test_scan_filter_contains(self):
        """Scan with contains() in FilterExpression."""
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="contains(pk, :substr)",
            ExpressionAttributeValues={":substr": {"S": "005"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["pk"]["S"] == "item-005"

    def test_scan_filter_attribute_exists(self):
        """Scan with attribute_exists in FilterExpression."""
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="attribute_exists(val)",
        )
        assert resp["Count"] == 10

    def test_scan_filter_attribute_not_exists(self):
        """Scan with attribute_not_exists in FilterExpression."""
        resp = self.client.scan(
            TableName=self.table,
            FilterExpression="attribute_not_exists(nonexistent_attr)",
        )
        assert resp["Count"] == 10

    def test_scan_nonexistent_table(self):
        """Scan a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            self.client.scan(TableName=f"nonexistent-{unique_name()}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
