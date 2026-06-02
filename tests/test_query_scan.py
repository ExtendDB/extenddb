# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 5 Query and Scan tests — dual-target against real DynamoDB and extenddb.

Covers: Query (KeyConditionExpression, FilterExpression, ProjectionExpression,
pagination, ScanIndexForward, Select=COUNT), Scan (FilterExpression,
ProjectionExpression, pagination, parallel scan), and error validation.
REQ-TEST-001, REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, scoped_table
@pytest.fixture(scope="class")
def query_table(dynamodb_client):
    """Create a hash+range (S,N) table with 10 items for query tests."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        for i in range(1, 11):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": "user-1"},
                    "sk": {"N": str(i)},
                    "name": {"S": f"item-{i}"},
                    "age": {"N": str(20 + i)},
                },
            )
        yield name
@pytest.fixture(scope="class")
def string_sk_table(dynamodb_client):
    """Create a hash+range (S,S) table with items for begins_with tests."""
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
        items = ["alpha-1", "alpha-2", "beta-1", "gamma-1"]
        for prefix in items:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "user-1"}, "sk": {"S": prefix}, "data": {"S": "v"}},
            )
        # Verify all items are visible before yielding to tests.
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
            ConsistentRead=True,
        )
        assert resp["Count"] == len(items), (
            f"string_sk_table fixture: expected {len(items)} items, got {resp['Count']}"
        )
        yield name
@pytest.fixture(scope="class")
def scan_table(dynamodb_client):
    """Create a hash-only table with 13 items for scan tests."""
    with scoped_table(dynamodb_client) as name:
        for i in range(1, 14):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": f"item-{i:03d}"},
                    "category": {"S": "a" if i <= 3 else "b"},
                },
            )
        yield name
class TestQuery:
    """Query operation tests."""

    def test_query_pk_only(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
        )
        assert resp["Count"] == 10
        assert resp["ScannedCount"] == 10

    def test_query_sk_eq(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk AND sk = :sk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}, ":sk": {"N": "5"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["name"] == {"S": "item-5"}

    def test_query_sk_lt(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk AND sk < :sk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}, ":sk": {"N": "4"}},
        )
        assert resp["Count"] == 3

    def test_query_reversed_sk_comparison(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk AND :lo <= sk",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":lo": {"N": "3"},
            },
        )
        assert resp["Count"] == 8

    def test_query_sk_between(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":lo": {"N": "3"},
                ":hi": {"N": "7"},
            },
        )
        assert resp["Count"] == 5

    def test_query_begins_with(self, dynamodb_client, string_sk_table):
        resp = dynamodb_client.query(
            TableName=string_sk_table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":prefix": {"S": "alpha"},
            },
        )
        assert resp["Count"] == 2

    def test_query_reverse_order(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
            ScanIndexForward=False,
        )
        sks = [int(item["sk"]["N"]) for item in resp["Items"]]
        assert sks == list(range(10, 0, -1))

    def test_query_limit(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
            Limit=3,
        )
        assert resp["Count"] == 3
        assert "LastEvaluatedKey" in resp

    def test_query_pagination(self, dynamodb_client, query_table):
        all_items: list = []
        kwargs: dict = {
            "TableName": query_table,
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "user-1"}},
            "Limit": 3,
        }
        while True:
            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 10

    def test_query_filter_expression(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="age > :min_age",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":min_age": {"N": "25"},
            },
        )
        assert resp["Count"] == 5
        assert resp["ScannedCount"] == 10

    def test_query_projection(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk AND sk = :sk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}, ":sk": {"N": "1"}},
            ProjectionExpression="#n",
            ExpressionAttributeNames={"#n": "name"},
        )
        item = resp["Items"][0]
        # Only projected attributes returned — keys not auto-included
        assert "name" in item
        assert "pk" not in item
        assert "sk" not in item
        assert "age" not in item

    def test_query_select_count(self, dynamodb_client, query_table):
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
            Select="COUNT",
        )
        assert resp["Count"] == 10
        assert "Items" not in resp or resp["Items"] is None

    def test_query_select_count_with_filter(self, dynamodb_client, query_table):
        """COUNT with FilterExpression: Count = filtered items, ScannedCount = total read."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="age > :min_age",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":min_age": {"N": "25"},
            },
            Select="COUNT",
        )
        assert resp["Count"] == 5
        assert resp["ScannedCount"] == 10

    def test_query_missing_kce(self, dynamodb_client, query_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(TableName=query_table)
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_ne_operator_rejected_in_kce(self, dynamodb_client, query_table):
        """DynamoDB rejects <> in KeyConditionExpression (only =, <, <=, >, >=, BETWEEN, begins_with)."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk AND sk <> :val",
                ExpressionAttributeValues={
                    ":pk": {"S": "user-1"},
                    ":val": {"S": "x"},
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"
class TestScan:
    """Scan operation tests."""

    def test_scan_full_table(self, dynamodb_client, scan_table):
        resp = dynamodb_client.scan(TableName=scan_table)
        assert resp["Count"] == 13

    def test_scan_filter_expression(self, dynamodb_client, scan_table):
        resp = dynamodb_client.scan(
            TableName=scan_table,
            FilterExpression="category = :cat",
            ExpressionAttributeValues={":cat": {"S": "a"}},
        )
        assert resp["Count"] == 3

    def test_scan_limit(self, dynamodb_client, scan_table):
        resp = dynamodb_client.scan(TableName=scan_table, Limit=5)
        assert resp["Count"] == 5
        assert "LastEvaluatedKey" in resp

    def test_scan_pagination(self, dynamodb_client, scan_table):
        all_items: list = []
        kwargs: dict = {"TableName": scan_table, "Limit": 4}
        while True:
            resp = dynamodb_client.scan(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 13

    def test_scan_projection(self, dynamodb_client, scan_table):
        resp = dynamodb_client.scan(
            TableName=scan_table,
            ProjectionExpression="pk",
            Limit=1,
        )
        item = resp["Items"][0]
        assert "pk" in item
        assert "category" not in item

    def test_scan_parallel(self, dynamodb_client, scan_table):
        total_segments = 3
        all_items: list = []
        for seg in range(total_segments):
            resp = dynamodb_client.scan(
                TableName=scan_table,
                Segment=seg,
                TotalSegments=total_segments,
            )
            all_items.extend(resp["Items"])
        assert len(all_items) == 13

    def test_scan_segment_without_total(self, dynamodb_client, scan_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(TableName=scan_table, Segment=0)
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_scan_total_without_segment(self, dynamodb_client, scan_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(TableName=scan_table, TotalSegments=3)
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# Query validation additions (covers commits since 6b98234dcf)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def binary_sk_table(dynamodb_client):
    """Create a hash+range (S,B) table for binary sort key tests."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "B"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        # Insert items with binary sort keys sharing a common prefix.
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "bin"}, "sk": {"B": b"\x01\x02\x03"}, "v": {"S": "a"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "bin"}, "sk": {"B": b"\x01\x02\x04"}, "v": {"S": "b"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "bin"}, "sk": {"B": b"\x02\x00\x00"}, "v": {"S": "c"}},
        )
        yield name


@pytest.fixture(scope="class")
def lsi_table(dynamodb_client):
    """Create a table with an LSI for synchronous write verification."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsi_sk", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi-index",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        yield name


class TestQueryValidation:
    """Query validation edge cases from recent fixes."""

    def test_query_kce_with_parentheses(self, dynamodb_client, query_table):
        """KeyConditionExpression with parentheses is accepted."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="(pk = :pk) AND (sk > :sk)",
            ExpressionAttributeValues={":pk": {"S": "user-1"}, ":sk": {"N": "8"}},
        )
        assert resp["Count"] == 2

    def test_query_kce_must_reference_partition_key(self, dynamodb_client, query_table):
        """KCE that doesn't reference the partition key is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="sk > :sk",
                ExpressionAttributeValues={":sk": {"N": "5"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_undefined_name_in_filter_expression(self, dynamodb_client, query_table):
        """FilterExpression referencing undefined #name fails before execution."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                FilterExpression="#undefined > :v",
                ExpressionAttributeValues={":pk": {"S": "user-1"}, ":v": {"N": "1"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_unused_expression_attribute_names(self, dynamodb_client, query_table):
        """Extra ExpressionAttributeNames not referenced in any expression are rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "user-1"}},
                ExpressionAttributeNames={"#unused": "some_attr"},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_unused_expression_attribute_values(self, dynamodb_client, query_table):
        """Extra ExpressionAttributeValues not referenced in any expression are rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "user-1"}, ":unused": {"N": "99"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_filter_size_on_missing_attribute(self, dynamodb_client, query_table):
        """size() on a missing attribute does not match (returns None, not 0)."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="size(nonexistent_attr) = :zero",
            ExpressionAttributeValues={":pk": {"S": "user-1"}, ":zero": {"N": "0"}},
        )
        # No items should match — size(missing) is not 0.
        assert resp["Count"] == 0

    def test_query_begins_with_binary_sort_key(self, dynamodb_client, binary_sk_table):
        """begins_with on binary sort key returns correct results (not ISE)."""
        resp = dynamodb_client.query(
            TableName=binary_sk_table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": "bin"},
                ":prefix": {"B": b"\x01\x02"},
            },
        )
        # Should match the two items with prefix \x01\x02.
        assert resp["Count"] == 2
        values = sorted(item["v"]["S"] for item in resp["Items"])
        assert values == ["a", "b"]

    def test_query_reserved_keyword_without_alias(self, dynamodb_client, query_table):
        """Using a reserved keyword without #alias in FilterExpression is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                FilterExpression="name > :v",
                ExpressionAttributeValues={":pk": {"S": "user-1"}, ":v": {"S": "a"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_all_projected_attributes_without_index_rejected(
        self, dynamodb_client, query_table
    ):
        """Select=ALL_PROJECTED_ATTRIBUTES on the base table is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "user-1"}},
                Select="ALL_PROJECTED_ATTRIBUTES",
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert (
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
            in err["Message"]
        )


class TestScanValidation:
    """Scan validation edge cases from recent fixes."""

    def test_scan_negative_segment(self, dynamodb_client_no_validation, scan_table):
        """Negative Segment value is rejected by the service."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=scan_table,
                Segment=-1,
                TotalSegments=3,
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_scan_unused_expression_attribute_names(self, dynamodb_client, scan_table):
        """Extra ExpressionAttributeNames not referenced in any expression are rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=scan_table,
                FilterExpression="category = :cat",
                ExpressionAttributeValues={":cat": {"S": "a"}},
                ExpressionAttributeNames={"#unused": "something"},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_scan_unused_expression_attribute_values(self, dynamodb_client, scan_table):
        """Extra ExpressionAttributeValues not referenced in any expression are rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=scan_table,
                FilterExpression="category = :cat",
                ExpressionAttributeValues={":cat": {"S": "a"}, ":extra": {"N": "1"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_scan_segment_at_max_accepted(self, dynamodb_client, scan_table):
        """Segment=999999 with TotalSegments=1000000 is at the documented bound."""
        dynamodb_client.scan(
            TableName=scan_table, Segment=999_999, TotalSegments=1_000_000
        )

    def test_scan_segment_one_over_max_rejected(
        self, dynamodb_client_no_validation, scan_table
    ):
        """Segment > 999999 returns the Coral-format ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=scan_table, Segment=1_000_000, TotalSegments=1_000_000
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        msg = err["Message"]
        assert "1 validation error detected" in msg
        assert "'1000000' at 'segment'" in msg
        assert "Member must have value less than or equal to 999999" in msg

    def test_scan_total_segments_at_max_accepted(self, dynamodb_client, scan_table):
        """TotalSegments=1000000 with Segment=0 is at the documented bound."""
        dynamodb_client.scan(
            TableName=scan_table, Segment=0, TotalSegments=1_000_000
        )

    def test_scan_total_segments_one_over_max_rejected(
        self, dynamodb_client_no_validation, scan_table
    ):
        """TotalSegments > 1000000 returns the Coral-format ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=scan_table, Segment=0, TotalSegments=1_000_001
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        msg = err["Message"]
        assert "1 validation error detected" in msg
        assert "'1000001' at 'totalSegments'" in msg
        assert "Member must have value less than or equal to 1000000" in msg

    def test_scan_all_projected_attributes_without_index_rejected(
        self, dynamodb_client, scan_table
    ):
        """Select=ALL_PROJECTED_ATTRIBUTES on a base-table scan is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=scan_table, Select="ALL_PROJECTED_ATTRIBUTES"
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert (
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
            in err["Message"]
        )


# ---------------------------------------------------------------------------
# Legacy API tests (pre-expression parameters)
# ---------------------------------------------------------------------------


class TestLegacyQuery:
    """Tests for legacy Query parameters (KeyConditions, QueryFilter, AttributesToGet)."""

    def test_query_key_conditions_eq(self, dynamodb_client, query_table):
        """KeyConditions with EQ operator returns correct results."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "user-1"}],
                    "ComparisonOperator": "EQ",
                },
                "sk": {
                    "AttributeValueList": [{"N": "5"}],
                    "ComparisonOperator": "EQ",
                },
            },
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["name"] == {"S": "item-5"}

    def test_query_key_conditions_between(self, dynamodb_client, query_table):
        """KeyConditions with BETWEEN operator."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "user-1"}],
                    "ComparisonOperator": "EQ",
                },
                "sk": {
                    "AttributeValueList": [{"N": "3"}, {"N": "7"}],
                    "ComparisonOperator": "BETWEEN",
                },
            },
        )
        assert resp["Count"] == 5

    def test_query_key_conditions_begins_with(self, dynamodb_client, string_sk_table):
        """KeyConditions with BEGINS_WITH operator."""
        resp = dynamodb_client.query(
            TableName=string_sk_table,
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "user-1"}],
                    "ComparisonOperator": "EQ",
                },
                "sk": {
                    "AttributeValueList": [{"S": "alpha"}],
                    "ComparisonOperator": "BEGINS_WITH",
                },
            },
        )
        assert resp["Count"] == 2

    def test_query_filter_legacy(self, dynamodb_client, query_table):
        """QueryFilter parameter filters results."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "user-1"}],
                    "ComparisonOperator": "EQ",
                },
            },
            QueryFilter={
                "age": {
                    "AttributeValueList": [{"N": "25"}],
                    "ComparisonOperator": "GT",
                },
            },
        )
        assert resp["Count"] == 5
        assert resp["ScannedCount"] == 10

    def test_query_attributes_to_get(self, dynamodb_client, query_table):
        """AttributesToGet returns only specified attributes."""
        resp = dynamodb_client.query(
            TableName=query_table,
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "user-1"}],
                    "ComparisonOperator": "EQ",
                },
                "sk": {
                    "AttributeValueList": [{"N": "1"}],
                    "ComparisonOperator": "EQ",
                },
            },
            AttributesToGet=["name"],
        )
        item = resp["Items"][0]
        assert "name" in item
        assert "age" not in item

    def test_query_key_conditions_with_kce_rejected(self, dynamodb_client, query_table):
        """Mixing KeyConditions with KeyConditionExpression is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditions={
                    "pk": {
                        "AttributeValueList": [{"S": "user-1"}],
                        "ComparisonOperator": "EQ",
                    },
                },
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "user-1"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_query_filter_with_filter_expression_rejected(self, dynamodb_client, query_table):
        """Mixing QueryFilter with FilterExpression is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=query_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "user-1"}, ":v": {"N": "25"}},
                FilterExpression="age > :v",
                QueryFilter={
                    "age": {
                        "AttributeValueList": [{"N": "25"}],
                        "ComparisonOperator": "GT",
                    },
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestLegacyScan:
    """Tests for legacy Scan parameters (ScanFilter, AttributesToGet)."""

    def test_scan_filter_legacy(self, dynamodb_client, scan_table):
        """ScanFilter parameter filters results."""
        resp = dynamodb_client.scan(
            TableName=scan_table,
            ScanFilter={
                "category": {
                    "AttributeValueList": [{"S": "a"}],
                    "ComparisonOperator": "EQ",
                },
            },
        )
        assert resp["Count"] == 3

    def test_scan_attributes_to_get(self, dynamodb_client, scan_table):
        """AttributesToGet returns only specified attributes."""
        resp = dynamodb_client.scan(
            TableName=scan_table,
            AttributesToGet=["pk"],
            Limit=1,
        )
        item = resp["Items"][0]
        assert "pk" in item
        assert "category" not in item

    def test_scan_filter_with_filter_expression_rejected(self, dynamodb_client, scan_table):
        """Mixing ScanFilter with FilterExpression is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=scan_table,
                FilterExpression="category = :cat",
                ExpressionAttributeValues={":cat": {"S": "a"}},
                ScanFilter={
                    "category": {
                        "AttributeValueList": [{"S": "a"}],
                        "ComparisonOperator": "EQ",
                    },
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# LSI synchronous write verification
# ---------------------------------------------------------------------------


class TestQueryLSI:
    """LSI queries return data immediately (synchronous write)."""

    def test_lsi_query_immediately_consistent(self, dynamodb_client, lsi_table):
        """Write to a table with LSI, immediately query LSI — item is visible."""
        dynamodb_client.put_item(
            TableName=lsi_table,
            Item={
                "pk": {"S": "lsi-user"},
                "sk": {"S": "profile"},
                "lsi_sk": {"N": "100"},
                "data": {"S": "hello"},
            },
        )
        # Single query — no polling. LSI writes are synchronous.
        resp = dynamodb_client.query(
            TableName=lsi_table,
            IndexName="lsi-index",
            KeyConditionExpression="pk = :pk AND lsi_sk = :sk",
            ExpressionAttributeValues={
                ":pk": {"S": "lsi-user"},
                ":sk": {"N": "100"},
            },
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["data"]["S"] == "hello"


# ---------------------------------------------------------------------------
# LSI pagination with duplicate index sort keys (issue #145)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def lsi_pagination_table(dynamodb_client):
    """Table with LSI and items sharing the same index sort key value."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "taskType", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "TaskTypeLSI",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "taskType", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        # Insert 5 items with same pk and same LSI sort key, different base SK
        for i in range(1, 6):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": "user1"},
                    "sk": {"S": f"task{i}"},
                    "taskType": {"S": "EXPORT"},
                    "data": {"S": f"payload-{i}"},
                },
            )
        yield name


class TestLSIPaginationDuplicateSortKeys:
    """LSI pagination must work when multiple items share the same index sort key."""

    def test_paginate_all_items_with_limit(self, dynamodb_client, lsi_pagination_table):
        """Paginating through all items with Limit returns all 5 items across pages."""
        all_items = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": lsi_pagination_table,
                "IndexName": "TaskTypeLSI",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "user1"}},
                "Limit": 2,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_items) == 5
        # Verify all items are present
        sks = sorted(item["sk"]["S"] for item in all_items)
        assert sks == ["task1", "task2", "task3", "task4", "task5"]

    def test_paginate_reverse_order(self, dynamodb_client, lsi_pagination_table):
        """ScanIndexForward=False with pagination returns all items in reverse."""
        all_items = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": lsi_pagination_table,
                "IndexName": "TaskTypeLSI",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "user1"}},
                "Limit": 2,
                "ScanIndexForward": False,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_items) == 5
        # Verify reverse order by base table sort key
        sks = [item["sk"]["S"] for item in all_items]
        assert sks == ["task5", "task4", "task3", "task2", "task1"]

    def test_page_two_returns_items(self, dynamodb_client, lsi_pagination_table):
        """Second page of LSI query with duplicate sort keys returns items (not 0)."""
        # First page
        resp1 = dynamodb_client.query(
            TableName=lsi_pagination_table,
            IndexName="TaskTypeLSI",
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user1"}},
            Limit=2,
        )
        assert resp1["Count"] == 2
        assert "LastEvaluatedKey" in resp1

        # Second page — this is the core regression test for issue #145
        resp2 = dynamodb_client.query(
            TableName=lsi_pagination_table,
            IndexName="TaskTypeLSI",
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user1"}},
            Limit=2,
            ExclusiveStartKey=resp1["LastEvaluatedKey"],
        )
        assert resp2["Count"] == 2, (
            f"Page 2 should return 2 items but got {resp2['Count']}. "
            "ExclusiveStartKey pagination with duplicate index sort keys is broken."
        )

    def test_last_evaluated_key_contains_all_keys(self, dynamodb_client, lsi_pagination_table):
        """LastEvaluatedKey for LSI query contains pk, sk (base), and taskType (index)."""
        resp = dynamodb_client.query(
            TableName=lsi_pagination_table,
            IndexName="TaskTypeLSI",
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user1"}},
            Limit=2,
        )
        lek = resp["LastEvaluatedKey"]
        assert "pk" in lek, "LastEvaluatedKey must contain partition key"
        assert "sk" in lek, "LastEvaluatedKey must contain base table sort key"
        assert "taskType" in lek, "LastEvaluatedKey must contain index sort key"


# ---------------------------------------------------------------------------
# Hash-only GSI pagination (related to issue #145)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def gsi_hash_only_table(dynamodb_client):
    """Table with a hash-only GSI for pagination tests."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "instanceId", "AttributeType": "S"},
            {"AttributeName": "nodeStatus", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "instanceId", "KeyType": "HASH"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "StatusGSI",
                "KeySchema": [
                    {"AttributeName": "nodeStatus", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        # Insert 10 items with same GSI hash key
        for i in range(1, 11):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "instanceId": {"S": f"node-{i}"},
                    "nodeStatus": {"S": "ACTIVE"},
                    "data": {"S": f"payload-{i}"},
                },
            )
        yield name


class TestHashOnlyGSIPagination:
    """Hash-only GSI pagination must work with ExclusiveStartKey."""

    def test_paginate_all_items_with_limit(self, dynamodb_client, gsi_hash_only_table):
        """Paginating through all items on a hash-only GSI returns all 10 items."""
        all_items = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": gsi_hash_only_table,
                "IndexName": "StatusGSI",
                "KeyConditionExpression": "nodeStatus = :s",
                "ExpressionAttributeValues": {":s": {"S": "ACTIVE"}},
                "Limit": 3,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_items) == 10
        ids = sorted(item["instanceId"]["S"] for item in all_items)
        expected = sorted(f"node-{i}" for i in range(1, 11))
        assert ids == expected

    def test_page_two_returns_items(self, dynamodb_client, gsi_hash_only_table):
        """Second page of hash-only GSI query returns items (not 0)."""
        resp1 = dynamodb_client.query(
            TableName=gsi_hash_only_table,
            IndexName="StatusGSI",
            KeyConditionExpression="nodeStatus = :s",
            ExpressionAttributeValues={":s": {"S": "ACTIVE"}},
            Limit=3,
        )
        assert resp1["Count"] == 3
        assert "LastEvaluatedKey" in resp1

        resp2 = dynamodb_client.query(
            TableName=gsi_hash_only_table,
            IndexName="StatusGSI",
            KeyConditionExpression="nodeStatus = :s",
            ExpressionAttributeValues={":s": {"S": "ACTIVE"}},
            Limit=3,
            ExclusiveStartKey=resp1["LastEvaluatedKey"],
        )
        assert resp2["Count"] == 3, (
            f"Page 2 should return 3 items but got {resp2['Count']}. "
            "Hash-only GSI pagination with ExclusiveStartKey is broken."
        )

    def test_no_duplicates_across_pages(self, dynamodb_client, gsi_hash_only_table):
        """Pagination does not return duplicate items across pages."""
        all_ids = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": gsi_hash_only_table,
                "IndexName": "StatusGSI",
                "KeyConditionExpression": "nodeStatus = :s",
                "ExpressionAttributeValues": {":s": {"S": "ACTIVE"}},
                "Limit": 2,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.query(**kwargs)
            all_ids.extend(item["instanceId"]["S"] for item in resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_ids) == len(set(all_ids)), "Duplicate items found across pages"


# ---------------------------------------------------------------------------
# GSI pagination on hash-only base table (reviewer comment #4)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def gsi_on_hash_only_base_table(dynamodb_client):
    """Table with hash-only PK (no range key) and a GSI with a sort key."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "itemId", "AttributeType": "S"},
            {"AttributeName": "category", "AttributeType": "S"},
            {"AttributeName": "priority", "AttributeType": "N"},
        ],
        key_schema=[
            {"AttributeName": "itemId", "KeyType": "HASH"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "CategoryPriorityGSI",
                "KeySchema": [
                    {"AttributeName": "category", "KeyType": "HASH"},
                    {"AttributeName": "priority", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        # Insert items with duplicate GSI sort keys (same priority)
        for i in range(1, 8):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "itemId": {"S": f"item-{i}"},
                    "category": {"S": "urgent"},
                    "priority": {"N": "1"},
                    "data": {"S": f"payload-{i}"},
                },
            )
        yield name


class TestGSIOnHashOnlyBaseTable:
    """GSI pagination works when the base table has no range key."""

    def test_paginate_duplicate_gsi_sort_keys(self, dynamodb_client, gsi_on_hash_only_base_table):
        """All 7 items with same GSI sort key are returned through pagination."""
        all_items = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": gsi_on_hash_only_base_table,
                "IndexName": "CategoryPriorityGSI",
                "KeyConditionExpression": "category = :c",
                "ExpressionAttributeValues": {":c": {"S": "urgent"}},
                "Limit": 2,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.query(**kwargs)
            all_items.extend(resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_items) == 7
        ids = sorted(item["itemId"]["S"] for item in all_items)
        assert ids == sorted(f"item-{i}" for i in range(1, 8))

    def test_scan_pagination_on_gsi(self, dynamodb_client, gsi_on_hash_only_base_table):
        """Scan on GSI with Limit paginates correctly over hash-only base table."""
        all_items = []
        exclusive_start_key = None

        while True:
            kwargs = {
                "TableName": gsi_on_hash_only_base_table,
                "IndexName": "CategoryPriorityGSI",
                "Limit": 3,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            resp = dynamodb_client.scan(**kwargs)
            all_items.extend(resp["Items"])

            if "LastEvaluatedKey" not in resp:
                break
            exclusive_start_key = resp["LastEvaluatedKey"]

        assert len(all_items) == 7
        ids = sorted(item["itemId"]["S"] for item in all_items)
        assert ids == sorted(f"item-{i}" for i in range(1, 8))

    def test_repeated_pagination_consistent(self, dynamodb_client, gsi_on_hash_only_base_table):
        """Multiple paginated queries produce consistent results (exercises cache path).

        The first query populates the internal base_key_cache. Subsequent queries
        hit the cache. Any cache corruption would cause pagination to break.
        """
        for run in range(5):
            all_items = []
            exclusive_start_key = None
            while True:
                kwargs = {
                    "TableName": gsi_on_hash_only_base_table,
                    "IndexName": "CategoryPriorityGSI",
                    "KeyConditionExpression": "category = :c",
                    "ExpressionAttributeValues": {":c": {"S": "urgent"}},
                    "Limit": 3,
                }
                if exclusive_start_key:
                    kwargs["ExclusiveStartKey"] = exclusive_start_key
                resp = dynamodb_client.query(**kwargs)
                all_items.extend(resp["Items"])
                if "LastEvaluatedKey" not in resp:
                    break
                exclusive_start_key = resp["LastEvaluatedKey"]
            assert len(all_items) == 7, (
                f"Run {run+1}: expected 7 items but got {len(all_items)}"
            )
