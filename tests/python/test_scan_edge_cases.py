# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Scan edge case tests: empty table, expression attribute names, numeric filters.

Covers scenarios from external suite: ScanEdgeCaseTests.
Tests run identically against real DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name


class TestScanEdgeCases:
    """Scan edge cases from external suite."""

    def test_scan_empty_table(self, table_factory, dynamodb_client):
        """Scan on empty table returns zero items."""
        name = table_factory()
        resp = dynamodb_client.scan(TableName=name)
        assert resp["Count"] == 0
        assert resp["Items"] == []

    def test_scan_all(self, table_factory, dynamodb_client):
        """Scan returns all items in a table."""
        name = table_factory()
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"k{i}"}, "v": {"N": str(i)}},
            )
        resp = dynamodb_client.scan(TableName=name)
        assert resp["Count"] == 10

    def test_scan_with_expression_attribute_names(self, table_factory, dynamodb_client):
        """Scan with ExpressionAttributeNames for reserved words."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "status": {"S": "active"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k2"}, "status": {"S": "inactive"}},
        )
        resp = dynamodb_client.scan(
            TableName=name,
            FilterExpression="#s = :val",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":val": {"S": "active"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["status"]["S"] == "active"

    def test_scan_with_numeric_filter(self, table_factory, dynamodb_client):
        """Scan with numeric comparison filter."""
        name = table_factory()
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"k{i}"}, "v": {"N": str(i)}},
            )
        resp = dynamodb_client.scan(
            TableName=name,
            FilterExpression="v > :min",
            ExpressionAttributeValues={":min": {"N": "5"}},
        )
        assert resp["Count"] == 4  # 6, 7, 8, 9

    def test_scan_with_multiple_filters(self, table_factory, dynamodb_client):
        """Scan with multiple filter conditions (AND)."""
        name = table_factory()
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": f"k{i}"},
                    "v": {"N": str(i)},
                    "cat": {"S": "even" if i % 2 == 0 else "odd"},
                },
            )
        resp = dynamodb_client.scan(
            TableName=name,
            FilterExpression="v > :min AND cat = :cat",
            ExpressionAttributeValues={
                ":min": {"N": "3"},
                ":cat": {"S": "even"},
            },
        )
        # Even numbers > 3: 4, 6, 8
        assert resp["Count"] == 3

    def test_scan_on_nonexistent_table(self, dynamodb_client):
        """Scan on nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.scan(TableName=unique_name("nonexistent"))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_scan_with_limit(self, table_factory, dynamodb_client):
        """Scan with Limit returns at most Limit items."""
        name = table_factory()
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"k{i}"}},
            )
        resp = dynamodb_client.scan(TableName=name, Limit=3)
        assert len(resp["Items"]) <= 3

    def test_scan_pagination(self, table_factory, dynamodb_client):
        """Paginate through all items using ExclusiveStartKey."""
        name = table_factory()
        for i in range(10):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"k{i}"}},
            )
        all_items = []
        kwargs = {"TableName": name, "Limit": 3}
        while True:
            resp = dynamodb_client.scan(**kwargs)
            all_items.extend(resp["Items"])
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert len(all_items) == 10

    def test_scan_with_filter_expression(self, table_factory, dynamodb_client):
        """Scan with FilterExpression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "color": {"S": "red"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k2"}, "color": {"S": "blue"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k3"}, "color": {"S": "red"}},
        )
        resp = dynamodb_client.scan(
            TableName=name,
            FilterExpression="color = :c",
            ExpressionAttributeValues={":c": {"S": "red"}},
        )
        assert resp["Count"] == 2

    def test_scan_with_projection_expression(self, table_factory, dynamodb_client):
        """Scan with ProjectionExpression returns only requested attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}},
        )
        resp = dynamodb_client.scan(
            TableName=name,
            ProjectionExpression="pk, a",
        )
        item = resp["Items"][0]
        assert "a" in item
        assert "b" not in item

    def test_scan_malformed_exclusive_start_key(self, table_factory, dynamodb_client):
        """Scan with ExclusiveStartKey that doesn't match schema returns ValidationException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=name,
                ExclusiveStartKey={"bad": {"S": "p"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid: The provided key element does not match the schema"

    def test_query_malformed_exclusive_start_key(self, table_factory, dynamodb_client):
        """Query with ExclusiveStartKey that doesn't match schema returns ValidationException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                ExclusiveStartKey={"bad": {"S": "p"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"

    def test_scan_malformed_exclusive_start_key_on_gsi(self, dynamodb_client):
        """Scan GSI with ExclusiveStartKey missing index key returns ValidationException."""
        name = unique_name("gsi-esk")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        # Start key has table PK but missing GSI PK
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=name,
                IndexName="gsi1",
                ExclusiveStartKey={"pk": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"
        dynamodb_client.delete_table(TableName=name)

    def test_query_malformed_exclusive_start_key_on_gsi(self, dynamodb_client):
        """Query GSI with ExclusiveStartKey missing index key returns ValidationException."""
        name = unique_name("gsi-esk-q")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = dynamodb_client.get_waiter("table_exists")
        waiter.wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
        # Start key has table PK but missing GSI PK
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                IndexName="gsi1",
                KeyConditionExpression="gsi_pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                ExclusiveStartKey={"pk": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"
        dynamodb_client.delete_table(TableName=name)

    def test_scan_empty_exclusive_start_key(self, table_factory, dynamodb_client):
        """Scan with an empty {} ExclusiveStartKey returns ValidationException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(TableName=name, ExclusiveStartKey={})
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "The provided starting key is invalid: "
            "The provided key element does not match the schema"
        )

    def test_query_empty_exclusive_start_key(self, table_factory, dynamodb_client):
        """Query with an empty {} ExclusiveStartKey returns ValidationException."""
        name = table_factory()
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                ExclusiveStartKey={},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"

    def test_scan_extra_attribute_in_exclusive_start_key(self, table_factory, dynamodb_client):
        """Scan with a valid composite start key plus an extra non-key attr."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=name,
                ExclusiveStartKey={
                    "pk": {"S": "P"},
                    "sk": {"S": "S"},
                    "extra": {"S": "junk"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "The provided starting key is invalid: "
            "The provided key element does not match the schema"
        )

    def test_query_extra_attribute_in_exclusive_start_key(self, table_factory, dynamodb_client):
        """Query with a valid composite start key plus an extra non-key attr."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "P"}},
                ExclusiveStartKey={
                    "pk": {"S": "P"},
                    "sk": {"S": "S"},
                    "extra": {"S": "junk"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"

    def test_scan_wrong_scalar_type_in_exclusive_start_key(
        self, table_factory, dynamodb_client
    ):
        """Scan with PK declared S but supplied as N raises ValidationException."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=name,
                ExclusiveStartKey={
                    "pk": {"N": "42"},
                    "sk": {"S": "S"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "The provided starting key is invalid: "
            "The provided key element does not match the schema"
        )

    def test_query_wrong_scalar_type_in_exclusive_start_key(
        self, table_factory, dynamodb_client
    ):
        """Query with PK declared S but supplied as N raises ValidationException."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "P"}},
                ExclusiveStartKey={
                    "pk": {"N": "42"},
                    "sk": {"S": "S"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"

    def test_scan_simple_table_extra_attribute_in_exclusive_start_key(
        self, table_factory, dynamodb_client
    ):
        """Scan on a single-key table rejects an extra sk in the start key."""
        name = table_factory()  # default: pk only
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=name,
                ExclusiveStartKey={"pk": {"S": "P"}, "sk": {"S": "S"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "The provided starting key is invalid: "
            "The provided key element does not match the schema"
        )

    def test_query_simple_table_extra_attribute_in_exclusive_start_key(
        self, table_factory, dynamodb_client
    ):
        """Query on a single-key table rejects an extra sk in the start key."""
        name = table_factory()  # default: pk only
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "P"}},
                ExclusiveStartKey={"pk": {"S": "P"}, "sk": {"S": "S"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "The provided starting key is invalid"

    def test_scan_full_composite_exclusive_start_key_is_accepted(
        self, table_factory, dynamodb_client
    ):
        """Sanity: a complete, well-typed ExclusiveStartKey paginates without error."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}, "v": {"N": "1"}},
        )
        resp = dynamodb_client.scan(
            TableName=name,
            ExclusiveStartKey={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        # Implementation detail: the storage layer is free to return zero or
        # more items past the cursor. Contract under test is "no error".
        assert "Items" in resp

    def test_query_full_composite_exclusive_start_key_is_accepted(
        self, table_factory, dynamodb_client
    ):
        """Sanity: a complete, well-typed Query ExclusiveStartKey paginates without error."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "P"}, "sk": {"S": "S"}, "v": {"N": "1"}},
        )
        resp = dynamodb_client.query(
            TableName=name,
            KeyConditionExpression="pk = :v",
            ExpressionAttributeValues={":v": {"S": "P"}},
            ExclusiveStartKey={"pk": {"S": "P"}, "sk": {"S": "S"}},
        )
        assert "Items" in resp


    def test_scan_rejects_negative_segment(self, table_factory, dynamodb_client):
        """Scan with negative Segment returns ValidationException."""
        import os
        import boto3
        import botocore.config

        name = table_factory()
        # Create a client without parameter validation to bypass boto3 client-side checks
        endpoint = os.environ.get("DYNAMODB_ENDPOINT")
        kwargs = {
            "service_name": "dynamodb",
            "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "config": botocore.config.Config(parameter_validation=False),
        }
        if endpoint:
            kwargs["endpoint_url"] = endpoint
            if endpoint.startswith("https://"):
                kwargs["verify"] = False
        raw_client = boto3.client(**kwargs)
        with pytest.raises(ClientError) as exc:
            raw_client.scan(TableName=name, Segment=-1, TotalSegments=4)
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "greater than or equal to 0" in err["Message"]
