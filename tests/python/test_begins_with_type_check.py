# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""begins_with operand type validation — upfront rejection.

Verifies that begins_with(path, value) rejects invalid operand types
before any rows are evaluated, so empty scans/queries still fail.
Covers issue #132.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active


class TestBeginsWithOperandTypeCheck:
    """begins_with rejects invalid operand types upfront."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_table(self, dynamodb_client, request):
        """Create an empty table (no items)."""
        name = unique_name("bw_type")
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
        request.cls.table = name
        yield
        dynamodb_client.delete_table(TableName=name)

    @pytest.mark.parametrize("val,type_code", [
        ({"N": "1"}, "N"),
        ({"BOOL": True}, "BOOL"),
        ({"NULL": True}, "NULL"),
        ({"L": []}, "L"),
        ({"M": {}}, "M"),
        ({"SS": ["a"]}, "SS"),
        ({"NS": ["1"]}, "NS"),
        ({"BS": [b"a"]}, "BS"),
    ])
    def test_scan_empty_table_rejects_invalid_type(self, dynamodb_client, val, type_code):
        """Scan on empty table rejects begins_with with invalid operand type."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=self.table,
                FilterExpression="begins_with(#a, :v)",
                ExpressionAttributeNames={"#a": "pk"},
                ExpressionAttributeValues={":v": val},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "Invalid FilterExpression" in err["Message"]
        assert f"operand type: {type_code}" in err["Message"]
        assert "begins_with" in err["Message"]

    def test_scan_empty_table_accepts_string(self, dynamodb_client):
        """Scan with begins_with using string operand succeeds."""
        resp = dynamodb_client.scan(
            TableName=self.table,
            FilterExpression="begins_with(#a, :v)",
            ExpressionAttributeNames={"#a": "pk"},
            ExpressionAttributeValues={":v": {"S": "hello"}},
        )
        assert resp["Count"] == 0

    def test_scan_empty_table_accepts_binary(self, dynamodb_client):
        """Scan with begins_with using binary operand succeeds."""
        resp = dynamodb_client.scan(
            TableName=self.table,
            FilterExpression="begins_with(#a, :v)",
            ExpressionAttributeNames={"#a": "pk"},
            ExpressionAttributeValues={":v": {"B": b"\x01\x02"}},
        )
        assert resp["Count"] == 0

    def test_query_rejects_invalid_type_in_filter(self, dynamodb_client):
        """Query with begins_with number in FilterExpression is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=self.table,
                KeyConditionExpression="pk = :pk",
                FilterExpression="begins_with(pk, :n)",
                ExpressionAttributeValues={
                    ":pk": {"S": "x"},
                    ":n": {"N": "1"},
                },
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "Invalid FilterExpression" in err["Message"]

    def test_condition_expression_rejects_invalid_type(self, dynamodb_client):
        """PutItem ConditionExpression with begins_with number is rejected."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=self.table,
                Item={"pk": {"S": "test"}},
                ConditionExpression="begins_with(pk, :n)",
                ExpressionAttributeValues={":n": {"N": "1"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "Invalid ConditionExpression" in err["Message"]
        assert "operand type: N" in err["Message"]

    def test_filter_error_label_not_condition(self, dynamodb_client):
        """FilterExpression error says 'FilterExpression', not 'ConditionExpression'."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=self.table,
                FilterExpression="begins_with(#a, :v)",
                ExpressionAttributeNames={"#a": "pk"},
                ExpressionAttributeValues={":v": {"N": "1"}},
            )
        msg = exc_info.value.response["Error"]["Message"]
        assert "FilterExpression" in msg
        assert "ConditionExpression" not in msg
