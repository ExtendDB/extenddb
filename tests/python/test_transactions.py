# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Transaction operations: TransactWriteItems, TransactGetItems.

Covers put/delete/update/condition-check actions, atomicity,
cancellation reasons, error paths, and limits.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError, ParamValidationError


class TestTransactWriteItems:
    """TransactWriteItems API tests."""

    def test_transact_write_put_single(self, table_factory, dynamodb_client):
        """Single put in a transaction."""
        name = table_factory()
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": name, "Item": {"pk": {"S": "k1"}, "v": {"S": "val"}}}}
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["S"] == "val"

    def test_transact_write_put_multiple(self, table_factory, dynamodb_client):
        """Multiple puts in a single transaction."""
        name = table_factory()
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": name, "Item": {"pk": {"S": f"k{i}"}, "v": {"N": str(i)}}}}
                for i in range(5)
            ]
        )
        for i in range(5):
            resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": f"k{i}"}})
            assert resp["Item"]["v"]["N"] == str(i)

    def test_transact_write_delete(self, table_factory, dynamodb_client):
        """Delete in a transaction."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "x"}})
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Delete": {"TableName": name, "Key": {"pk": {"S": "k1"}}}}
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_transact_write_update(self, table_factory, dynamodb_client):
        """Update in a transaction."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}})
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": name,
                        "Key": {"pk": {"S": "k1"}},
                        "UpdateExpression": "SET v = v + :inc",
                        "ExpressionAttributeValues": {":inc": {"N": "10"}},
                    }
                }
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["N"] == "11"

    def test_transact_write_condition_check(self, table_factory, dynamodb_client):
        """ConditionCheck passes when condition is met."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "status": {"S": "active"}}
        )
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "ConditionCheck": {
                        "TableName": name,
                        "Key": {"pk": {"S": "k1"}},
                        "ConditionExpression": "#s = :v",
                        "ExpressionAttributeNames": {"#s": "status"},
                        "ExpressionAttributeValues": {":v": {"S": "active"}},
                    }
                },
                {
                    "Put": {
                        "TableName": name,
                        "Item": {"pk": {"S": "k2"}, "v": {"S": "ok"}},
                    }
                },
            ]
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k2"}})
        assert resp["Item"]["v"]["S"] == "ok"

    def test_transact_write_mixed_ops(self, table_factory, dynamodb_client):
        """Mix put, delete, and update in a single transaction."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "upd"}, "v": {"N": "1"}}
        )
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "del"}, "v": {"S": "x"}}
        )
        dynamodb_client.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": name, "Item": {"pk": {"S": "new"}, "v": {"S": "a"}}}},
                {"Delete": {"TableName": name, "Key": {"pk": {"S": "del"}}}},
                {
                    "Update": {
                        "TableName": name,
                        "Key": {"pk": {"S": "upd"}},
                        "UpdateExpression": "SET v = :val",
                        "ExpressionAttributeValues": {":val": {"N": "99"}},
                    }
                },
            ]
        )
        assert dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "new"}})["Item"]["v"]["S"] == "a"
        assert "Item" not in dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "del"}})
        assert dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "upd"}})["Item"]["v"]["N"] == "99"

    def test_transact_write_condition_fail_rolls_back(self, table_factory, dynamodb_client):
        """Failed condition rolls back all operations atomically."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "guard"}, "v": {"S": "wrong"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "should_not_exist"}}}},
                    {
                        "ConditionCheck": {
                            "TableName": name,
                            "Key": {"pk": {"S": "guard"}},
                            "ConditionExpression": "v = :expected",
                            "ExpressionAttributeValues": {":expected": {"S": "right"}},
                        }
                    },
                ]
            )
        assert exc.value.response["Error"]["Code"] == "TransactionCanceledException"
        # The put should have been rolled back
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "should_not_exist"}})
        assert "Item" not in resp

    def test_transact_write_cancellation_reasons(self, table_factory, dynamodb_client):
        """CancellationReasons are returned on failure."""
        name = table_factory()
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": name,
                            "Item": {"pk": {"S": "k1"}},
                            "ConditionExpression": "attribute_exists(pk)",
                        }
                    }
                ]
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "TransactionCanceledException"
        reasons = exc.value.response.get("CancellationReasons", [])
        assert len(reasons) >= 1

    def test_transact_write_empty_items(self, dynamodb_client):
        """Empty TransactItems is rejected (by boto3 or server)."""
        with pytest.raises((ClientError, ParamValidationError)):
            dynamodb_client.transact_write_items(TransactItems=[])

    def test_transact_write_too_many_items(self, table_factory, dynamodb_client):
        """More than 100 items is rejected."""
        name = table_factory()
        items = [
            {"Put": {"TableName": name, "Item": {"pk": {"S": f"k{i}"}}}}
            for i in range(101)
        ]
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(TransactItems=items)
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_transact_write_duplicate_targets(self, table_factory, dynamodb_client):
        """Two operations targeting the same item are rejected."""
        name = table_factory()
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "k1"}, "v": {"S": "a"}}}},
                    {"Put": {"TableName": name, "Item": {"pk": {"S": "k1"}, "v": {"S": "b"}}}},
                ]
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"


class TestTransactGetItems:
    """TransactGetItems API tests."""

    def test_transact_get_single(self, table_factory, dynamodb_client):
        """Get a single item in a transaction."""
        name = table_factory()
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "val"}})
        resp = dynamodb_client.transact_get_items(
            TransactItems=[{"Get": {"TableName": name, "Key": {"pk": {"S": "k1"}}}}]
        )
        assert resp["Responses"][0]["Item"]["v"]["S"] == "val"

    def test_transact_get_multiple(self, table_factory, dynamodb_client):
        """Get multiple items in a single transaction."""
        name = table_factory()
        for i in range(3):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": f"k{i}"}, "v": {"N": str(i)}}
            )
        resp = dynamodb_client.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": name, "Key": {"pk": {"S": f"k{i}"}}}}
                for i in range(3)
            ]
        )
        assert len(resp["Responses"]) == 3
        for i, r in enumerate(resp["Responses"]):
            assert r["Item"]["v"]["N"] == str(i)

    def test_transact_get_missing_item(self, table_factory, dynamodb_client):
        """Missing item returns empty Item in response."""
        name = table_factory()
        resp = dynamodb_client.transact_get_items(
            TransactItems=[{"Get": {"TableName": name, "Key": {"pk": {"S": "missing"}}}}]
        )
        assert resp["Responses"][0] == {}

    def test_transact_get_with_projection(self, table_factory, dynamodb_client):
        """TransactGetItems with ProjectionExpression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}}
        )
        resp = dynamodb_client.transact_get_items(
            TransactItems=[
                {
                    "Get": {
                        "TableName": name,
                        "Key": {"pk": {"S": "k1"}},
                        "ProjectionExpression": "pk, a",
                    }
                }
            ]
        )
        item = resp["Responses"][0]["Item"]
        assert "a" in item
        assert "b" not in item

    def test_transact_get_empty_items(self, dynamodb_client):
        """Empty TransactItems is rejected (by boto3 or server)."""
        with pytest.raises((ClientError, ParamValidationError)):
            dynamodb_client.transact_get_items(TransactItems=[])

    def test_transact_get_consumed_capacity(self, table_factory, dynamodb_client):
        """TransactGetItems costs 2 RCU per item, including missing items."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "exists"}},
        )
        resp = dynamodb_client.transact_get_items(
            ReturnConsumedCapacity="TOTAL",
            TransactItems=[
                {"Get": {"TableName": name, "Key": {"pk": {"S": "exists"}}}},
                {"Get": {"TableName": name, "Key": {"pk": {"S": "missing"}}}},
            ],
        )
        cap = resp["ConsumedCapacity"][0]
        assert cap["CapacityUnits"] == 4.0
        assert cap["ReadCapacityUnits"] == 4.0
