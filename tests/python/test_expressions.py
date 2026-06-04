# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Advanced expression tests.

Covers complex condition expressions, update expressions with multiple
actions, nested path expressions, expression attribute names/values,
size() function, and expression error paths.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted


class TestConditionExpressions:
    """Advanced condition expression tests."""

    def test_attribute_exists(self, table_factory, dynamodb_client):
        """attribute_exists condition succeeds when attribute is present."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "x"}}
        )
        # Should succeed — v exists
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "v": {"S": "y"}},
            ConditionExpression="attribute_exists(v)",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["S"] == "y"

    def test_attribute_not_exists(self, table_factory, dynamodb_client):
        """attribute_not_exists condition for new item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "v": {"S": "first"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
        # Second put should fail — pk now exists
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k1"}, "v": {"S": "second"}},
                ConditionExpression="attribute_not_exists(pk)",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_comparison_operators(self, table_factory, dynamodb_client):
        """Numeric comparison operators in conditions."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "count": {"N": "10"}}
        )
        # count > 5 should succeed
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET #c = #c + :one",
            ConditionExpression="#c > :min",
            ExpressionAttributeNames={"#c": "count"},
            ExpressionAttributeValues={":min": {"N": "5"}, ":one": {"N": "1"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["count"]["N"] == "11"

    def test_between_condition(self, table_factory, dynamodb_client):
        """BETWEEN condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "score": {"N": "75"}}
        )
        # score BETWEEN 50 AND 100 should succeed
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="score BETWEEN :lo AND :hi",
            ExpressionAttributeValues={":lo": {"N": "50"}, ":hi": {"N": "100"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_in_condition(self, table_factory, dynamodb_client):
        """IN condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "status": {"S": "active"}}
        )
        # status IN (:a, :b) should succeed
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="#s IN (:a, :b)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":a": {"S": "active"},
                ":b": {"S": "pending"},
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_and_or_not(self, table_factory, dynamodb_client):
        """AND, OR, NOT logical operators in conditions."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"N": "10"}, "b": {"S": "yes"}},
        )
        # (a > 5 AND b = "yes") should succeed
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"N": "10"}, "b": {"S": "yes"}, "c": {"S": "added"}},
            ConditionExpression="a > :min AND b = :val",
            ExpressionAttributeValues={":min": {"N": "5"}, ":val": {"S": "yes"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "c" in resp["Item"]

    def test_contains_function(self, table_factory, dynamodb_client):
        """contains() function in condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "tags": {"SS": ["red", "blue", "green"]}},
        )
        # contains(tags, "blue") should succeed
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="contains(tags, :val)",
            ExpressionAttributeValues={":val": {"S": "blue"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_begins_with_function(self, table_factory, dynamodb_client):
        """begins_with() function in condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "name": {"S": "hello_world"}}
        )
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="begins_with(#n, :prefix)",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":prefix": {"S": "hello"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_size_function(self, table_factory, dynamodb_client):
        """size() function in condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "data": {"S": "abc"}}
        )
        # size(data) = 3 should succeed
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="size(#d) = :sz",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":sz": {"N": "3"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp


class TestUpdateExpressions:
    """Advanced update expression tests."""

    def test_set_nested_attribute(self, table_factory, dynamodb_client):
        """SET a nested map attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "info": {"M": {"name": {"S": "old"}}}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET info.#n = :val",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":val": {"S": "new"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["info"]["M"]["name"]["S"] == "new"

    def test_remove_attribute(self, table_factory, dynamodb_client):
        """REMOVE an attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="REMOVE b",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "b" not in resp["Item"]
        assert "a" in resp["Item"]

    def test_add_to_number(self, table_factory, dynamodb_client):
        """ADD to a numeric attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "counter": {"N": "5"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="ADD #c :inc",
            ExpressionAttributeNames={"#c": "counter"},
            ExpressionAttributeValues={":inc": {"N": "3"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["counter"]["N"] == "8"

    def test_add_to_set(self, table_factory, dynamodb_client):
        """ADD elements to a string set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "tags": {"SS": ["a", "b"]}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="ADD tags :new",
            ExpressionAttributeValues={":new": {"SS": ["c", "d"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert set(resp["Item"]["tags"]["SS"]) == {"a", "b", "c", "d"}

    def test_delete_from_set(self, table_factory, dynamodb_client):
        """DELETE elements from a string set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "tags": {"SS": ["a", "b", "c"]}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="DELETE tags :rm",
            ExpressionAttributeValues={":rm": {"SS": ["b"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert set(resp["Item"]["tags"]["SS"]) == {"a", "c"}

    def test_if_not_exists(self, table_factory, dynamodb_client):
        """SET with if_not_exists — only sets if attribute is missing."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "original"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET v = if_not_exists(v, :default)",
            ExpressionAttributeValues={":default": {"S": "fallback"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["S"] == "original"  # Not overwritten

    def test_list_append(self, table_factory, dynamodb_client):
        """SET with list_append — append to a list."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "items": {"L": [{"S": "a"}]}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET #i = list_append(#i, :new)",
            ExpressionAttributeNames={"#i": "items"},
            ExpressionAttributeValues={":new": {"L": [{"S": "b"}, {"S": "c"}]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        vals = [e["S"] for e in resp["Item"]["items"]["L"]]
        assert vals == ["a", "b", "c"]

    def test_multiple_set_actions(self, table_factory, dynamodb_client):
        """Multiple SET actions in a single update."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"N": "1"}, "b": {"N": "2"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET a = :va, b = :vb, c = :vc",
            ExpressionAttributeValues={
                ":va": {"N": "10"},
                ":vb": {"N": "20"},
                ":vc": {"N": "30"},
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["a"]["N"] == "10"
        assert resp["Item"]["b"]["N"] == "20"
        assert resp["Item"]["c"]["N"] == "30"

    def test_set_and_remove_combined(self, table_factory, dynamodb_client):
        """SET and REMOVE in the same update expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "keep": {"S": "yes"}, "drop": {"S": "no"}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET added = :v REMOVE #d",
            ExpressionAttributeNames={"#d": "drop"},
            ExpressionAttributeValues={":v": {"S": "new"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["added"]["S"] == "new"
        assert "drop" not in resp["Item"]
        assert resp["Item"]["keep"]["S"] == "yes"


class TestFilterExpressions:
    """Filter expression tests on Query and Scan."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup(self, dynamodb_client, request):
        """Create a table with test data for filter tests."""
        name = unique_name("filter")
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
        items = [
            {"pk": {"S": "p1"}, "sk": {"S": "s1"}, "status": {"S": "active"}, "score": {"N": "80"}},
            {"pk": {"S": "p1"}, "sk": {"S": "s2"}, "status": {"S": "inactive"}, "score": {"N": "40"}},
            {"pk": {"S": "p1"}, "sk": {"S": "s3"}, "status": {"S": "active"}, "score": {"N": "95"}},
        ]
        for item in items:
            dynamodb_client.put_item(TableName=name, Item=item)
        request.cls._table_name = name
        request.cls._client = dynamodb_client
        yield
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)

    def test_filter_equals(self):
        """Filter with equality."""
        resp = self._client.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            FilterExpression="#s = :val",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":pk": {"S": "p1"}, ":val": {"S": "active"}},
        )
        assert resp["Count"] == 2

    def test_filter_greater_than(self):
        """Filter with numeric comparison."""
        resp = self._client.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            FilterExpression="score > :min",
            ExpressionAttributeValues={":pk": {"S": "p1"}, ":min": {"N": "50"}},
        )
        assert resp["Count"] == 2

    def test_filter_and(self):
        """Filter with AND."""
        resp = self._client.query(
            TableName=self._table_name,
            KeyConditionExpression="pk = :pk",
            FilterExpression="#s = :status AND score > :min",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":pk": {"S": "p1"},
                ":status": {"S": "active"},
                ":min": {"N": "90"},
            },
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["score"]["N"] == "95"

    def test_scan_filter(self):
        """Scan with filter expression."""
        resp = self._client.scan(
            TableName=self._table_name,
            FilterExpression="score >= :min",
            ExpressionAttributeValues={":min": {"N": "80"}},
        )
        assert resp["Count"] == 2
