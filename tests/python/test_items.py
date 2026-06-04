# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Item operations: PutItem, GetItem, DeleteItem, UpdateItem.

Covers all expression types (condition, update, projection, filter),
return values, and error paths.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active


class TestPutItem:
    """PutItem API tests."""

    def test_put_and_get_string(self, table_factory, dynamodb_client):
        """Put and retrieve a simple string item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "data": {"S": "hello"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["pk"]["S"] == "key1"
        assert resp["Item"]["data"]["S"] == "hello"

    def test_put_and_get_number(self, table_factory, dynamodb_client):
        """Put and retrieve a number attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "count": {"N": "42"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["count"]["N"] == "42"

    def test_put_and_get_binary(self, table_factory, dynamodb_client):
        """Put and retrieve a binary attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "blob": {"B": b"\x00\x01\x02"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["blob"]["B"] == b"\x00\x01\x02"

    def test_put_and_get_boolean(self, table_factory, dynamodb_client):
        """Put and retrieve a boolean attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "flag": {"BOOL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["flag"]["BOOL"] is True

    def test_put_and_get_null(self, table_factory, dynamodb_client):
        """Put and retrieve a NULL attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "empty": {"NULL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["empty"]["NULL"] is True

    def test_put_and_get_list(self, table_factory, dynamodb_client):
        """Put and retrieve a list attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "key1"},
                "items": {"L": [{"S": "a"}, {"N": "1"}, {"BOOL": False}]},
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        lst = resp["Item"]["items"]["L"]
        assert len(lst) == 3
        assert lst[0]["S"] == "a"
        assert lst[1]["N"] == "1"
        assert lst[2]["BOOL"] is False

    def test_put_and_get_map(self, table_factory, dynamodb_client):
        """Put and retrieve a map attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "key1"},
                "meta": {"M": {"nested": {"S": "value"}, "count": {"N": "5"}}},
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        m = resp["Item"]["meta"]["M"]
        assert m["nested"]["S"] == "value"
        assert m["count"]["N"] == "5"

    def test_put_and_get_string_set(self, table_factory, dynamodb_client):
        """Put and retrieve a string set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "tags": {"SS": ["a", "b", "c"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert set(resp["Item"]["tags"]["SS"]) == {"a", "b", "c"}

    def test_put_and_get_number_set(self, table_factory, dynamodb_client):
        """Put and retrieve a number set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "nums": {"NS": ["1", "2", "3"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert set(resp["Item"]["nums"]["NS"]) == {"1", "2", "3"}

    def test_put_overwrite(self, table_factory, dynamodb_client):
        """PutItem overwrites an existing item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "1"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "2"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["v"]["N"] == "2"

    def test_put_condition_succeeds(self, table_factory, dynamodb_client):
        """PutItem with condition expression that succeeds."""
        name = table_factory()
        # First put — no existing item, attribute_not_exists succeeds
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "1"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["v"]["N"] == "1"

    def test_put_condition_fails(self, table_factory, dynamodb_client):
        """PutItem with condition expression that fails."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "1"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "key1"}, "v": {"N": "2"}},
                ConditionExpression="attribute_not_exists(pk)",
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_put_return_old(self, table_factory, dynamodb_client):
        """PutItem with ReturnValues=ALL_OLD returns the previous item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "1"}},
        )
        resp = dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "2"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["v"]["N"] == "1"

    def test_put_return_none_on_new(self, table_factory, dynamodb_client):
        """PutItem with ReturnValues=ALL_OLD on new item returns no Attributes."""
        name = table_factory()
        resp = dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"N": "1"}},
            ReturnValues="ALL_OLD",
        )
        assert "Attributes" not in resp

    def test_put_with_hash_range(self, table_factory, dynamodb_client):
        """PutItem on a hash+range table."""
        name = table_factory(range_key="sk")
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "data": {"S": "val"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}}
        )
        assert resp["Item"]["data"]["S"] == "val"

    def test_put_missing_key(self, table_factory, dynamodb_client):
        """PutItem without required key attribute fails."""
        name = table_factory(range_key="sk")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "p1"}},  # missing sk
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_put_item_rejects_empty_binary_key(self, table_factory, dynamodb_client):
        """PutItem rejects empty binary values in key positions"""

        name = table_factory(hash_type="B")
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"B": b""}})
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert "empty binary value" in err["Message"], err["Message"]
        assert "Key: pk" in err["Message"], err["Message"]


class TestGetItem:
    """GetItem API tests."""

    def test_get_nonexistent(self, table_factory, dynamodb_client):
        """GetItem for a nonexistent key returns no Item."""
        name = table_factory()
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "missing"}})
        assert "Item" not in resp

    def test_get_consistent_read(self, table_factory, dynamodb_client):
        """GetItem with ConsistentRead=True."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "v": {"S": "val"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "key1"}}, ConsistentRead=True
        )
        assert resp["Item"]["v"]["S"] == "val"

    def test_get_projection(self, table_factory, dynamodb_client):
        """GetItem with ProjectionExpression returns only requested attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "a": {"S": "1"}, "b": {"S": "2"}, "c": {"S": "3"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            ProjectionExpression="pk, a",
        )
        assert "a" in resp["Item"]
        assert "pk" in resp["Item"]
        assert "b" not in resp["Item"]
        assert "c" not in resp["Item"]

    def test_get_projection_with_names(self, table_factory, dynamodb_client):
        """GetItem with ProjectionExpression using ExpressionAttributeNames."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "status": {"S": "active"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            ProjectionExpression="#s",
            ExpressionAttributeNames={"#s": "status"},
        )
        assert resp["Item"]["status"]["S"] == "active"
        assert "pk" not in resp["Item"]


class TestDeleteItem:
    """DeleteItem API tests."""

    def test_delete_existing(self, table_factory, dynamodb_client):
        """Delete an existing item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"S": "val"}}
        )
        dynamodb_client.delete_item(TableName=name, Key={"pk": {"S": "key1"}})
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert "Item" not in resp

    def test_delete_nonexistent_succeeds(self, table_factory, dynamodb_client):
        """Delete a nonexistent item succeeds silently."""
        name = table_factory()
        # Should not raise
        dynamodb_client.delete_item(TableName=name, Key={"pk": {"S": "missing"}})

    def test_delete_return_old(self, table_factory, dynamodb_client):
        """DeleteItem with ReturnValues=ALL_OLD returns the deleted item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "42"}}
        )
        resp = dynamodb_client.delete_item(
            TableName=name, Key={"pk": {"S": "key1"}}, ReturnValues="ALL_OLD"
        )
        assert resp["Attributes"]["v"]["N"] == "42"

    def test_delete_condition_succeeds(self, table_factory, dynamodb_client):
        """DeleteItem with condition that succeeds."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            ConditionExpression="v = :val",
            ExpressionAttributeValues={":val": {"N": "1"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert "Item" not in resp

    def test_delete_condition_fails(self, table_factory, dynamodb_client):
        """DeleteItem with condition that fails."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.delete_item(
                TableName=name,
                Key={"pk": {"S": "key1"}},
                ConditionExpression="v = :val",
                ExpressionAttributeValues={":val": {"N": "999"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestUpdateItem:
    """UpdateItem API tests."""

    def test_update_set(self, table_factory, dynamodb_client):
        """UpdateItem SET expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"N": "10"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["v"]["N"] == "10"

    def test_update_set_creates_item(self, table_factory, dynamodb_client):
        """UpdateItem on nonexistent key creates the item."""
        name = table_factory()
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "new-key"}},
            UpdateExpression="SET v = :val",
            ExpressionAttributeValues={":val": {"N": "1"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "new-key"}})
        assert resp["Item"]["v"]["N"] == "1"

    def test_update_remove(self, table_factory, dynamodb_client):
        """UpdateItem REMOVE expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "a": {"S": "x"}, "b": {"S": "y"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="REMOVE b",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert "b" not in resp["Item"]
        assert resp["Item"]["a"]["S"] == "x"

    def test_update_add_number(self, table_factory, dynamodb_client):
        """UpdateItem ADD on a number attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "counter": {"N": "10"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="ADD #c :inc",
            ExpressionAttributeNames={"#c": "counter"},
            ExpressionAttributeValues={":inc": {"N": "5"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["counter"]["N"] == "15"

    def test_update_add_to_set(self, table_factory, dynamodb_client):
        """UpdateItem ADD to a string set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "tags": {"SS": ["a", "b"]}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="ADD tags :new",
            ExpressionAttributeValues={":new": {"SS": ["c"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert set(resp["Item"]["tags"]["SS"]) == {"a", "b", "c"}

    def test_update_delete_from_set(self, table_factory, dynamodb_client):
        """UpdateItem DELETE from a string set."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "tags": {"SS": ["a", "b", "c"]}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="DELETE tags :rem",
            ExpressionAttributeValues={":rem": {"SS": ["b"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert set(resp["Item"]["tags"]["SS"]) == {"a", "c"}

    def test_update_return_all_new(self, table_factory, dynamodb_client):
        """UpdateItem with ReturnValues=ALL_NEW."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        resp = dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"N": "10"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"]["v"]["N"] == "10"

    def test_update_return_all_old(self, table_factory, dynamodb_client):
        """UpdateItem with ReturnValues=ALL_OLD."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        resp = dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"N": "10"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["v"]["N"] == "1"

    def test_update_return_updated_new(self, table_factory, dynamodb_client):
        """UpdateItem with ReturnValues=UPDATED_NEW."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}, "other": {"S": "x"}}
        )
        resp = dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"N": "10"}},
            ReturnValues="UPDATED_NEW",
        )
        assert resp["Attributes"]["v"]["N"] == "10"
        # UPDATED_NEW should only include updated attributes
        assert "other" not in resp["Attributes"]

    def test_update_return_updated_old(self, table_factory, dynamodb_client):
        """UpdateItem with ReturnValues=UPDATED_OLD."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}, "other": {"S": "x"}}
        )
        resp = dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"N": "10"}},
            ReturnValues="UPDATED_OLD",
        )
        assert resp["Attributes"]["v"]["N"] == "1"
        assert "other" not in resp["Attributes"]

    def test_update_condition_fails(self, table_factory, dynamodb_client):
        """UpdateItem with condition that fails."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=name,
                Key={"pk": {"S": "key1"}},
                UpdateExpression="SET v = :new",
                ConditionExpression="v = :expected",
                ExpressionAttributeValues={":new": {"N": "10"}, ":expected": {"N": "999"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_update_set_if_not_exists(self, table_factory, dynamodb_client):
        """UpdateItem SET with if_not_exists function."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "key1"}, "v": {"N": "1"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET v = if_not_exists(v, :default)",
            ExpressionAttributeValues={":default": {"N": "99"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        # v already exists, so if_not_exists returns existing value
        assert resp["Item"]["v"]["N"] == "1"

    def test_update_set_list_append(self, table_factory, dynamodb_client):
        """UpdateItem SET with list_append function."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "items": {"L": [{"S": "a"}]}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET #i = list_append(#i, :new)",
            ExpressionAttributeNames={"#i": "items"},
            ExpressionAttributeValues={":new": {"L": [{"S": "b"}]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        lst = resp["Item"]["items"]["L"]
        assert len(lst) == 2
        assert lst[0]["S"] == "a"
        assert lst[1]["S"] == "b"

    def test_update_multiple_actions(self, table_factory, dynamodb_client):
        """UpdateItem with SET and REMOVE in one expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "key1"}, "a": {"S": "x"}, "b": {"S": "y"}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "key1"}},
            UpdateExpression="SET a = :new REMOVE b",
            ExpressionAttributeValues={":new": {"S": "updated"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "key1"}})
        assert resp["Item"]["a"]["S"] == "updated"
        assert "b" not in resp["Item"]
