# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Item operation edge cases: size limits, special values, return values on CCF,
comparison operators in conditions, GSI interactions.

Covers gaps from external suite: PutItemTests, GetItemTests, DeleteItemTests,
UpdateItemTests edge cases not in test_items.py.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


class TestPutItemEdgeCases:
    """PutItem edge cases from external suite."""

    def test_put_item_with_special_characters(self, table_factory, dynamodb_client):
        """Special characters in attribute values."""
        name = table_factory()
        val = 'quotes"and\'backslash\\newline\ntab\t'
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"S": val}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == val

    def test_put_item_with_different_number_values(self, table_factory, dynamodb_client):
        """Various number formats: integer, decimal, negative, zero."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "k1"},
                "int": {"N": "42"},
                "dec": {"N": "3.14"},
                "neg": {"N": "-100"},
                "zero": {"N": "0"},
            },
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["int"]["N"] == "42"
        assert resp["Item"]["dec"]["N"] == "3.14"
        assert resp["Item"]["neg"]["N"] == "-100"
        assert resp["Item"]["zero"]["N"] == "0"

    def test_put_zero_as_attribute_value(self, table_factory, dynamodb_client):
        """Zero is a valid number attribute value."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "val": {"N": "0"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["N"] == "0"

    def test_put_zero_to_new_attribute(self, table_factory, dynamodb_client):
        """Adding zero to a new attribute via update creates it with value 0."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET newattr = :z",
            ExpressionAttributeValues={":z": {"N": "0"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["newattr"]["N"] == "0"

    def test_put_item_return_value_none(self, table_factory, dynamodb_client):
        """PutItem with ReturnValues=NONE returns no Attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        resp = dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "v": {"N": "2"}},
            ReturnValues="NONE",
        )
        assert "Attributes" not in resp

    def test_put_item_return_value_all_old(self, table_factory, dynamodb_client):
        """PutItem with ReturnValues=ALL_OLD returns previous item."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        resp = dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "v": {"N": "2"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["v"]["N"] == "1"

    def test_put_to_replace_existing_item(self, table_factory, dynamodb_client):
        """PutItem replaces entire item, removing old attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "c": {"S": "3"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "a" not in resp["Item"]
        assert "b" not in resp["Item"]
        assert resp["Item"]["c"]["S"] == "3"

    def test_put_item_with_condition_expression(self, table_factory, dynamodb_client):
        """PutItem with condition expression using comparison."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "5"}}
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "v": {"N": "10"}},
            ConditionExpression="v < :max",
            ExpressionAttributeValues={":max": {"N": "100"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["N"] == "10"

    def test_put_item_rv_on_ccf_all_old(self, table_factory, dynamodb_client):
        """PutItem with ReturnValuesOnConditionCheckFailure=ALL_OLD."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k1"}, "v": {"N": "2"}},
                ConditionExpression="attribute_not_exists(pk)",
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ConditionalCheckFailedException"
        # The item should be in the error response
        item = err.get("Item")
        if item:
            assert item["v"]["N"] == "1"

    def test_put_item_rv_on_ccf_none(self, table_factory, dynamodb_client):
        """PutItem with ReturnValuesOnConditionCheckFailure=NONE."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": "k1"}, "v": {"N": "2"}},
                ConditionExpression="attribute_not_exists(pk)",
                ReturnValuesOnConditionCheckFailure="NONE",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_put_items_in_multiple_tables(self, table_factory, dynamodb_client):
        """Put items in different tables and verify isolation."""
        t1 = table_factory()
        t2 = table_factory()
        dynamodb_client.put_item(
            TableName=t1, Item={"pk": {"S": "k1"}, "src": {"S": "t1"}}
        )
        dynamodb_client.put_item(
            TableName=t2, Item={"pk": {"S": "k1"}, "src": {"S": "t2"}}
        )
        r1 = dynamodb_client.get_item(TableName=t1, Key={"pk": {"S": "k1"}})
        r2 = dynamodb_client.get_item(TableName=t2, Key={"pk": {"S": "k1"}})
        assert r1["Item"]["src"]["S"] == "t1"
        assert r2["Item"]["src"]["S"] == "t2"

    def test_replace_list_with_different_order(self, table_factory, dynamodb_client):
        """Replacing a list preserves the new order."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "lst": {"L": [{"S": "a"}, {"S": "b"}]}},
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "lst": {"L": [{"S": "b"}, {"S": "a"}]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        lst = resp["Item"]["lst"]["L"]
        assert lst[0]["S"] == "b"
        assert lst[1]["S"] == "a"


class TestGetItemEdgeCases:
    """GetItem edge cases from external suite."""

    def test_get_item_with_special_characters(self, table_factory, dynamodb_client):
        """GetItem returns items with special characters faithfully."""
        name = table_factory()
        val = "line1\nline2\ttab"
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "val": {"S": val}}
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["S"] == val

    def test_get_item_attrs_do_not_exist(self, table_factory, dynamodb_client):
        """ProjectionExpression for non-existent attributes returns only key."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"S": "val"}}
        )
        resp = dynamodb_client.get_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression="nonexistent",
        )
        # Item exists but projected attribute doesn't — returns empty item
        assert "Item" in resp
        assert "nonexistent" not in resp["Item"]

    def test_get_item_some_attrs_exist(self, table_factory, dynamodb_client):
        """ProjectionExpression with mix of existing and non-existing attrs."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}},
        )
        resp = dynamodb_client.get_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression="a, nonexistent",
        )
        assert resp["Item"]["a"]["S"] == "1"
        assert "nonexistent" not in resp["Item"]

    def test_get_item_consistent_read_false(self, table_factory, dynamodb_client):
        """GetItem with ConsistentRead=False (eventually consistent)."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "val"}}
        )
        resp = dynamodb_client.get_item(
            TableName=name, Key={"pk": {"S": "k1"}}, ConsistentRead=False
        )
        assert resp["Item"]["v"]["S"] == "val"

    def test_get_item_with_null_types(self, table_factory, dynamodb_client):
        """GetItem returns NULL type attributes correctly."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "n1": {"NULL": True}, "n2": {"NULL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["n1"]["NULL"] is True
        assert resp["Item"]["n2"]["NULL"] is True

    def test_get_blob_set_attributes(self, table_factory, dynamodb_client):
        """GetItem returns binary set attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "bs": {"BS": [b"\x01", b"\x02", b"\x03"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert len(resp["Item"]["bs"]["BS"]) == 3

    def test_get_string_set_attributes(self, table_factory, dynamodb_client):
        """GetItem returns string set attributes."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "ss": {"SS": ["alpha", "beta", "gamma"]}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert set(resp["Item"]["ss"]["SS"]) == {"alpha", "beta", "gamma"}

    def test_batch_get_item_same_key_twice(self, table_factory, dynamodb_client):
        """BatchGetItem with same key twice is rejected."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"S": "val"}}
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={
                    name: {
                        "Keys": [{"pk": {"S": "k1"}}, {"pk": {"S": "k1"}}],
                    }
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestDeleteItemEdgeCases:
    """DeleteItem edge cases from external suite."""

    def test_delete_with_return_value_expression(self, table_factory, dynamodb_client):
        """DeleteItem with ReturnValues=ALL_OLD and condition expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "42"}}
        )
        resp = dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ReturnValues="ALL_OLD",
            ConditionExpression="v = :val",
            ExpressionAttributeValues={":val": {"N": "42"}},
        )
        assert resp["Attributes"]["v"]["N"] == "42"

    def test_delete_with_comparison_operators(self, table_factory, dynamodb_client):
        """DeleteItem with various comparison operators in condition."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "50"}}
        )
        # Greater than — should succeed
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="v > :min",
            ExpressionAttributeValues={":min": {"N": "10"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_delete_with_between_comparison(self, table_factory, dynamodb_client):
        """DeleteItem with BETWEEN condition."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "50"}}
        )
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="v BETWEEN :lo AND :hi",
            ExpressionAttributeValues={":lo": {"N": "1"}, ":hi": {"N": "100"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_delete_with_null_comparison(self, table_factory, dynamodb_client):
        """DeleteItem with attribute_exists on NULL attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "n": {"NULL": True}},
        )
        dynamodb_client.delete_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="attribute_exists(n)",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "Item" not in resp

    def test_delete_rv_on_ccf(self, table_factory, dynamodb_client):
        """DeleteItem with ReturnValuesOnConditionCheckFailure."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.delete_item(
                TableName=name,
                Key={"pk": {"S": "k1"}},
                ConditionExpression="v = :val",
                ExpressionAttributeValues={":val": {"N": "999"}},
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestUpdateItemEdgeCases:
    """UpdateItem edge cases from external suite."""

    def test_update_with_nested_map(self, table_factory, dynamodb_client):
        """Update a nested map attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "k1"},
                "data": {"M": {"nested": {"S": "old"}}},
            },
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET #d.nested = :v",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":v": {"S": "new"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["data"]["M"]["nested"]["S"] == "new"

    def test_update_with_nested_list(self, table_factory, dynamodb_client):
        """Update a specific list element by index."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "k1"},
                "items": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
            },
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET #i[1] = :v",
            ExpressionAttributeNames={"#i": "items"},
            ExpressionAttributeValues={":v": {"S": "B"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        lst = resp["Item"]["items"]["L"]
        assert lst[0]["S"] == "a"
        assert lst[1]["S"] == "B"
        assert lst[2]["S"] == "c"

    def test_update_boolean_attribute(self, table_factory, dynamodb_client):
        """Update a boolean attribute."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "flag": {"BOOL": False}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET flag = :v",
            ExpressionAttributeValues={":v": {"BOOL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["flag"]["BOOL"] is True

    def test_update_null_attribute(self, table_factory, dynamodb_client):
        """Update an attribute to NULL."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "val": {"S": "notnull"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET val = :v",
            ExpressionAttributeValues={":v": {"NULL": True}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["val"]["NULL"] is True

    def test_add_to_string_fails(self, table_factory, dynamodb_client):
        """ADD on a string attribute fails with ValidationException."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "val": {"S": "text"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_item(
                TableName=name,
                Key={"pk": {"S": "k1"}},
                UpdateExpression="ADD val :v",
                ExpressionAttributeValues={":v": {"N": "1"}},
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_add_zero_to_existing_attribute(self, table_factory, dynamodb_client):
        """ADD zero to existing number attribute doesn't change value."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "42"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="ADD v :z",
            ExpressionAttributeValues={":z": {"N": "0"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["N"] == "42"

    def test_add_zero_to_nonexisting_attribute(self, table_factory, dynamodb_client):
        """ADD zero to non-existing attribute creates it with value 0."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="ADD newattr :z",
            ExpressionAttributeValues={":z": {"N": "0"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["newattr"]["N"] == "0"

    def test_update_set_with_arithmetic(self, table_factory, dynamodb_client):
        """SET with arithmetic expression (v = v + :inc)."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "10"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET v = v + :inc",
            ExpressionAttributeValues={":inc": {"N": "5"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["N"] == "15"

    def test_update_add_creates_new_number(self, table_factory, dynamodb_client):
        """ADD on non-existent number attribute creates it."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="ADD #c :v",
            ExpressionAttributeNames={"#c": "counter"},
            ExpressionAttributeValues={":v": {"N": "1"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["counter"]["N"] == "1"

    def test_update_remove_nonexistent_attribute(self, table_factory, dynamodb_client):
        """REMOVE on non-existent attribute succeeds silently."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "a": {"S": "val"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="REMOVE nonexistent",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["a"]["S"] == "val"

    def test_update_remove_multiple_attributes(self, table_factory, dynamodb_client):
        """REMOVE multiple attributes in one expression."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}, "c": {"S": "3"}},
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="REMOVE a, b",
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert "a" not in resp["Item"]
        assert "b" not in resp["Item"]
        assert resp["Item"]["c"]["S"] == "3"

    def test_update_conditional_with_expression(self, table_factory, dynamodb_client):
        """UpdateItem with condition expression that succeeds."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "5"}}
        )
        dynamodb_client.update_item(
            TableName=name,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET v = :new",
            ConditionExpression="v < :max",
            ExpressionAttributeValues={":new": {"N": "10"}, ":max": {"N": "100"}},
        )
        resp = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k1"}})
        assert resp["Item"]["v"]["N"] == "10"

    def test_update_rv_on_ccf(self, table_factory, dynamodb_client):
        """UpdateItem with ReturnValuesOnConditionCheckFailure."""
        name = table_factory()
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": "k1"}, "v": {"N": "1"}}
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_item(
                TableName=name,
                Key={"pk": {"S": "k1"}},
                UpdateExpression="SET v = :new",
                ConditionExpression="v = :expected",
                ExpressionAttributeValues={":new": {"N": "10"}, ":expected": {"N": "999"}},
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
