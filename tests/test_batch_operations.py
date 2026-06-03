# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Phase 6 batch operations tests — dual-target against real DynamoDB and extenddb.

Covers: BatchGetItem, BatchWriteItem.
REQ-TEST-001, REQ-TEST-002, REQ-TEST-003
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, scoped_table
@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Create a hash-only table for the class, delete on teardown."""
    with scoped_table(dynamodb_client) as name:
        yield name
@pytest.fixture(scope="class")
def hash_range_table(dynamodb_client):
    """Create a hash+range (S,S) table for the class, delete on teardown."""
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
        yield name
@pytest.fixture(scope="class")
def second_table(dynamodb_client):
    """Create a second hash-only table for cross-table batch tests."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
    ) as name:
        yield name
# ── BatchGetItem ──────────────────────────────────────────────────────
class TestBatchGetItem:
    """Tests for the BatchGetItem operation."""

    def test_batch_get_single_table(self, dynamodb_client, hash_table):
        """BatchGetItem retrieves multiple items from a single table."""
        for i in range(5):
            dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": f"item-{i}"}, "data": {"S": f"value-{i}"}},
            )

        resp = dynamodb_client.batch_get_item(
            RequestItems={
                hash_table: {
                    "Keys": [
                        {"pk": {"S": "item-0"}},
                        {"pk": {"S": "item-2"}},
                        {"pk": {"S": "item-4"}},
                    ]
                }
            }
        )

        items = resp["Responses"][hash_table]
        assert len(items) == 3
        pks = sorted(item["pk"]["S"] for item in items)
        assert pks == ["item-0", "item-2", "item-4"]

    def test_batch_get_cross_table(
        self, dynamodb_client, hash_table, second_table
    ):
        """BatchGetItem retrieves items from multiple tables."""
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "a1"}, "val": {"N": "10"}},
        )
        dynamodb_client.put_item(
            TableName=second_table,
            Item={"id": {"S": "b1"}, "val": {"N": "20"}},
        )

        resp = dynamodb_client.batch_get_item(
            RequestItems={
                hash_table: {"Keys": [{"pk": {"S": "a1"}}]},
                second_table: {"Keys": [{"id": {"S": "b1"}}]},
            }
        )

        assert len(resp["Responses"][hash_table]) == 1
        assert len(resp["Responses"][second_table]) == 1
        assert resp["Responses"][hash_table][0]["val"]["N"] == "10"
        assert resp["Responses"][second_table][0]["val"]["N"] == "20"

    def test_batch_get_missing_items(self, dynamodb_client, hash_table):
        """BatchGetItem returns only items that exist; missing keys are silently skipped."""
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "exists"}},
        )

        resp = dynamodb_client.batch_get_item(
            RequestItems={
                hash_table: {
                    "Keys": [
                        {"pk": {"S": "exists"}},
                        {"pk": {"S": "does-not-exist"}},
                    ]
                }
            }
        )

        items = resp["Responses"][hash_table]
        assert len(items) == 1
        assert items[0]["pk"]["S"] == "exists"

    def test_batch_get_composite_key(self, dynamodb_client, hash_range_table):
        """BatchGetItem works with composite (hash+range) keys."""
        dynamodb_client.put_item(
            TableName=hash_range_table,
            Item={"pk": {"S": "user1"}, "sk": {"S": "profile"}, "name": {"S": "Alice"}},
        )
        dynamodb_client.put_item(
            TableName=hash_range_table,
            Item={"pk": {"S": "user1"}, "sk": {"S": "settings"}, "theme": {"S": "dark"}},
        )

        resp = dynamodb_client.batch_get_item(
            RequestItems={
                hash_range_table: {
                    "Keys": [
                        {"pk": {"S": "user1"}, "sk": {"S": "profile"}},
                        {"pk": {"S": "user1"}, "sk": {"S": "settings"}},
                    ]
                }
            }
        )

        items = resp["Responses"][hash_range_table]
        assert len(items) == 2

    def test_batch_get_too_many_keys(self, dynamodb_client, hash_table):
        """BatchGetItem with > 100 keys returns ValidationException."""
        keys = [{"pk": {"S": f"k-{i}"}} for i in range(101)]
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={hash_table: {"Keys": keys}}
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_get_empty_request_items(self, dynamodb_client):
        """BatchGetItem with empty RequestItems returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(RequestItems={})
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_get_unprocessed_keys_empty(self, dynamodb_client, hash_table):
        """BatchGetItem returns empty UnprocessedKeys when all keys are processed."""
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "x"}},
        )

        resp = dynamodb_client.batch_get_item(
            RequestItems={hash_table: {"Keys": [{"pk": {"S": "x"}}]}}
        )

        # UnprocessedKeys should be empty (or absent)
        unprocessed = resp.get("UnprocessedKeys", {})
        assert len(unprocessed) == 0

    def test_batch_get_unprocessed_keys_always_present(
        self, dynamodb_client, hash_table
    ):
        """BatchGetItem always includes UnprocessedKeys, even when all keys processed.

        Amazon DynamoDB returns ``"UnprocessedKeys": {}`` on the wire when every
        key is processed; the field is never omitted. A response that drops the
        field entirely is non-conformant.
        """
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "present"}},
        )

        resp = dynamodb_client.batch_get_item(
            RequestItems={hash_table: {"Keys": [{"pk": {"S": "present"}}]}}
        )

        assert "UnprocessedKeys" in resp, (
            "UnprocessedKeys must always be present in the BatchGetItem "
            "response, even when empty"
        )
        assert resp["UnprocessedKeys"] == {}
# ── BatchWriteItem ────────────────────────────────────────────────────
class TestBatchWriteItem:
    """Tests for the BatchWriteItem operation."""

    def test_batch_write_puts(self, dynamodb_client, hash_table):
        """BatchWriteItem with PutRequests writes multiple items."""
        dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": f"item-{i}"}, "n": {"N": str(i)}}}}
                    for i in range(5)
                ]
            }
        )

        # Verify all items exist
        for i in range(5):
            resp = dynamodb_client.get_item(
                TableName=hash_table, Key={"pk": {"S": f"item-{i}"}}
            )
            assert "Item" in resp
            assert resp["Item"]["n"]["N"] == str(i)

    def test_batch_write_deletes(self, dynamodb_client, hash_table):
        """BatchWriteItem with DeleteRequests removes items."""
        for i in range(3):
            dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": f"del-{i}"}},
            )

        dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"DeleteRequest": {"Key": {"pk": {"S": f"del-{i}"}}}}
                    for i in range(3)
                ]
            }
        )

        for i in range(3):
            resp = dynamodb_client.get_item(
                TableName=hash_table, Key={"pk": {"S": f"del-{i}"}}
            )
            assert "Item" not in resp

    def test_batch_write_mixed_put_delete(self, dynamodb_client, hash_table):
        """BatchWriteItem with mixed PutRequests and DeleteRequests."""
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "to-delete"}, "val": {"S": "old"}},
        )

        dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": "to-create"}, "val": {"S": "new"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "to-delete"}}}},
                ]
            }
        )

        resp = dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "to-create"}}
        )
        assert resp["Item"]["val"]["S"] == "new"

        resp = dynamodb_client.get_item(
            TableName=hash_table, Key={"pk": {"S": "to-delete"}}
        )
        assert "Item" not in resp

    def test_batch_write_cross_table(
        self, dynamodb_client, hash_table, second_table
    ):
        """BatchWriteItem writes to multiple tables."""
        dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": "t1-item"}}}},
                ],
                second_table: [
                    {"PutRequest": {"Item": {"id": {"S": "t2-item"}}}},
                ],
            }
        )

        r1 = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "t1-item"}})
        r2 = dynamodb_client.get_item(TableName=second_table, Key={"id": {"S": "t2-item"}})
        assert "Item" in r1
        assert "Item" in r2

    def test_batch_write_too_many_items(self, dynamodb_client, hash_table):
        """BatchWriteItem with > 25 items returns ValidationException."""
        reqs = [
            {"PutRequest": {"Item": {"pk": {"S": f"k-{i}"}}}}
            for i in range(26)
        ]
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={hash_table: reqs}
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_duplicate_keys(self, dynamodb_client, hash_table):
        """BatchWriteItem with duplicate keys in same table returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_table: [
                        {"PutRequest": {"Item": {"pk": {"S": "dup"}}}},
                        {"PutRequest": {"Item": {"pk": {"S": "dup"}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_empty_request_items(self, dynamodb_client):
        """BatchWriteItem with empty RequestItems returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(RequestItems={})
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_unprocessed_items_empty(self, dynamodb_client, hash_table):
        """BatchWriteItem returns empty UnprocessedItems on success."""
        resp = dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": "ok-item"}}}},
                ]
            }
        )

        unprocessed = resp.get("UnprocessedItems", {})
        assert len(unprocessed) == 0

    def test_batch_write_unprocessed_items_always_present(
        self, dynamodb_client, hash_table
    ):
        """BatchWriteItem always includes UnprocessedItems, even on full success.

        Amazon DynamoDB returns ``"UnprocessedItems": {}`` on the wire when every
        write succeeds; the field is never omitted. A response that drops the
        field entirely is non-conformant.
        """
        resp = dynamodb_client.batch_write_item(
            RequestItems={
                hash_table: [
                    {"PutRequest": {"Item": {"pk": {"S": "always-present"}}}},
                ]
            }
        )

        assert "UnprocessedItems" in resp, (
            "UnprocessedItems must always be present in the BatchWriteItem "
            "response, even when empty"
        )
        assert resp["UnprocessedItems"] == {}

    def test_batch_write_put_wrong_key_type(self, dynamodb_client, hash_table):
        """BatchWriteItem PutRequest with wrong key type returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_table: [
                        {"PutRequest": {"Item": {"pk": {"N": "123"}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_put_missing_key(self, dynamodb_client, hash_range_table):
        """BatchWriteItem PutRequest missing sort key returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_range_table: [
                        {"PutRequest": {"Item": {"pk": {"S": "x"}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_put_oversized_item(self, dynamodb_client, hash_table):
        """BatchWriteItem PutRequest with item > 400 KB returns ValidationException."""
        big_value = "x" * (400 * 1024 + 1)
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_table: [
                        {"PutRequest": {"Item": {"pk": {"S": "big"}, "data": {"S": big_value}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"
        assert "Item size has exceeded" in exc_info.value.response["Error"]["Message"]

    def test_batch_write_delete_wrong_key_type(self, dynamodb_client, hash_table):
        """BatchWriteItem DeleteRequest with wrong key type returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_table: [
                        {"DeleteRequest": {"Key": {"pk": {"N": "123"}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_delete_extra_key_attrs(self, dynamodb_client, hash_table):
        """BatchWriteItem DeleteRequest with extra attributes returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    hash_table: [
                        {"DeleteRequest": {"Key": {"pk": {"S": "x"}, "extra": {"S": "y"}}}},
                    ]
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_write_rejects_empty_key_values(
        self, dynamodb_client, create_and_cleanup_table
    ):
        """BatchWriteItem rejects empty key attribute values for both binary
        and string types in both PutRequest and DeleteRequest"""

        # Set up a binary-keyed table and a string-keyed table.
        binary_table = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        create_and_cleanup_table(
            binary_table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "B"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        )
        wait_for_active(dynamodb_client, binary_table)

        string_table = f"extenddb-test-{uuid.uuid4().hex[:12]}"
        create_and_cleanup_table(
            string_table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        )
        wait_for_active(dynamodb_client, string_table)

        # Empty binary in PutRequest — exercises validate_key_sizes.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    binary_table: [
                        {"PutRequest": {"Item": {"pk": {"B": b""}}}},
                    ]
                }
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert "empty binary value" in err["Message"], err["Message"]
        assert "Key: pk" in err["Message"], err["Message"]

        # Empty binary in DeleteRequest — exercises validate_key_only via
        # the validate_batch_key_only wrapper.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    binary_table: [
                        {"DeleteRequest": {"Key": {"pk": {"B": b""}}}},
                    ]
                }
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert "empty binary value" in err["Message"], err["Message"]
        assert "Key: pk" in err["Message"], err["Message"]

        # Empty string in PutRequest — same validate_key_sizes path with the
        # string-typed message variant.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    string_table: [
                        {"PutRequest": {"Item": {"pk": {"S": ""}}}},
                    ]
                }
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert "empty string value" in err["Message"], err["Message"]
        assert "Key: pk" in err["Message"], err["Message"]

        # Empty string in DeleteRequest — exercises validate_key_only with the
        # string-typed message variant. This is a NEW behavior the CR adds:
        # validate_key_only previously had no empty-key check at all.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_write_item(
                RequestItems={
                    string_table: [
                        {"DeleteRequest": {"Key": {"pk": {"S": ""}}}},
                    ]
                }
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException", err
        assert "empty string value" in err["Message"], err["Message"]
        assert "Key: pk" in err["Message"], err["Message"]
# ── BatchGetItem key validation ───────────────────────────────────────
class TestBatchGetItemKeyValidation:
    """Tests for BatchGetItem per-key validation (M2 fix)."""

    def test_batch_get_wrong_key_type(self, dynamodb_client, hash_table):
        """BatchGetItem with wrong key type returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={
                    hash_table: {"Keys": [{"pk": {"N": "123"}}]}
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_get_missing_sort_key(self, dynamodb_client, hash_range_table):
        """BatchGetItem with missing sort key returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={
                    hash_range_table: {"Keys": [{"pk": {"S": "x"}}]}
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"

    def test_batch_get_extra_key_attrs(self, dynamodb_client, hash_table):
        """BatchGetItem with extra key attributes returns ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.batch_get_item(
                RequestItems={
                    hash_table: {"Keys": [{"pk": {"S": "x"}, "extra": {"S": "y"}}]}
                }
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"
