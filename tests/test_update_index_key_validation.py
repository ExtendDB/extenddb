# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Secondary-index key emptiness validation on UpdateItem (standalone and transacted).

Setting a GSI key attribute to an empty binary or empty string must be refused
with a 400 ValidationException on every backend, the same as PutItem and the
same as the service. See issue #410: the Cassandra backend skipped this check on
both update paths and returned a 500.

The expected messages are the UpdateExpression-context forms produced by
crates/core/src/validation/mod.rs::validate_index_key_not_empty
(SecondaryIndexEmptyContext::UpdateExpression) and asserted by that module's
own unit tests. That context carries no "IndexName/IndexKey" suffix (unlike the
PutItem/Item context), so these assertions do not include one.

These cases carry no backend skip: they assert index-key validation, not the
item-size rule. (The size-rule classes in test_item_size_at_limit.py, from
unmerged PR #409, are the ones that skip on Cassandra/MongoDB. Do not confuse the
two.)

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

# Exact UpdateExpression-context messages from validate_index_key_not_empty.
EMPTY_BINARY_MESSAGE = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty binary value."
)
EMPTY_STRING_MESSAGE = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty string value."
)


def _err(exc_info) -> dict:
    return exc_info.value.response["Error"]


@pytest.fixture(scope="class")
def gsi_table(dynamodb_client):
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "bidx", "AttributeType": "B"},
            {"AttributeName": "sidx", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-bidx",
                "KeySchema": [{"AttributeName": "bidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            },
            {
                "IndexName": "by-sidx",
                "KeySchema": [{"AttributeName": "sidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            },
        ],
    ) as name:
        yield name


class TestEmptyIndexKeyOnUpdate:
    """UpdateItem setting a GSI key to an empty value is refused on every backend."""

    def test_standalone_update_refuses_empty_binary_index_key(self, dynamodb_client, gsi_table):
        dynamodb_client.put_item(TableName=gsi_table, Item={"pk": {"S": "eb"}, "v": {"S": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=gsi_table,
                Key={"pk": {"S": "eb"}},
                UpdateExpression="SET bidx = :b",
                ExpressionAttributeValues={":b": {"B": b""}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_BINARY_MESSAGE
        got = dynamodb_client.get_item(
            TableName=gsi_table, Key={"pk": {"S": "eb"}}, ConsistentRead=True
        )
        assert "bidx" not in got["Item"]

    def test_standalone_update_refuses_empty_string_index_key(self, dynamodb_client, gsi_table):
        dynamodb_client.put_item(TableName=gsi_table, Item={"pk": {"S": "es"}, "v": {"S": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=gsi_table,
                Key={"pk": {"S": "es"}},
                UpdateExpression="SET sidx = :s",
                ExpressionAttributeValues={":s": {"S": ""}},
            )
        assert _err(exc_info)["Code"] == "ValidationException"
        assert _err(exc_info)["Message"] == EMPTY_STRING_MESSAGE
        got = dynamodb_client.get_item(
            TableName=gsi_table, Key={"pk": {"S": "es"}}, ConsistentRead=True
        )
        assert "sidx" not in got["Item"]

    def test_transacted_update_refuses_empty_binary_index_key(self, dynamodb_client, gsi_table):
        dynamodb_client.put_item(TableName=gsi_table, Item={"pk": {"S": "tb"}, "v": {"S": "1"}})
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": gsi_table,
                            "Key": {"pk": {"S": "tb"}},
                            "UpdateExpression": "SET bidx = :b",
                            "ExpressionAttributeValues": {":b": {"B": b""}},
                        }
                    }
                ]
            )
        # A per-item validation failure inside a transaction is reported as a
        # top-level TransactionCanceledException with a ValidationError
        # cancellation reason, matching real DynamoDB (see
        # tests/test_transaction_key_size_validation.py for the same pattern).
        resp = exc_info.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        reasons = resp.get("CancellationReasons")
        assert reasons, "expected CancellationReasons in the error response"
        assert any(r.get("Code") == "ValidationError" for r in reasons), reasons
        got = dynamodb_client.get_item(
            TableName=gsi_table, Key={"pk": {"S": "tb"}}, ConsistentRead=True
        )
        assert "bidx" not in got["Item"]
