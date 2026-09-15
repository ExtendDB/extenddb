# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Developer-mode authorization tests.

`dev-mode` opens the IAM policy decision for an authenticated caller: SigV4 is
still verified, but no `dynamodb:*` policy is required. The seeded `dev` user
holds only `SelfServicePolicy` (four `iam:*` actions on its own ARN), so any
operation that still reaches the IAM evaluator is denied. That makes an
`AccessDeniedException` here proof that an operation is bypassing the dev-mode
gate, not a policy misconfiguration.

Batch and transaction operations regressed exactly this way: their per-table
authorization branch returned before the dev-mode check was reached, so
`BatchGetItem`, `BatchWriteItem`, `TransactGetItems` and `TransactWriteItems`
failed while every single-item operation succeeded.

These require a server built and started in dev mode, so they are gated on
EXTENDDB_TEST_DEV_MODE and excluded from the backend-agnostic suite, which runs
against a production-mode server where these operations are correctly denied.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

pytestmark = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_DEV_MODE", "").strip() != "1",
    reason="requires a dev-mode server (set EXTENDDB_TEST_DEV_MODE=1)",
)


def _assert_not_denied(op_name: str, call):
    """Run `call`, failing loudly if the operation was denied rather than served.

    Any other ClientError is re-raised: this test is about the authorization
    decision, so a genuine request error should not be swallowed into a pass.
    """
    try:
        return call()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("AccessDeniedException", "UnrecognizedClientException"):
            pytest.fail(
                f"{op_name} was denied in dev mode with {code}: "
                f"{exc.response['Error'].get('Message', '')}"
            )
        raise


@pytest.fixture()
def dev_table(dynamodb_client, unique_table_name):
    dynamodb_client.create_table(
        TableName=unique_table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, unique_table_name)
    yield unique_table_name
    try:
        dynamodb_client.delete_table(TableName=unique_table_name)
    except ClientError:
        pass


class TestDevModeOpensAuthorization:
    """Every data-plane operation is served in dev mode, not just single-item ones."""

    def test_single_item_operations_are_not_denied(self, dynamodb_client, dev_table):
        # These already worked. They are kept so a regression that closes
        # authorization altogether is distinguishable from one that affects only
        # the batch and transaction branch.
        _assert_not_denied(
            "PutItem",
            lambda: dynamodb_client.put_item(
                TableName=dev_table, Item={"pk": {"S": "a"}, "v": {"N": "1"}}
            ),
        )
        _assert_not_denied(
            "GetItem",
            lambda: dynamodb_client.get_item(TableName=dev_table, Key={"pk": {"S": "a"}}),
        )
        _assert_not_denied(
            "UpdateItem",
            lambda: dynamodb_client.update_item(
                TableName=dev_table,
                Key={"pk": {"S": "a"}},
                UpdateExpression="SET #v = :v",
                ExpressionAttributeNames={"#v": "v"},
                ExpressionAttributeValues={":v": {"N": "2"}},
            ),
        )
        _assert_not_denied(
            "Query",
            lambda: dynamodb_client.query(
                TableName=dev_table,
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "a"}},
            ),
        )
        _assert_not_denied("Scan", lambda: dynamodb_client.scan(TableName=dev_table))
        _assert_not_denied(
            "DeleteItem",
            lambda: dynamodb_client.delete_item(
                TableName=dev_table, Key={"pk": {"S": "a"}}
            ),
        )

    def test_batch_write_item_is_not_denied(self, dynamodb_client, dev_table):
        resp = _assert_not_denied(
            "BatchWriteItem",
            lambda: dynamodb_client.batch_write_item(
                RequestItems={
                    dev_table: [
                        {"PutRequest": {"Item": {"pk": {"S": "b1"}}}},
                        {"PutRequest": {"Item": {"pk": {"S": "b2"}}}},
                    ]
                }
            ),
        )
        assert not resp.get("UnprocessedItems", {}).get(dev_table)

    def test_batch_get_item_is_not_denied(self, dynamodb_client, dev_table):
        dynamodb_client.put_item(TableName=dev_table, Item={"pk": {"S": "g1"}})
        resp = _assert_not_denied(
            "BatchGetItem",
            lambda: dynamodb_client.batch_get_item(
                RequestItems={dev_table: {"Keys": [{"pk": {"S": "g1"}}]}}
            ),
        )
        assert len(resp["Responses"][dev_table]) == 1

    def test_transact_write_items_is_not_denied(self, dynamodb_client, dev_table):
        _assert_not_denied(
            "TransactWriteItems",
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {"Put": {"TableName": dev_table, "Item": {"pk": {"S": "t1"}}}}
                ]
            ),
        )
        got = dynamodb_client.get_item(TableName=dev_table, Key={"pk": {"S": "t1"}})
        assert "Item" in got

    def test_transact_get_items_is_not_denied(self, dynamodb_client, dev_table):
        dynamodb_client.put_item(TableName=dev_table, Item={"pk": {"S": "tg1"}})
        resp = _assert_not_denied(
            "TransactGetItems",
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    {"Get": {"TableName": dev_table, "Key": {"pk": {"S": "tg1"}}}}
                ]
            ),
        )
        assert resp["Responses"][0]["Item"]["pk"]["S"] == "tg1"
