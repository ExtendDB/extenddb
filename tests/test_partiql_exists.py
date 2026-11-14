# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for an EXISTS(SELECT ...) member of ExecuteTransaction
beside a member on a missing table.

An EXISTS member on a missing table cancels the transaction with a
ResourceNotFound reason at its own position, and None for the other members.
An EXISTS member that is not a read of one item with a further condition fails
the whole request instead, even when another member names a missing table, and
so does one through an index. A projection, a projected call or an ORDER BY in
the SELECT leaves the member a plain condition check.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected value was measured on the service (2026-11-02).
Tables use the ``extenddb-measure-pqe-`` prefix and are deleted at the end.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
NOT_FOUND = {"Code": "ResourceNotFound", "Message": "Requested resource not found"}
NONE = {"Code": "None"}


@pytest.fixture(scope="module")
def tables(dynamodb_client):
    """A pk(S)/sk(S) table and the name of a table that does not exist."""
    prefix = f"extenddb-measure-pqe-{uuid.uuid4().hex[:12]}"
    name = f"{prefix}-c"
    dynamodb_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield {"table": name, "absent": f"{prefix}-absent"}
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


@pytest.fixture
def key(dynamodb_client, tables) -> str:
    """The partition key of a fresh item (sk 's', n 7)."""
    key = f"ex-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(
        TableName=tables["table"],
        Item={"pk": {"S": key}, "sk": {"S": "s"}, "n": {"N": "7"}},
    )
    return key


def refusal(client, statements: list[dict]) -> dict:
    with pytest.raises(ClientError) as excinfo:
        client.execute_transaction(TransactStatements=statements)
    return excinfo.value.response


def extra(client, tables, key: str):
    item = client.get_item(
        TableName=tables["table"],
        Key={"pk": {"S": key}, "sk": {"S": "s"}},
        ConsistentRead=True,
    )["Item"]
    return item.get("extra")


@pytest.mark.parametrize("position", [0, 1])
def test_an_exists_member_on_a_missing_table_is_not_found_at_its_position(
    dynamodb_client, tables, key, position
):
    exists = {
        "Statement": (
            f"EXISTS(SELECT * FROM \"{tables['absent']}\" "
            f"WHERE pk = '{key}' AND sk = 's' AND n = 7)"
        )
    }
    statements = [
        {"Statement": f"UPDATE \"{tables['table']}\" SET extra = 1 WHERE pk = '{key}' AND sk = 's'"}
    ]
    statements.insert(position, exists)
    response = refusal(dynamodb_client, statements)
    reasons = [NONE, NONE]
    reasons[position] = NOT_FOUND
    codes = ", ".join(reason["Code"] for reason in reasons)
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [{codes}]",
    }
    assert response["CancellationReasons"] == reasons
    assert extra(dynamodb_client, tables, key) is None


def test_an_exists_member_with_a_partial_key_fails_the_request_beside_a_missing_table(
    dynamodb_client, tables, key
):
    response = refusal(
        dynamodb_client,
        [
            {
                "Statement": (
                    f"EXISTS(SELECT * FROM \"{tables['table']}\" WHERE pk = '{key}' AND n = 7)"
                )
            },
            {
                "Statement": (
                    f"UPDATE \"{tables['absent']}\" SET extra = 1 WHERE pk = '{key}' AND sk = 's'"
                )
            },
        ],
    )
    assert response["Error"] == {
        "Code": "ValidationException",
        "Message": (
            "Validation failed in TransactStatements[0]: EXISTS() must contain a single item "
            "read with additional condition"
        ),
    }


def test_an_exists_member_through_an_index_is_refused(dynamodb_client, tables, key):
    response = refusal(
        dynamodb_client,
        [
            {
                "Statement": (
                    f"EXISTS(SELECT * FROM \"{tables['table']}\".\"idx\" "
                    f"WHERE pk = '{key}' AND sk = 's' AND n = 7)"
                )
            },
            {
                "Statement": (
                    f"UPDATE \"{tables['table']}\" SET extra = 1 WHERE pk = '{key}' AND sk = 's'"
                )
            },
        ],
    )
    assert response["Error"] == {
        "Code": "ValidationException",
        "Message": (
            "Validation failed in TransactStatements[0]: Operations on indices are not "
            "supported in transactions"
        ),
    }
    assert extra(dynamodb_client, tables, key) is None


@pytest.mark.parametrize(
    "select",
    [
        "SELECT n FROM \"{t}\" WHERE pk = '{k}' AND sk = 's' AND n = 7",
        "SELECT COUNT(*) FROM \"{t}\" WHERE pk = '{k}' AND sk = 's' AND n = 7",
        "SELECT * FROM \"{t}\" WHERE pk = '{k}' AND sk = 's' AND n = 7 ORDER BY pk",
    ],
    ids=["projection", "count", "order-by"],
)
def test_an_exists_member_checks_only_its_condition(dynamodb_client, tables, key, select):
    other = f"ex-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(
        TableName=tables["table"], Item={"pk": {"S": other}, "sk": {"S": "s"}}
    )
    resp = dynamodb_client.execute_transaction(
        TransactStatements=[
            {"Statement": f"EXISTS({select.format(t=tables['table'], k=key)})"},
            {
                "Statement": (
                    f"UPDATE \"{tables['table']}\" SET extra = 1 WHERE pk = '{other}' "
                    "AND sk = 's'"
                )
            },
        ]
    )
    assert resp["Responses"] == []
    assert extra(dynamodb_client, tables, other) == {"N": "1"}
