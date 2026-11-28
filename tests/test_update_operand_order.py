# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The first operand of `+`, `-` or `list_append` that fails is the one reported.

Measured in us-east-1: the first five rows on 2026-11-14 and 2026-11-16, the
others on 2026-11-19. Dual-target: the same assertions run against DynamoDB and
against an ExtendDB server (selected by EXTENDDB_TEST_ENDPOINT, see conftest).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

PK = "em-upd-operand"
NO_ITEM = "em-upd-operand-absent"
ITEM = {
    "pk": {"S": PK},
    "n": {"N": "10"},
    "k": {"L": [{"N": "4"}, {"N": "5"}]},
    "m": {"M": {"x": {"N": "1"}}},
    "s": {"S": "str"},
}
VALUES = {":v": {"N": "2"}, ":l": {"L": [{"S": "x"}]}}
TYPE = "An operand in the update expression has an incorrect data type"
ABSENT = "The provided expression refers to an attribute that does not exist in the item"
CANCELLED = (
    "Transaction cancelled, please refer cancellation reasons for specific reasons "
    "[ValidationError]"
)

# The operation, the assignment of its SET, the key it updates, and the text
# DynamoDB answers.
ROWS = [
    ("UpdateItem", "a = s + z", PK, TYPE),
    ("UpdateItem", "l = list_append(s, z)", PK, TYPE),
    ("UpdateItem", "a = z + s", PK, ABSENT),
    ("UpdateItem", "a = s + :v", PK, TYPE),
    ("UpdateItem", "l = list_append(s, :l)", PK, TYPE),
    ("UpdateItem", "a = s - z", PK, TYPE),
    ("UpdateItem", "a = n + z", PK, ABSENT),
    ("UpdateItem", "a = s + m.q", PK, TYPE),
    ("UpdateItem", "l = list_append(k, z)", PK, ABSENT),
    ("UpdateItem", "a = s + z", NO_ITEM, ABSENT),
    ("TransactWriteItems", "a = s + z", PK, TYPE),
    ("TransactWriteItems", "a = z + s", PK, ABSENT),
    ("ExecuteStatement", "a = s + z", PK, TYPE),
    ("ExecuteStatement", "a = z + s", PK, ABSENT),
]


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _update(table: str, pk: str, assignment: str) -> dict:
    """The Update of `SET <assignment>` on `pk`, with the values it names."""
    update = {"TableName": table, "Key": {"pk": {"S": pk}}, "UpdateExpression": f"SET {assignment}"}
    values = {name: value for name, value in VALUES.items() if name in assignment}
    if values:
        update["ExpressionAttributeValues"] = values
    return update


def _send(client, operation: str, table: str, pk: str, assignment: str) -> None:
    if operation == "UpdateItem":
        client.update_item(**_update(table, pk, assignment))
    elif operation == "TransactWriteItems":
        client.transact_write_items(TransactItems=[{"Update": _update(table, pk, assignment)}])
    else:
        client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET {assignment} WHERE pk = '{pk}'"
        )


def _stored(client, table: str, pk: str) -> dict | None:
    return client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True).get(
        "Item"
    )


@pytest.mark.parametrize(
    "operation, assignment, pk, message",
    ROWS,
    ids=[f"{op} SET {a}{' (no item)' if pk == NO_ITEM else ''}" for op, a, pk, _ in ROWS],
)
def test_the_first_operand_that_fails_is_reported(
    dynamodb_client, table, operation, assignment, pk, message
):
    dynamodb_client.put_item(TableName=table, Item=ITEM)
    with pytest.raises(ClientError) as exc_info:
        _send(dynamodb_client, operation, table, pk, assignment)
    response = exc_info.value.response
    if operation == "TransactWriteItems":
        assert (response["Error"]["Code"], response["Error"]["Message"]) == (
            "TransactionCanceledException",
            CANCELLED,
        )
        assert response["CancellationReasons"] == [{"Code": "ValidationError", "Message": message}]
    else:
        assert (response["Error"]["Code"], response["Error"]["Message"]) == (
            "ValidationException",
            message,
        )
    assert _stored(dynamodb_client, table, PK) == ITEM
    assert _stored(dynamodb_client, table, NO_ITEM) is None
