# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TransactWriteItems token replay and the two Return fields.

A ClientRequestToken replay is compared against the first request by
TransactItems, ReturnConsumedCapacity and ReturnItemCollectionMetrics. An
absent Return field is NONE. A replay that changes either field answers
IdempotentParameterMismatchException with the message

    Specified idempotent token was used with different request parameters
    within the idempotency window

A replay with matching fields answers 200 and reports the replay as a
transactional read (ReadCapacityUnits, no WriteCapacityUnits) when
ReturnConsumedCapacity asks for capacity, and carries no
ItemCollectionMetrics. An invalid enum value on the replay is refused by
validation before the token is compared.

Every expected answer here was measured against Amazon DynamoDB in
us-east-1 on 2026-10-24 (17:31 UTC and 17:36 UTC), client retries off.
The tests run against both Amazon DynamoDB and ExtendDB.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

MISMATCH = "IdempotentParameterMismatchException"
MISMATCH_MESSAGE = (
    "Specified idempotent token was used with different request parameters "
    "within the idempotency window"
)
RCC_ENUM_MESSAGE = (
    "1 validation error detected: Value '{value}' at 'returnConsumedCapacity' "
    "failed to satisfy constraint: Member must satisfy enum value set: "
    "[INDEXES, TOTAL, NONE]"
)
RICM_ENUM_MESSAGE = (
    "1 validation error detected: Value '{value}' at 'returnItemCollectionMetrics' "
    "failed to satisfy constraint: Member must satisfy enum value set: [SIZE, NONE]"
)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _items(table: str, pk: str) -> list[dict]:
    return [
        {
            "Update": {
                "TableName": table,
                "Key": {"pk": {"S": pk}},
                "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"S": "x"}},
            }
        }
    ]


def _fresh() -> tuple[str, str]:
    return f"tok-{uuid.uuid4().hex}", uuid.uuid4().hex[:32]


def _call(client, table: str, pk: str, token: str, **extra) -> dict:
    resp = client.transact_write_items(
        TransactItems=_items(table, pk), ClientRequestToken=token, **extra
    )
    resp.pop("ResponseMetadata", None)
    return resp


def _assert_mismatch(exc: pytest.ExceptionInfo) -> None:
    err = exc.value.response["Error"]
    assert err["Code"] == MISMATCH
    assert err["Message"] == MISMATCH_MESSAGE


# ---------------------------------------------------------------------------
# A changed Return field is a different request
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("first", "replay"),
    [
        pytest.param({}, {"ReturnConsumedCapacity": "TOTAL"}, id="none-then-rcc-total"),
        pytest.param(
            {}, {"ReturnItemCollectionMetrics": "SIZE"}, id="none-then-ricm-size"
        ),
        pytest.param({"ReturnConsumedCapacity": "TOTAL"}, {}, id="rcc-total-then-none"),
        pytest.param(
            {"ReturnConsumedCapacity": "TOTAL"},
            {"ReturnConsumedCapacity": "INDEXES"},
            id="rcc-total-then-indexes",
        ),
        pytest.param(
            {"ReturnConsumedCapacity": "INDEXES"},
            {"ReturnConsumedCapacity": "TOTAL"},
            id="rcc-indexes-then-total",
        ),
        pytest.param(
            {"ReturnItemCollectionMetrics": "SIZE"},
            {"ReturnItemCollectionMetrics": "NONE"},
            id="ricm-size-then-none",
        ),
        pytest.param(
            {},
            {"ReturnConsumedCapacity": "TOTAL", "ReturnItemCollectionMetrics": "SIZE"},
            id="none-then-both",
        ),
    ],
)
def test_changed_return_field_is_a_mismatch(dynamodb_client, table, first, replay):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token, **first)
    with pytest.raises(ClientError) as exc:
        _call(dynamodb_client, table, pk, token, **replay)
    _assert_mismatch(exc)


def test_changed_items_and_return_field_is_a_mismatch(dynamodb_client, table):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token)
    with pytest.raises(ClientError) as exc:
        _call(dynamodb_client, table, pk + "-b", token, ReturnConsumedCapacity="TOTAL")
    _assert_mismatch(exc)
    assert "Item" not in dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": pk + "-b"}}, ConsistentRead=True
    )


def test_changed_items_alone_is_a_mismatch(dynamodb_client, table):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token)
    with pytest.raises(ClientError) as exc:
        _call(dynamodb_client, table, pk + "-b", token)
    _assert_mismatch(exc)


# ---------------------------------------------------------------------------
# An absent Return field is NONE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("first", "replay"),
    [
        pytest.param({}, {}, id="same-body"),
        pytest.param({}, {"ReturnConsumedCapacity": "NONE"}, id="none-then-rcc-none"),
        pytest.param(
            {}, {"ReturnItemCollectionMetrics": "NONE"}, id="none-then-ricm-none"
        ),
        pytest.param(
            {"ReturnConsumedCapacity": "NONE", "ReturnItemCollectionMetrics": "NONE"},
            {},
            id="both-none-then-absent",
        ),
    ],
)
def test_none_and_absent_replay(dynamodb_client, table, first, replay):
    pk, token = _fresh()
    assert _call(dynamodb_client, table, pk, token, **first) == {}
    assert _call(dynamodb_client, table, pk, token, **replay) == {}


# ---------------------------------------------------------------------------
# The matching replay's body
# ---------------------------------------------------------------------------


def test_matching_replay_total_reports_read_capacity(dynamodb_client, table):
    pk, token = _fresh()
    first = _call(dynamodb_client, table, pk, token, ReturnConsumedCapacity="TOTAL")
    assert first["ConsumedCapacity"] == [
        {"TableName": table, "CapacityUnits": 2.0, "WriteCapacityUnits": 2.0}
    ]
    replay = _call(dynamodb_client, table, pk, token, ReturnConsumedCapacity="TOTAL")
    assert replay == {
        "ConsumedCapacity": [
            {"TableName": table, "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}
        ]
    }


def test_matching_replay_indexes_reports_read_capacity(dynamodb_client, table):
    pk, token = _fresh()
    first = _call(dynamodb_client, table, pk, token, ReturnConsumedCapacity="INDEXES")
    assert first["ConsumedCapacity"] == [
        {
            "TableName": table,
            "CapacityUnits": 2.0,
            "WriteCapacityUnits": 2.0,
            "Table": {"CapacityUnits": 2.0, "WriteCapacityUnits": 2.0},
        }
    ]
    replay = _call(dynamodb_client, table, pk, token, ReturnConsumedCapacity="INDEXES")
    assert replay == {
        "ConsumedCapacity": [
            {
                "TableName": table,
                "CapacityUnits": 2.0,
                "ReadCapacityUnits": 2.0,
                "Table": {"CapacityUnits": 2.0, "ReadCapacityUnits": 2.0},
            }
        ]
    }


def test_matching_replay_size_carries_no_metrics(dynamodb_client, table):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token, ReturnItemCollectionMetrics="SIZE")
    replay = _call(dynamodb_client, table, pk, token, ReturnItemCollectionMetrics="SIZE")
    assert replay == {}


# ---------------------------------------------------------------------------
# Validation of the Return fields runs before the token is compared
# ---------------------------------------------------------------------------


def _raw(client, table: str, pk: str, token: str, **extra) -> ClientError:
    with pytest.raises(ClientError) as exc:
        client.transact_write_items(
            TransactItems=_items(table, pk), ClientRequestToken=token, **extra
        )
    return exc.value


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        pytest.param("ReturnConsumedCapacity", "total", RCC_ENUM_MESSAGE, id="rcc-lowercase"),
        pytest.param("ReturnConsumedCapacity", "BOGUS", RCC_ENUM_MESSAGE, id="rcc-bogus"),
        pytest.param(
            "ReturnItemCollectionMetrics", "BOGUS", RICM_ENUM_MESSAGE, id="ricm-bogus"
        ),
    ],
)
def test_invalid_return_value_on_replay_is_a_validation_error(
    dynamodb_client, dynamodb_client_no_validation, table, field, value, message
):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token)
    err = _raw(dynamodb_client_no_validation, table, pk, token, **{field: value})
    assert err.response["Error"]["Code"] == "ValidationException"
    assert err.response["Error"]["Message"] == message.format(value=value)


def test_invalid_return_value_wins_over_changed_items(
    dynamodb_client, dynamodb_client_no_validation, table
):
    pk, token = _fresh()
    _call(dynamodb_client, table, pk, token)
    err = _raw(
        dynamodb_client_no_validation,
        table,
        pk + "-b",
        token,
        ReturnConsumedCapacity="BOGUS",
    )
    assert err.response["Error"]["Code"] == "ValidationException"
    assert err.response["Error"]["Message"] == RCC_ENUM_MESSAGE.format(value="BOGUS")


def test_invalid_return_value_without_token(dynamodb_client_no_validation, table):
    pk = uuid.uuid4().hex[:32]
    with pytest.raises(ClientError) as exc:
        dynamodb_client_no_validation.transact_write_items(
            TransactItems=_items(table, pk), ReturnConsumedCapacity="total"
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == RCC_ENUM_MESSAGE.format(value="total")
