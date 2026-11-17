# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Nesting depth of UpdateExpression and ConditionExpression.

An UpdateExpression nests a level for each parenthesized value and each
function call (``if_not_exists``, ``list_append``), and a ConditionExpression
nests a level for each ``size`` call read as an operand, as it does for
parentheses and ``NOT``. ExtendDB refuses an expression nested past
``max_expression_depth`` (150 by default) with a ValidationException, before
the parser recurses further. The tests nest each form one level past the
limit, and as deep as the configured size and token limits allow, and then
read the item back to show the server still answers and nothing was written.

The deepest form uses the server's ``[limits] max_expression_length_bytes``
and ``max_expression_tokens`` when its config file sets them, so a server
started with raised limits is driven far past the depth limit. The error
class is asserted; the message is not, since the service's wording for this
condition has not been measured.

A PartiQL UPDATE is written as a classic UpdateExpression, so the same limit
applies to it. A nested ``if_not_exists`` step becomes two classic levels (the
call and a parenthesized sum), so a statement with more than 75 such steps is
refused, although the PartiQL parser accepts it.

Run with EXTENDDB_TEST_ENDPOINT unset to assert the same against the service.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from conftest import config_value, scoped_table

DEPTH_LIMIT = 150
DEFAULT_SIZE_LIMIT = 4096
DEFAULT_TOKEN_LIMIT = 4096
# Deep enough to have overflowed a server worker's stack before the parsers
# checked the depth, when the size and token limits allow it.
MOST_LEVELS = 200_000


def _limit(key: str, default: int) -> int:
    if not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip():
        return default
    value = config_value("limits", key)
    return int(value.replace("_", "")) if value else default


SIZE_LIMIT = _limit("max_expression_length_bytes", DEFAULT_SIZE_LIMIT)
TOKEN_LIMIT = _limit("max_expression_tokens", DEFAULT_TOKEN_LIMIT)


def _levels(fixed_bytes: int, bytes_per_level: int, fixed_tokens: int, tokens_per_level: int) -> int:
    """The most levels a form fits in under the size and token limits."""
    by_size = (SIZE_LIMIT - fixed_bytes) // bytes_per_level
    by_tokens = (TOKEN_LIMIT - fixed_tokens) // tokens_per_level
    return min(by_size, by_tokens, MOST_LEVELS)


def groups(levels: int) -> str:
    """``SET qty=(qty+(qty+...:v))``: 10 bytes and 4 tokens, then 6 bytes and
    4 tokens a level."""
    return "SET qty=" + "(qty+" * levels + ":v" + ")" * levels


def calls(levels: int) -> str:
    """``SET lst=list_append(lst,...:v)``: 10 bytes and 4 tokens, then 17
    bytes and 5 tokens a level. With ``lst`` and ``:v`` empty lists the
    expression is valid, so only its depth refuses it."""
    return "SET lst=" + "list_append(lst," * levels + ":v" + ")" * levels


def sizes(levels: int) -> str:
    """``size(size(...qty))>:v``: 6 bytes and 3 tokens, then 6 bytes and 3
    tokens a level."""
    return "size(" * levels + "qty" + ")" * levels + ">:v"


GROUPS_DEEPEST = _levels(10, 6, 4, 4)
CALLS_DEEPEST = _levels(10, 17, 4, 5)
SIZES_DEEPEST = _levels(6, 6, 3, 3)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k"}, "qty": {"N": "1"}, "lst": {"L": []}},
        )
        yield name


def _assert_refused(call) -> None:
    with pytest.raises(ClientError) as exc_info:
        call()
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"


def _assert_unchanged(client, table: str) -> None:
    item = client.get_item(TableName=table, Key={"pk": {"S": "k"}}, ConsistentRead=True)["Item"]
    assert item == {"pk": {"S": "k"}, "qty": {"N": "1"}, "lst": {"L": []}}


def _update(client, table: str, expression: str, value: dict) -> None:
    client.update_item(
        TableName=table,
        Key={"pk": {"S": "k"}},
        UpdateExpression=expression,
        ExpressionAttributeValues={":v": value},
    )


class TestUpdateExpressionDepth:
    def test_groups_one_past_the_limit_are_refused(self, dynamodb_client, table):
        _assert_refused(lambda: _update(dynamodb_client, table, groups(DEPTH_LIMIT + 1), {"N": "1"}))
        _assert_unchanged(dynamodb_client, table)

    def test_deepest_groups_are_refused_and_the_server_stays_up(self, dynamodb_client, table):
        assert GROUPS_DEEPEST > DEPTH_LIMIT
        _assert_refused(lambda: _update(dynamodb_client, table, groups(GROUPS_DEEPEST), {"N": "1"}))
        _assert_unchanged(dynamodb_client, table)

    def test_calls_one_past_the_limit_are_refused(self, dynamodb_client, table):
        _assert_refused(lambda: _update(dynamodb_client, table, calls(DEPTH_LIMIT + 1), {"L": []}))
        _assert_unchanged(dynamodb_client, table)

    def test_deepest_calls_are_refused_and_the_server_stays_up(self, dynamodb_client, table):
        assert CALLS_DEEPEST > DEPTH_LIMIT
        _assert_refused(lambda: _update(dynamodb_client, table, calls(CALLS_DEEPEST), {"L": []}))
        _assert_unchanged(dynamodb_client, table)

    def test_deepest_groups_in_a_transaction_are_refused(self, dynamodb_client, table):
        _assert_refused(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": table,
                            "Key": {"pk": {"S": "k"}},
                            "UpdateExpression": groups(GROUPS_DEEPEST),
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            )
        )
        _assert_unchanged(dynamodb_client, table)


class TestConditionExpressionDepth:
    def test_size_calls_one_past_the_limit_are_refused(self, dynamodb_client, table):
        _assert_refused(
            lambda: dynamodb_client.delete_item(
                TableName=table,
                Key={"pk": {"S": "k"}},
                ConditionExpression=sizes(DEPTH_LIMIT + 1),
                ExpressionAttributeValues={":v": {"N": "0"}},
            )
        )
        _assert_unchanged(dynamodb_client, table)

    def test_deepest_size_calls_are_refused_and_the_server_stays_up(self, dynamodb_client, table):
        assert SIZES_DEEPEST > DEPTH_LIMIT
        _assert_refused(
            lambda: dynamodb_client.delete_item(
                TableName=table,
                Key={"pk": {"S": "k"}},
                ConditionExpression=sizes(SIZES_DEEPEST),
                ExpressionAttributeValues={":v": {"N": "0"}},
            )
        )
        _assert_unchanged(dynamodb_client, table)

    def test_deepest_size_calls_in_a_filter_are_refused(self, dynamodb_client, table):
        _assert_refused(
            lambda: dynamodb_client.scan(
                TableName=table,
                FilterExpression=sizes(SIZES_DEEPEST),
                ExpressionAttributeValues={":v": {"N": "0"}},
            )
        )
        _assert_unchanged(dynamodb_client, table)


# One nested if_not_exists step is two levels of the classic expression
# the PartiQL UPDATE is turned into, so 75 steps fill the limit and 76 pass it.
PARTIQL_STEPS_AT_LIMIT = DEPTH_LIMIT // 2


def _partiql_nested(table: str, steps: int) -> str:
    value = "if_not_exists(fresh, 1 + " * steps + "1" + ")" * steps
    return f"UPDATE \"{table}\" SET fresh = {value} WHERE pk = 'k'"


class TestPartiQLUpdateDepth:
    def test_steps_at_the_limit_run(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "k"}, "qty": {"N": "1"}})
            dynamodb_client.execute_statement(Statement=_partiql_nested(name, PARTIQL_STEPS_AT_LIMIT))
            item = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k"}}, ConsistentRead=True)["Item"]
            assert item["fresh"] == {"N": str(PARTIQL_STEPS_AT_LIMIT + 1)}

    def test_one_step_past_the_limit_is_refused_and_the_item_is_unchanged(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            original = {"pk": {"S": "k"}, "qty": {"N": "1"}}
            dynamodb_client.put_item(TableName=name, Item=original)
            _assert_refused(
                lambda: dynamodb_client.execute_statement(
                    Statement=_partiql_nested(name, PARTIQL_STEPS_AT_LIMIT + 1)
                )
            )
            item = dynamodb_client.get_item(TableName=name, Key={"pk": {"S": "k"}}, ConsistentRead=True)["Item"]
            assert item == original
