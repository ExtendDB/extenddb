# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the item nesting check on PartiQL writes.

A parameter may hold 32 maps around its leaf and a stored attribute 31. A
PartiQL INSERT or UPDATE of a 32-level parameter passes the parameter check and
meets the item check, which answers a shorter message than PutItem and
UpdateItem give the same value. An UPDATE meets the item check only once its
condition holds and its SET applies: on a missing item it fails its condition,
and a SET through a path the item does not have answers the update's own
message. A SET of a shallow parameter through a path that takes a stored value
past the limit meets the same item check: in ExecuteStatement, as the
statement's ValidationError entry in BatchExecuteStatement, as a ValidationError
reason in ExecuteTransaction, and ahead of the size check when the statement
also sets a 409,500-byte parameter. A SET of a key attribute is refused
ahead of all of this, and a 32-level value the statement does not store (an
if_not_exists value the item does not take, a value only in the WHERE clause)
is accepted. In ExecuteTransaction two members on one item are refused first;
otherwise the item check cancels the transaction with a ValidationError reason
at the member's position, beside each other member's own reason. A 33-level
parameter is refused by the parameter check in every form.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected value was measured on the service (2026-11-02; the
SET through a path 30 maps deep on 2026-11-10).
Tables use the ``extenddb-measure-pqd-`` prefix and are deleted at the end.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

ITEM_CHECK = "Nesting Levels have exceeded supported limits"
NESTED = f"{ITEM_CHECK}: Attributes in the item have nested levels beyond supported limit"
BAD_PATH = "The document path provided in the update expression is invalid for update"
SIZE_TO_UPDATE = "Item size to update has exceeded the maximum allowed size"
CONDITION_FAILED = "The conditional request failed"
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
ITEM_REASON = {"Code": "ValidationError", "Message": ITEM_CHECK}
FAILED_REASON = {"Code": "ConditionalCheckFailed", "Message": CONDITION_FAILED}
KEY_UPDATE = (
    "One or more parameter values were invalid: Cannot update attribute pk. This attribute "
    "is part of the key"
)

# ExtendDB hands a PartiQL parameter to UpdateItem or DeleteItem as an
# ExpressionAttributeValue, and those refuse every value nested 32 levels. The
# service runs a statement that does not store such a value (measured
# 2026-11-02).
UNSTORED_DEEP_VALUE = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()),
    reason="ExtendDB refuses a 32-level parameter the statement does not store",
    raises=ClientError,
    strict=True,
)

# The PostgreSQL and SQLite storage layers size updates the way the service
# does. The MongoDB layer applies no limit to updates, and the Cassandra layer
# sizes every update against a fixed 409,600 bytes (see
# docs/differences-from-dynamodb.md) and words the refusal "Item size has
# exceeded the maximum allowed size" (validate_item_size in
# crates/core/src/validation/mod.rs). The case that needs the size refusal is
# skipped on those two backends, as in tests/test_partiql_item_size.py.
refused_for_size_on_default_backends_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_BACKEND", "") in {"mongodb", "cassandra"},
    reason="updates are sized against a fixed limit (Cassandra) or not at all (MongoDB); "
    "see docs/differences-from-dynamodb.md",
)


def deep(levels: int) -> dict:
    """``levels`` maps wrapped around a string."""
    value: dict = {"S": "leaf"}
    for _ in range(levels):
        value = {"M": {"n": value}}
    return value


@pytest.fixture(scope="module")
def table(dynamodb_client):
    name = f"extenddb-measure-pqd-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    )
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


@pytest.fixture
def stored(dynamodb_client, table) -> str:
    """The key of a fresh item that has no ``x`` and no ``y``."""
    key = f"exists-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": key}, "a": {"N": "1"}})
    return key


def error(call) -> dict:
    with pytest.raises(ClientError) as excinfo:
        call()
    return excinfo.value.response["Error"]


def validation_message(call) -> str:
    err = error(call)
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def read_x(client, table: str, key: str) -> dict | None:
    item = client.get_item(TableName=table, Key={"pk": {"S": key}}, ConsistentRead=True)["Item"]
    return item.get("x")


def stored_item(client, table: str, key: str) -> dict | None:
    return client.get_item(TableName=table, Key={"pk": {"S": key}}, ConsistentRead=True).get(
        "Item"
    )


def cancellation(client, statements: list[dict]) -> list:
    """The reasons an ExecuteTransaction is cancelled with, after checking the
    message lists their codes in order."""
    with pytest.raises(ClientError) as excinfo:
        client.execute_transaction(TransactStatements=statements)
    response = excinfo.value.response
    reasons = response["CancellationReasons"]
    codes = ", ".join(reason["Code"] for reason in reasons)
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [{codes}]",
    }
    return reasons


class TestPartiqlInsert:
    def insert(self, client, table: str, key: str, levels: int):
        return client.execute_statement(
            Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': ?, 'x': ?}}",
            Parameters=[{"S": key}, deep(levels)],
        )

    @pytest.mark.parametrize("levels", [30, 31])
    def test_a_value_the_item_holds_is_stored(self, dynamodb_client, table, levels):
        key = f"insert-{levels}-{uuid.uuid4().hex[:8]}"
        self.insert(dynamodb_client, table, key, levels)
        assert read_x(dynamodb_client, table, key) == deep(levels)

    def test_32_levels_meet_the_item_check(self, dynamodb_client, table):
        key = f"insert-32-{uuid.uuid4().hex[:8]}"
        assert (
            validation_message(lambda: self.insert(dynamodb_client, table, key, 32))
            == ITEM_CHECK
        )

    def test_33_levels_meet_the_parameter_check(self, dynamodb_client, table):
        key = f"insert-33-{uuid.uuid4().hex[:8]}"
        assert (
            validation_message(lambda: self.insert(dynamodb_client, table, key, 33))
            == NESTED
        )


class TestPartiqlUpdate:
    def set_x(self, client, table: str, key: str, levels: int):
        return client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET x = ? WHERE pk = '{key}'",
            Parameters=[deep(levels)],
        )

    def set_y_z(self, client, table: str, key: str, levels: int):
        return client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET y.z = ? WHERE pk = '{key}'",
            Parameters=[deep(levels)],
        )

    @pytest.mark.parametrize("levels", [30, 31])
    def test_a_value_the_item_holds_is_applied(self, dynamodb_client, table, stored, levels):
        self.set_x(dynamodb_client, table, stored, levels)
        assert read_x(dynamodb_client, table, stored) == deep(levels)

    def test_32_levels_meet_the_item_check(self, dynamodb_client, table, stored):
        assert (
            validation_message(lambda: self.set_x(dynamodb_client, table, stored, 32))
            == ITEM_CHECK
        )
        assert read_x(dynamodb_client, table, stored) is None

    def test_33_levels_meet_the_parameter_check(self, dynamodb_client, table, stored):
        assert (
            validation_message(lambda: self.set_x(dynamodb_client, table, stored, 33))
            == NESTED
        )

    @pytest.mark.parametrize("levels", [30, 31, 32])
    def test_a_path_the_item_lacks_answers_ahead_of_the_item_check(
        self, dynamodb_client, table, stored, levels
    ):
        assert (
            validation_message(lambda: self.set_y_z(dynamodb_client, table, stored, levels))
            == BAD_PATH
        )

    def test_a_path_the_item_lacks_meets_the_parameter_check_at_33_levels(
        self, dynamodb_client, table, stored
    ):
        assert (
            validation_message(lambda: self.set_y_z(dynamodb_client, table, stored, 33))
            == NESTED
        )

    @pytest.mark.parametrize("levels", [31, 32])
    def test_a_missing_item_fails_its_condition_ahead_of_the_item_check(
        self, dynamodb_client, table, levels
    ):
        key = f"absent-{uuid.uuid4().hex[:8]}"
        err = error(lambda: self.set_x(dynamodb_client, table, key, levels))
        assert err == {"Code": "ConditionalCheckFailedException", "Message": CONDITION_FAILED}

    def test_a_missing_item_meets_the_parameter_check_at_33_levels(self, dynamodb_client, table):
        key = f"absent-{uuid.uuid4().hex[:8]}"
        assert (
            validation_message(lambda: self.set_x(dynamodb_client, table, key, 33))
            == NESTED
        )

    @pytest.mark.parametrize("levels", [31, 32])
    def test_a_set_of_a_key_attribute_is_refused_at_any_depth(
        self, dynamodb_client, table, stored, levels
    ):
        statement = f"UPDATE \"{table}\" SET pk = ? WHERE pk = '{stored}'"
        assert (
            validation_message(
                lambda: dynamodb_client.execute_statement(
                    Statement=statement, Parameters=[deep(levels)]
                )
            )
            == KEY_UPDATE
        )

    @UNSTORED_DEEP_VALUE
    def test_an_if_not_exists_value_the_item_does_not_take_is_accepted(
        self, dynamodb_client, table, stored
    ):
        dynamodb_client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET a = if_not_exists(a, ?) WHERE pk = '{stored}'",
            Parameters=[deep(32)],
        )
        assert stored_item(dynamodb_client, table, stored) == {
            "pk": {"S": stored},
            "a": {"N": "1"},
        }

    @UNSTORED_DEEP_VALUE
    def test_a_deep_value_only_in_the_where_clause_is_accepted(
        self, dynamodb_client, table, stored
    ):
        dynamodb_client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET y = 1 WHERE pk = '{stored}' AND a <> ?",
            Parameters=[deep(32)],
        )
        assert stored_item(dynamodb_client, table, stored)["y"] == {"N": "1"}


class TestPartiqlUpdateThroughADeepPath:
    """A SET of a shallow parameter at a path 30 maps deep, on an item that holds
    ``x`` and 30 maps in ``m``: in ExecuteStatement, as a BatchExecuteStatement
    member and as an ExecuteTransaction member, and beside a SET of a
    409,500-byte parameter (measured 2026-11-10)."""

    @staticmethod
    def deep_set(table: str, key: str, levels: int) -> dict:
        """The UPDATE that sets ``levels`` maps at 30 keys below ``m``."""
        return {
            "Statement": f"UPDATE \"{table}\" SET m" + ".n" * 30 + " = ? WHERE pk = ?",
            "Parameters": [deep(levels), {"S": key}],
        }

    @staticmethod
    def x_set(table: str, key: str) -> dict:
        return {
            "Statement": f"UPDATE \"{table}\" SET x = ? WHERE pk = ?",
            "Parameters": [{"S": "y"}, {"S": key}],
        }

    @staticmethod
    def fresh(client, table: str) -> str:
        """The key of a fresh item that holds ``x`` and 30 maps in ``m``."""
        key = f"deep-path-{uuid.uuid4().hex[:8]}"
        client.put_item(
            TableName=table, Item={"pk": {"S": key}, "x": {"S": "x"}, "m": deep(30)}
        )
        return key

    @staticmethod
    def unchanged(key: str) -> dict:
        return {"pk": {"S": key}, "x": {"S": "x"}, "m": deep(30)}

    def set_leaf(self, client, table: str, key: str, levels: int):
        return client.execute_statement(**self.deep_set(table, key, levels))

    def set_big_beside(self, client, table: str, key: str, depth: int, big_first: bool):
        """SET a 409,500-byte ``big`` and SET 2 maps at ``depth`` keys below ``m``,
        as two SET clauses in the order ``big_first`` gives."""
        clauses = ["big = ?", "m" + ".n" * depth + " = ?"]
        params = [{"S": "a" * 409_500}, deep(2)]
        if not big_first:
            clauses, params = clauses[::-1], params[::-1]
        return client.execute_statement(
            Statement=f"UPDATE \"{table}\" SET {clauses[0]} SET {clauses[1]} WHERE pk = ?",
            Parameters=[*params, {"S": key}],
        )

    @pytest.fixture
    def deep_item(self, dynamodb_client, table) -> str:
        return self.fresh(dynamodb_client, table)

    def test_a_value_that_brings_the_item_to_the_limit_is_applied(
        self, dynamodb_client, table, deep_item
    ):
        assert self.set_leaf(dynamodb_client, table, deep_item, 1)["Items"] == []
        assert stored_item(dynamodb_client, table, deep_item)["m"] == deep(31)

    def test_a_value_that_takes_the_item_past_the_limit_meets_the_item_check(
        self, dynamodb_client, table, deep_item
    ):
        assert (
            validation_message(lambda: self.set_leaf(dynamodb_client, table, deep_item, 2))
            == ITEM_CHECK
        )
        assert stored_item(dynamodb_client, table, deep_item)["m"] == deep(30)

    def test_a_batch_member_past_the_limit_fails_only_that_statement(
        self, dynamodb_client, table, deep_item
    ):
        other = self.fresh(dynamodb_client, table)
        resp = dynamodb_client.batch_execute_statement(
            Statements=[self.x_set(table, other), self.deep_set(table, deep_item, 2)]
        )
        assert [entry.get("Error") for entry in resp["Responses"]] == [
            None,
            {"Code": "ValidationError", "Message": ITEM_CHECK},
        ]
        assert stored_item(dynamodb_client, table, other)["x"] == {"S": "y"}
        assert stored_item(dynamodb_client, table, deep_item) == self.unchanged(deep_item)

    def test_a_transaction_member_past_the_limit_cancels_the_transaction(
        self, dynamodb_client, table, deep_item
    ):
        other = self.fresh(dynamodb_client, table)
        statements = [self.x_set(table, other), self.deep_set(table, deep_item, 2)]
        assert cancellation(dynamodb_client, statements) == [{"Code": "None"}, ITEM_REASON]
        assert stored_item(dynamodb_client, table, other) == self.unchanged(other)
        assert stored_item(dynamodb_client, table, deep_item) == self.unchanged(deep_item)

    @pytest.mark.parametrize("big_first", [True, False], ids=["big_first", "deep_first"])
    def test_a_large_parameter_beside_the_deep_set_meets_the_item_check(
        self, dynamodb_client, table, deep_item, big_first
    ):
        assert (
            validation_message(
                lambda: self.set_big_beside(dynamodb_client, table, deep_item, 30, big_first)
            )
            == ITEM_CHECK
        )
        assert stored_item(dynamodb_client, table, deep_item) == self.unchanged(deep_item)

    @refused_for_size_on_default_backends_only
    def test_the_large_parameter_beside_a_set_at_28_keys_is_refused_for_its_size(
        self, dynamodb_client, table, deep_item
    ):
        # The control for the test above: m keeps 30 maps, so only the size of
        # the updated item is past a limit.
        assert (
            validation_message(
                lambda: self.set_big_beside(dynamodb_client, table, deep_item, 28, True)
            )
            == SIZE_TO_UPDATE
        )
        assert stored_item(dynamodb_client, table, deep_item) == self.unchanged(deep_item)


class TestPartiqlDeleteAndSelect:
    """A 32-level value in the WHERE clause of a DELETE or a SELECT."""

    @UNSTORED_DEEP_VALUE
    def test_a_delete_with_a_deep_value_in_its_where_clause_is_accepted(
        self, dynamodb_client, table, stored
    ):
        dynamodb_client.execute_statement(
            Statement=f"DELETE FROM \"{table}\" WHERE pk = '{stored}' AND a <> ?",
            Parameters=[deep(32)],
        )
        assert stored_item(dynamodb_client, table, stored) is None

    def test_a_select_compares_a_deep_value_like_any_other(
        self, dynamodb_client, table, stored
    ):
        items = dynamodb_client.execute_statement(
            Statement=f"SELECT * FROM \"{table}\" WHERE pk = '{stored}' AND a <> ?",
            Parameters=[deep(32)],
            ConsistentRead=True,
        )["Items"]
        assert items == [{"pk": {"S": stored}, "a": {"N": "1"}}]


class TestPartiqlBatch:
    """The item check on a member statement of a BatchExecuteStatement."""

    def test_an_insert_of_32_levels_fails_only_that_statement(self, dynamodb_client, table):
        good = f"batch-good-{uuid.uuid4().hex[:8]}"
        bad = f"batch-bad-{uuid.uuid4().hex[:8]}"
        statement = f"INSERT INTO \"{table}\" VALUE {{'pk': ?, 'x': ?}}"
        resp = dynamodb_client.batch_execute_statement(
            Statements=[
                {"Statement": statement, "Parameters": [{"S": good}, deep(31)]},
                {"Statement": statement, "Parameters": [{"S": bad}, deep(32)]},
            ]
        )
        assert [entry.get("Error") for entry in resp["Responses"]] == [
            None,
            {"Code": "ValidationError", "Message": ITEM_CHECK},
        ]
        assert read_x(dynamodb_client, table, good) == deep(31)

    def test_an_update_of_32_levels_fails_only_that_statement(
        self, dynamodb_client, table, stored
    ):
        resp = dynamodb_client.batch_execute_statement(
            Statements=[
                {
                    "Statement": f"UPDATE \"{table}\" SET x = ? WHERE pk = '{stored}'",
                    "Parameters": [deep(32)],
                }
            ]
        )
        assert [entry.get("Error") for entry in resp["Responses"]] == [
            {"Code": "ValidationError", "Message": ITEM_CHECK}
        ]


class TestPartiqlTransaction:
    """A 32-level value in an ExecuteTransaction member statement cancels the
    transaction with the item check's reason at that member's position. An
    UPDATE of a missing item fails its condition first."""

    def insert(self, key: str, levels: int, table: str) -> dict:
        return {
            "Statement": f"INSERT INTO \"{table}\" VALUE {{'pk': ?, 'x': ?}}",
            "Parameters": [{"S": key}, deep(levels)],
        }

    def update(self, key: str, levels: int, table: str) -> dict:
        return {
            "Statement": f"UPDATE \"{table}\" SET x = ? WHERE pk = '{key}'",
            "Parameters": [deep(levels)],
        }

    def test_an_insert_of_32_levels_cancels_the_transaction(self, dynamodb_client, table):
        key = f"txn-insert-32-{uuid.uuid4().hex[:8]}"
        statements = [self.insert(key, 32, table)]
        assert cancellation(dynamodb_client, statements) == [ITEM_REASON]
        assert stored_item(dynamodb_client, table, key) is None

    def test_an_update_of_32_levels_cancels_the_transaction(self, dynamodb_client, table, stored):
        statements = [self.update(stored, 32, table)]
        assert cancellation(dynamodb_client, statements) == [ITEM_REASON]
        assert read_x(dynamodb_client, table, stored) is None

    def test_only_the_member_past_the_limit_has_a_reason(self, dynamodb_client, table):
        fits = f"txn-insert-31-{uuid.uuid4().hex[:8]}"
        past = f"txn-insert-32-{uuid.uuid4().hex[:8]}"
        statements = [self.insert(fits, 31, table), self.insert(past, 32, table)]
        assert cancellation(dynamodb_client, statements) == [{"Code": "None"}, ITEM_REASON]
        assert stored_item(dynamodb_client, table, fits) is None

    def test_an_update_of_a_missing_item_fails_its_condition_first(self, dynamodb_client, table):
        key = f"txn-absent-{uuid.uuid4().hex[:8]}"
        statements = [self.update(key, 32, table)]
        assert cancellation(dynamodb_client, statements) == [
            {"Code": "ConditionalCheckFailed", "Message": CONDITION_FAILED}
        ]

    def test_another_member_reports_its_failed_condition_beside_the_item_check(
        self, dynamodb_client, table, stored
    ):
        key = f"txn-insert-32-{uuid.uuid4().hex[:8]}"
        statements = [
            {"Statement": f"UPDATE \"{table}\" SET a = 2 WHERE pk = '{stored}' AND a = 99"},
            self.insert(key, 32, table),
        ]
        assert cancellation(dynamodb_client, statements) == [FAILED_REASON, ITEM_REASON]
        assert stored_item(dynamodb_client, table, key) is None

    def test_every_member_reports_its_own_failed_condition(self, dynamodb_client, table, stored):
        absent = f"txn-absent-{uuid.uuid4().hex[:8]}"
        statements = [
            self.update(absent, 32, table),
            {"Statement": f"UPDATE \"{table}\" SET a = 2 WHERE pk = '{stored}' AND a = 99"},
        ]
        assert cancellation(dynamodb_client, statements) == [FAILED_REASON, FAILED_REASON]

    def test_two_members_on_one_item_are_refused_ahead_of_the_item_check(
        self, dynamodb_client, table, stored
    ):
        statements = [
            self.update(stored, 32, table),
            {"Statement": f"UPDATE \"{table}\" SET a = 3 WHERE pk = '{stored}'"},
        ]
        err = error(lambda: dynamodb_client.execute_transaction(TransactStatements=statements))
        assert err == {
            "Code": "ValidationException",
            "Message": "Transaction request cannot include multiple operations on one item",
        }
        assert stored_item(dynamodb_client, table, stored) == {
            "pk": {"S": stored},
            "a": {"N": "1"},
        }

    def test_a_missing_table_is_reported_ahead_of_the_item_check(self, dynamodb_client, table):
        key = f"txn-insert-32-{uuid.uuid4().hex[:8]}"
        statements = [
            {"Statement": f"UPDATE \"{table}-missing\" SET a = 1 WHERE pk = 'q'"},
            self.insert(key, 32, table),
        ]
        assert cancellation(dynamodb_client, statements) == [
            {"Code": "ResourceNotFound", "Message": "Requested resource not found"},
            {"Code": "None"},
        ]


class TestClassicWrites:
    """PutItem and UpdateItem refuse a 32-level value as part of the request.
    The service puts "1 validation error detected: " in front of the sentence
    these tests compare (measured 2026-11-02)."""

    def put(self, client, table: str, levels: int):
        key = f"put-{levels}-{uuid.uuid4().hex[:8]}"
        client.put_item(TableName=table, Item={"pk": {"S": key}, "x": deep(levels)})
        return key

    def update(self, client, table: str, key: str, levels: int):
        client.update_item(
            TableName=table,
            Key={"pk": {"S": key}},
            UpdateExpression="SET x = :v",
            ExpressionAttributeValues={":v": deep(levels)},
        )

    @pytest.mark.parametrize("levels", [30, 31])
    def test_put_item_stores_a_value_the_item_holds(self, dynamodb_client, table, levels):
        key = self.put(dynamodb_client, table, levels)
        assert read_x(dynamodb_client, table, key) == deep(levels)

    @pytest.mark.parametrize("levels", [32, 33])
    def test_put_item_refuses_32_levels_with_the_long_message(
        self, dynamodb_client, table, levels
    ):
        message = validation_message(lambda: self.put(dynamodb_client, table, levels))
        assert message.endswith(NESTED), message

    @pytest.mark.parametrize("levels", [30, 31])
    def test_update_item_applies_a_value_the_item_holds(
        self, dynamodb_client, table, stored, levels
    ):
        self.update(dynamodb_client, table, stored, levels)
        assert read_x(dynamodb_client, table, stored) == deep(levels)

    @pytest.mark.parametrize("levels", [32, 33])
    def test_update_item_refuses_32_levels_with_the_long_message(
        self, dynamodb_client, table, stored, levels
    ):
        message = validation_message(
            lambda: self.update(dynamodb_client, table, stored, levels)
        )
        assert message.endswith(NESTED), message
