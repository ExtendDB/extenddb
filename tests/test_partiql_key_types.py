# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for a key value of the wrong type in a PartiQL read.

A key attribute compared with a value of a type the table does not define for
it is refused with the key type message wherever the comparison could be a key
condition: an equality in an OR branch, an IN on the partition key, a sort-key
range. A begins_with key condition whose prefix is a number gets the
operand-type message a filter gets. BatchExecuteStatement carries the key type
message as the statement's error, and a read in ExecuteTransaction cancels the
transaction with a reason saying the key does not match the schema.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-02).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

KEY_TYPE = "Key attribute's data type should match its data type in table's schema: Key {}"
BEGINS_WITH_NUMBER = (
    "Incorrect operand type for operator or function; operator or function: begins_with, "
    "operand type: N"
)
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
KEY_SCHEMA_MISMATCH = {
    "Code": "ValidationError",
    "Message": "The provided key element does not match the schema",
}


@pytest.fixture(scope="module")
def stable(dynamodb_client):
    """A table keyed on a string pk."""
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def ctable(dynamodb_client):
    """A table keyed on a string pk and a string sk."""
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


def _message(client, statement):
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _batch_errors(client, statement):
    resp = client.batch_execute_statement(Statements=[{"Statement": statement}])
    return [entry.get("Error") for entry in resp["Responses"]]


def test_an_in_list_of_the_wrong_type_on_the_partition_key(dynamodb_client, stable):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk IN [1, 2]"
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("pk")


@pytest.mark.parametrize(
    "condition", ["sk > 1", "sk BETWEEN 1 AND 2"], ids=["greater", "between"]
)
def test_a_number_in_a_string_sort_key_range(dynamodb_client, ctable, condition):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'ov' AND {condition}"
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("sk")


def test_a_number_prefix_in_a_begins_with_key_condition(dynamodb_client, ctable):
    statement = f"SELECT * FROM \"{ctable}\" WHERE pk = 'ov' AND begins_with(sk, 1)"
    assert _message(dynamodb_client, statement) == BEGINS_WITH_NUMBER


@pytest.mark.parametrize("number", ["1", "1.e3"])
def test_a_single_read_with_a_key_of_the_wrong_type(dynamodb_client, stable, number):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = {number}"
    assert _message(dynamodb_client, statement) == KEY_TYPE.format("pk")


@pytest.mark.parametrize("number", ["1", "1.e3"])
def test_a_batch_read_with_a_key_of_the_wrong_type(dynamodb_client, stable, number):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = {number}"
    assert _batch_errors(dynamodb_client, statement) == [
        {"Code": "ValidationError", "Message": KEY_TYPE.format("pk")}
    ]


def test_a_batch_write_with_a_key_of_the_wrong_type(dynamodb_client, stable):
    statement = f"UPDATE \"{stable}\" SET a = 1 WHERE pk = 1"
    assert _batch_errors(dynamodb_client, statement) == [
        {"Code": "ValidationError", "Message": KEY_TYPE.format("pk")}
    ]


@pytest.mark.parametrize("number", ["1", "1.e3"])
def test_a_transaction_read_with_a_key_of_the_wrong_type(dynamodb_client, stable, number):
    statement = f"SELECT * FROM \"{stable}\" WHERE pk = {number}"
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    response = e.value.response
    assert response["Error"] == {
        "Code": "TransactionCanceledException",
        "Message": f"{CANCELLED} [ValidationError]",
    }
    assert response["CancellationReasons"] == [KEY_SCHEMA_MISMATCH]
