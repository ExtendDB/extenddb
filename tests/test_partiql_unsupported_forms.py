# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the PartiQL statements and expressions the
service reads to their end and then refuses by their form: a statement that
is a call, in ExecuteStatement, BatchExecuteStatement and ExecuteTransaction;
a nested DELETE in a call; a nested SELECT in a condition and in a SET value;
a bag member that is neither a number nor a string; and set_add or set_delete
with an arithmetic operator after it.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected text was measured on the
service (2026-11-13 and 2026-11-14).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "
CHECK = "EXISTS can only be used in ExecuteTransaction write requests."
IN_TRANSACTION = "Validation failed in TransactStatements[0]: Unsupported operation in transaction."
BAG_MEMBER_TYPES = (
    "Unsupported data type in Bag. DynamoDB only supports either numbers or strings in bags"
)


@pytest.fixture(scope="module")
def table(dynamodb_client):
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


def _refusal(call):
    with pytest.raises(ClientError) as excinfo:
        call()
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException", err["Message"]
    return err["Message"]


def _message(client, statement):
    return _refusal(lambda: client.execute_statement(Statement=statement))


def _at(statement, text):
    """The `line:column:length` of the first `text` in a one-line statement."""
    return f"1:{statement.index(text) + 1}:{len(text)}"


# ---- a statement that is a call ----------------------------------------------

CALLS = [
    "foo(1)",
    "foo()",
    "FOO (1)",
    "foo(?)",
    "foo(<<>>)",
    "foo(1e999999)",
    "foo(foo(1), a.b[0])",
    "size(1)",
    "attribute_exists(a)",
    "upper('a')",
    "EXISTS()",
    "EXISTS(1, 2)",
    "exists(foo(1))",
]


@pytest.mark.parametrize("statement", CALLS)
def test_a_call_is_a_condition_check_outside_a_transaction(dynamodb_client, statement):
    assert _message(dynamodb_client, statement) == CHECK
    resp = dynamodb_client.batch_execute_statement(Statements=[{"Statement": statement}])
    error = resp["Responses"][0]["Error"]
    assert (error["Code"], error["Message"]) == ("ValidationError", CHECK)


@pytest.mark.parametrize("statement", CALLS + ['EXISTS(SELECT * FROM "{t}" WHERE pk = 1, 1)'])
def test_a_call_is_an_unsupported_operation_in_a_transaction(dynamodb_client, table, statement):
    statement = statement.format(t=table)
    assert _refusal(
        lambda: dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    ) == IN_TRANSACTION


# ---- a nested DELETE or SELECT -----------------------------------------------


def test_a_nested_delete_is_refused_for_its_place(dynamodb_client, table):
    statement = f'foo(DELETE FROM "{table}" WHERE pk = 1)'
    assert _message(dynamodb_client, statement) == (
        f"{WF}Keyword delete only expected at the top level in the query"
    )


@pytest.mark.parametrize(
    "template",
    [
        'SELECT * FROM "{t}" WHERE pk = (SELECT * FROM "{t}")',
        'SELECT * FROM "{t}" WHERE a IN [SELECT a FROM "{t}"]',
        "UPDATE \"{t}\" SET a = (SELECT * FROM \"{t}\") + 1 WHERE pk = 'a'",
    ],
)
def test_a_nested_select_is_refused_at_its_select(dynamodb_client, table, template):
    statement = template.format(t=table)
    column = statement.rindex("SELECT") + 1
    assert _message(dynamodb_client, statement) == (
        f"Unsupported token in expression: SELECT at 1:{column}:6"
    )


# ---- values ------------------------------------------------------------------


def test_a_bag_member_that_is_neither_a_number_nor_a_string_is_refused_at_its_position(
    dynamodb_client, ctable
):
    statement = f"INSERT INTO \"{ctable}\" VALUE {{'pk':'pq-gr-bag', 'sk':'1', 's':<<true>>}}"
    assert _message(dynamodb_client, statement) == f"{BAG_MEMBER_TYPES} at {_at(statement, 'true')}"


@pytest.mark.parametrize(
    ("value", "function"),
    [
        ("set_add(a) + 1", "set_add"),
        ("set_add(a, <<'b'>>) + 1", "set_add"),
        ("set_delete(a, <<'b'>>) - 1", "set_delete"),
        ("set_add(<<>>) + 1", None),
    ],
)
def test_a_set_function_with_arithmetic_after_it_is_a_call_the_value_does_not_take(
    dynamodb_client, table, value, function
):
    statement = f"UPDATE \"{table}\" SET a = {value} WHERE pk = 'pq-fn-msg'"
    expected = (
        f"Unrecognized function: {function} at {_at(statement, function)}"
        if function
        else "Empty bags are not supported"
    )
    assert _message(dynamodb_client, statement) == expected
