# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for a map literal that names a key twice.

The statement is refused with "Duplicate keys in tuple. Key: <name> under
<path> at <line>:<column>:<length>", the path being where the map sits in the
value that holds it ("root", "root[0]", "root.y"). At the top level of an
INSERT VALUE the position is the second occurrence of the name, its length
the quoted name's; in any other map (one nested in a value, a map in a list,
a SET value, a WHERE literal) it is the map's opening `{`, length 1. Names
differing only in case are different names, and a syntax error later in the
statement wins. BatchExecuteStatement gives the refusal as the statement's
ValidationError entry, ExecuteTransaction names the statement.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (2026-11-03).
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WF = "Statement wasn't well formed, can't be processed: "


@pytest.fixture(scope="module")
def table(dynamodb_client):
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


def _message(client, statement: str) -> str:
    with pytest.raises(ClientError) as e:
        client.execute_statement(Statement=statement)
    err = e.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    return err["Message"]


def _refusal(name: str, path: str, statement: str, at: str, length: int) -> str:
    """The refusal of `name` given twice in the map at `path`, at the column
    where `at` starts in `statement`."""
    column = statement.index(at) + 1
    return f"Duplicate keys in tuple. Key: {name} under {path} at 1:{column}:{length}"


def _second(statement: str, quoted: str) -> int:
    """The index of the second occurrence of `quoted` in `statement`."""
    return statement.index(quoted, statement.index(quoted) + 1)


@pytest.mark.parametrize(
    ("value", "name"),
    [
        ("{'pk': 'd1', 'pk': 'd2', 'sk': 's'}", "pk"),
        ("{'pk': 'd3', 'sk': 's', 'v': '1', 'v': '2'}", "v"),
        ("{'pk': 'd4', 'sk': 's1', 'sk': 's2'}", "sk"),
    ],
)
def test_a_name_given_twice_at_the_top_of_a_value_is_refused_at_its_second_occurrence(
    dynamodb_client, table, value, name
):
    statement = f'INSERT INTO "{table}" VALUE {value}'
    quoted = f"'{name}'"
    column = _second(statement, quoted) + 1
    assert _message(dynamodb_client, statement) == (
        f"Duplicate keys in tuple. Key: {name} under root at 1:{column}:{len(quoted)}"
    )


@pytest.mark.parametrize(
    ("template", "path"),
    [
        ("INSERT INTO {t} VALUE {{'pk': 'n1', 'sk': 's', 'm': {{'x': 1, 'x': 2}}}}", "root"),
        ("INSERT INTO {t} VALUE {{'pk': 'n2', 'sk': 's', 'l': [{{'x': 1, 'x': 2}}]}}", "root[0]"),
        (
            "INSERT INTO {t} VALUE {{'pk': 'n4', 'sk': 's', 'm': {{'y': {{'x': 1, 'x': 2}}}}}}",
            "root.y",
        ),
        ("UPDATE {t} SET m = {{'x': 1, 'x': 2}} WHERE pk = 'a' AND sk = 'x'", "root"),
        ("SELECT * FROM {t} WHERE pk = 'a' AND m = {{'x': 1, 'x': 2}}", "root"),
    ],
    ids=["nested-map", "map-in-a-list", "two-levels-down", "set-value", "where-literal"],
)
def test_a_name_given_twice_in_any_other_map_is_refused_at_its_opening_brace(
    dynamodb_client, table, template, path
):
    statement = template.format(t=f'"{table}"')
    assert _message(dynamodb_client, statement) == _refusal("x", path, statement, "{'x'", 1)


def test_names_that_differ_in_case_are_different_names(dynamodb_client, table):
    statement = f"INSERT INTO \"{table}\" VALUE {{'pk': 'n3', 'sk': 's', 'v': 1, 'V': 2}}"
    assert dynamodb_client.execute_statement(Statement=statement)["Items"] == []
    item = dynamodb_client.get_item(
        TableName=table, Key={"pk": {"S": "n3"}, "sk": {"S": "s"}}, ConsistentRead=True
    )["Item"]
    assert item["v"] == {"N": "1"} and item["V"] == {"N": "2"}


def test_a_syntax_error_after_the_value_wins(dynamodb_client, table):
    statement = f"INSERT INTO \"{table}\" VALUE {{'pk': 'n7', 'pk': 'n8', 'sk': 's'}} WHERE"
    assert _message(dynamodb_client, statement) == WF + "Unexpected token after expression"


def test_a_batch_and_a_transaction_give_the_refusal_of_their_statement(dynamodb_client, table):
    statement = f"INSERT INTO \"{table}\" VALUE {{'pk': 'n5', 'pk': 'n6', 'sk': 's'}}"
    refusal = _refusal("pk", "root", statement, "'pk': 'n6'", 4)
    responses = dynamodb_client.batch_execute_statement(
        Statements=[{"Statement": statement}]
    )["Responses"]
    assert responses == [{"Error": {"Code": "ValidationError", "Message": refusal}}]
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_transaction(TransactStatements=[{"Statement": statement}])
    assert e.value.response["Error"] == {
        "Code": "ValidationException",
        "Message": f"Validation failed in TransactStatements[0]: {refusal}",
    }
    for pk in ("n5", "n6"):
        found = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": pk}, "sk": {"S": "s"}}, ConsistentRead=True
        )
        assert "Item" not in found, pk
