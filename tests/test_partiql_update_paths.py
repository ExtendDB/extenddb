# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the document paths a PartiQL UPDATE writes.

An UPDATE whose SET, REMOVE, set_add and set_delete targets name one path
twice, a path and a path inside it, or one element both as a map member and
as a list element, is refused before anything is written, with or without
RETURNING. set_add and set_delete apply through a map path and through a list
element.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected value was measured on the service (2026-11-02). The
service returns the members of a set in no fixed order, so sets compare as
sets. Tables use the ``extenddb-measure-pqp-`` prefix and are deleted at the
end.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

OVERLAP = (
    "Two document paths overlap with each other; must remove or rewrite one of these "
    "paths; path one: {}, path two: {}"
)
CONFLICT = (
    "Two document paths conflict with each other; must remove or rewrite one of these "
    "paths; path one: {}, path two: {}"
)

# ExtendDB translates set_add and set_delete into the ADD and DELETE update
# actions, which it applies only to an attribute in a map. The service applies
# set_add to a list element (measured 2026-11-02).
SET_FUNCTION_ON_A_LIST_ELEMENT = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()),
    reason="ExtendDB applies ADD and DELETE only to an attribute in a map",
    raises=ClientError,
    strict=True,
)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    name = f"extenddb-measure-pqp-{uuid.uuid4().hex[:12]}"
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


def stored_item(client, table: str, key: str) -> dict | None:
    return client.get_item(TableName=table, Key={"pk": {"S": key}}, ConsistentRead=True).get(
        "Item"
    )


def as_sets(value):
    """``value`` with every string set held as a Python set, at any depth."""
    if isinstance(value, dict):
        if "SS" in value:
            return ("SS", frozenset(value["SS"]))
        return {name: as_sets(member) for name, member in value.items()}
    if isinstance(value, list):
        return [as_sets(member) for member in value]
    return value


@pytest.mark.parametrize(
    ("assignments", "one", "two"),
    [
        ("SET q = 1, q = 2", "[q]", "[q]"),
        ("SET q = 1 REMOVE q", "[q]", "[q]"),
        ("SET m = {'a': 1}, m.a = 2", "[m, a]", "[m]"),
        ("SET ss = set_add(ss, <<'b'>>), ss = set_add(ss, <<'c'>>)", "[ss]", "[ss]"),
        ("SET ss = 'plain', ss = set_add(ss, <<'b'>>)", "[ss]", "[ss]"),
        ("SET q = 1, q = 2 RETURNING MODIFIED NEW *", "[q]", "[q]"),
    ],
    ids=[
        "set-twice",
        "set-then-remove",
        "parent-and-child",
        "set-add-twice",
        "set-beside-set-add",
        "returning-modified",
    ],
)
def test_overlapping_paths_are_refused_before_anything_is_written(
    dynamodb_client, table, assignments, one, two
):
    key = f"o-{uuid.uuid4().hex[:8]}"
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': '{key}', 'ss': <<'a'>>, 'm': {{'a': 0}}}}"
    )
    head, _, tail = assignments.partition(" RETURNING ")
    statement = f"UPDATE \"{table}\" {head} WHERE pk = '{key}'"
    if tail:
        statement += f" RETURNING {tail}"
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.execute_statement(Statement=statement)
    err = excinfo.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", OVERLAP.format(one, two))
    assert stored_item(dynamodb_client, table, key) == {
        "pk": {"S": key},
        "ss": {"SS": ["a"]},
        "m": {"M": {"a": {"N": "0"}}},
    }


@pytest.mark.parametrize(
    "stored",
    [{"M": {"b": {"N": "0"}}}, {"L": [{"N": "0"}]}, None, "missing"],
    ids=["map", "list", "absent", "missing-item"],
)
@pytest.mark.parametrize(
    ("assignments", "one", "two"),
    [
        ("SET a.b = 1, a[0] = 2", "[a, [0]]", "[a, b]"),
        ("SET a[0] = 2, a.b = 1", "[a, b]", "[a, [0]]"),
    ],
    ids=["member-then-element", "element-then-member"],
)
def test_one_element_named_as_a_member_and_an_element_conflicts(
    dynamodb_client, table, stored, assignments, one, two
):
    key = f"c-{uuid.uuid4().hex[:8]}"
    item = {"pk": {"S": key}}
    if isinstance(stored, dict):
        item["a"] = stored
    if stored != "missing":
        dynamodb_client.put_item(TableName=table, Item=item)
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.execute_statement(
            Statement=f"UPDATE \"{table}\" {assignments} WHERE pk = '{key}'"
        )
    err = excinfo.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", CONFLICT.format(one, two))
    assert stored_item(dynamodb_client, table, key) == (None if stored == "missing" else item)


def insert_nested(client, table: str) -> str:
    key = f"n-{uuid.uuid4().hex[:8]}"
    client.execute_statement(
        Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': '{key}', 'm': {{'ss': <<'a'>>}}, "
        "'l': [<<'a'>>]}"
    )
    return key


def update(client, table: str, key: str, assignment: str) -> list:
    statement = f"UPDATE \"{table}\" {assignment} WHERE pk = '{key}' RETURNING ALL NEW *"
    return client.execute_statement(Statement=statement)["Items"]


def test_set_add_and_set_delete_apply_through_a_map_path(dynamodb_client, table):
    key = insert_nested(dynamodb_client, table)
    added = update(dynamodb_client, table, key, "SET m.ss = set_add(m.ss, <<'b'>>)")
    assert as_sets(added) == as_sets(
        [{"pk": {"S": key}, "m": {"M": {"ss": {"SS": ["a", "b"]}}}, "l": {"L": [{"SS": ["a"]}]}}]
    )
    deleted = update(dynamodb_client, table, key, "SET m.ss = set_delete(m.ss, <<'a'>>)")
    assert as_sets(deleted) == as_sets(
        [{"pk": {"S": key}, "m": {"M": {"ss": {"SS": ["b"]}}}, "l": {"L": [{"SS": ["a"]}]}}]
    )


@SET_FUNCTION_ON_A_LIST_ELEMENT
def test_set_add_applies_to_a_list_element(dynamodb_client, table):
    key = insert_nested(dynamodb_client, table)
    added = update(dynamodb_client, table, key, "SET l[0] = set_add(l[0], <<'b'>>)")
    assert as_sets(added) == as_sets(
        [{"pk": {"S": key}, "m": {"M": {"ss": {"SS": ["a"]}}}, "l": {"L": [{"SS": ["a", "b"]}]}}]
    )
