# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for set_add and set_delete in a PartiQL UPDATE: what
a RETURNING clause gives back, and how a number literal in the bag is checked.

RETURNING MODIFIED NEW * and RETURNING MODIFIED OLD * give the attribute a
set_add or set_delete names, beside any plain SET target, as it is after or
before the update. An attribute the update empties, or one the item does not
have yet, gives no row. ALL NEW * and ALL OLD * give the whole item. A number
literal in the bag is checked like any literal value: a rule's message ends
with "under root", and a literal that cannot be converted is a parse error with
nothing after the colon.

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS credentials) and
against an extenddb server (EXTENDDB_TEST_ENDPOINT set) with identical
assertions. Every expected value was measured on the service (2026-11-02). The
service returns the members of a set in no fixed order, so sets compare as
sets. Tables use the ``extenddb-measure-pqs-`` prefix and are deleted at the
end.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

NOT_WELL_FORMED = "Statement wasn't well formed, can't be processed: "
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than "
    "supported range"
)
UNDERFLOW = (
    "Number underflow. Attempting to store a number with magnitude smaller than "
    "supported range"
)
DIGITS = "Attempting to store more than 38 significant digits in a Number"
DIGITS_39 = "123456789012345678901234567890123456789"

SET_ADD = "SET ss = set_add(ss, <<'b'>>)"
SET_DELETE = "SET ns = set_delete(ns, <<1>>)"


@pytest.fixture(scope="module")
def table(dynamodb_client):
    name = f"extenddb-measure-pqs-{uuid.uuid4().hex[:12]}"
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


def seeded(key: str) -> dict:
    """The item every test starts from."""
    return {
        "pk": {"S": key},
        "ss": {"SS": ["a"]},
        "ns": {"NS": ["1", "2"]},
        "other": {"N": "5"},
    }


@pytest.fixture
def key(dynamodb_client, table) -> str:
    """The key of a fresh copy of the seeded item."""
    key = f"s-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=table, Item=seeded(key))
    return key


def as_sets(item: dict) -> dict:
    """``item`` with each string set and number set held as a Python set."""
    out = {}
    for name, value in item.items():
        if "SS" in value:
            out[name] = ("SS", frozenset(value["SS"]))
        elif "NS" in value:
            out[name] = ("NS", frozenset(Decimal(n) for n in value["NS"]))
        else:
            out[name] = value
    return out


def update(client, table: str, key: str, assignment: str, returning: str) -> list:
    statement = f"UPDATE \"{table}\" {assignment} WHERE pk = '{key}' RETURNING {returning} *"
    return client.execute_statement(Statement=statement)["Items"]


@pytest.mark.parametrize(
    ("assignment", "returning", "expected"),
    [
        (SET_ADD, "MODIFIED NEW", [{"ss": {"SS": ["a", "b"]}}]),
        (SET_ADD, "MODIFIED OLD", [{"ss": {"SS": ["a"]}}]),
        (SET_DELETE, "MODIFIED NEW", [{"ns": {"NS": ["2"]}}]),
        (SET_DELETE, "MODIFIED OLD", [{"ns": {"NS": ["1", "2"]}}]),
        (
            f"{SET_ADD} SET other = 6",
            "MODIFIED NEW",
            [{"other": {"N": "6"}, "ss": {"SS": ["a", "b"]}}],
        ),
        ("SET ss = set_delete(ss, <<'a'>>)", "MODIFIED NEW", []),
        ("SET ss = set_delete(ss, <<'a'>>)", "MODIFIED OLD", [{"ss": {"SS": ["a"]}}]),
        ("SET zz = set_add(zz, <<'q'>>)", "MODIFIED OLD", []),
    ],
    ids=[
        "add-new",
        "add-old",
        "delete-new",
        "delete-old",
        "add-and-set-new",
        "emptied-new",
        "emptied-old",
        "absent-old",
    ],
)
def test_modified_gives_the_set_the_function_names(
    dynamodb_client, table, key, assignment, returning, expected
):
    items = update(dynamodb_client, table, key, assignment, returning)
    assert [as_sets(item) for item in items] == [as_sets(item) for item in expected]


@pytest.mark.parametrize(
    ("assignment", "returning", "changed"),
    [
        (SET_ADD, "ALL NEW", {"ss": {"SS": ["a", "b"]}}),
        (SET_DELETE, "ALL NEW", {"ns": {"NS": ["2"]}}),
        (SET_ADD, "ALL OLD", {}),
        (SET_DELETE, "ALL OLD", {}),
    ],
    ids=["add-new", "delete-new", "add-old", "delete-old"],
)
def test_all_gives_the_whole_item(dynamodb_client, table, key, assignment, returning, changed):
    items = update(dynamodb_client, table, key, assignment, returning)
    assert [as_sets(item) for item in items] == [as_sets({**seeded(key), **changed})]


@pytest.mark.parametrize("function", ["set_add", "set_delete"])
@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        (DIGITS_39, f"{DIGITS} under root"),
        ("1e126", f"{OVERFLOW} under root"),
        ("1e-131", f"{UNDERFLOW} under root"),
        ("1e99999999999", NOT_WELL_FORMED),
    ],
    ids=["digits", "overflow", "underflow", "unconvertible"],
)
def test_a_number_literal_in_the_bag_is_checked(
    dynamodb_client, table, key, function, literal, expected
):
    statement = f"UPDATE \"{table}\" SET ns = {function}(ns, <<{literal}>>) WHERE pk = '{key}'"
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client.execute_statement(Statement=statement)
    err = excinfo.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", expected)
    item = dynamodb_client.get_item(TableName=table, Key={"pk": {"S": key}}, ConsistentRead=True)
    assert as_sets(item["Item"]) == as_sets(seeded(key))
