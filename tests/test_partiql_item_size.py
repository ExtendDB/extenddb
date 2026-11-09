# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for the item size limit of a PartiQL UPDATE.

The service sizes a PartiQL UPDATE by its finished item alone, as it sizes a
transacted Update: the item may grow to exactly 409,600 bytes, and one byte
more is refused with "Item size to update has exceeded the maximum allowed
size". A standalone UpdateItem is sized by its statement instead, so at the
same key it refuses a finished item of 409,600 bytes. More SET clauses and
an if_not_exists add nothing to a PartiQL UPDATE. A WHERE term that fails, and
the implicit check that the item exists, are answered before the size: an
UPDATE over the limit that would not run gets ConditionalCheckFailedException.
BatchExecuteStatement gives the refusal as the statement's ValidationError
entry, and ExecuteTransaction cancels with a ValidationError reason.

Item size: each attribute's name bytes plus its value bytes. Every item here
holds string attributes only, at a three-byte key, so {pk, p} is
2 + 3 + 1 + len(p) bytes.

Dual-target: EXTENDDB_TEST_ENDPOINT selects an extenddb server; unset, the
tests run against the live service with the default credential chain. The
assertions are identical on both. Every expected value was measured on the
service (us-east-1, 2026-11-04).
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

LIMIT = 409_600
UPDATE_WORDING = "Item size to update has exceeded the maximum allowed size"

# The PostgreSQL and SQLite storage layers size updates the way the service
# does. The Cassandra layer sizes every update against a fixed 409,600 bytes
# with the put wording, and the MongoDB layer applies no limit to updates (see
# docs/differences-from-dynamodb.md), so the cases that need a refusal are
# skipped on those two backends.
refused_on_default_backends_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_BACKEND", "") in {"mongodb", "cassandra"},
    reason="updates are sized against a fixed limit (Cassandra) or not at all (MongoDB); "
    "see docs/differences-from-dynamodb.md",
)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on a string pk."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
    ) as name:
        yield name


def _p(key: str, finished: int, other: int = 0) -> str:
    """A value for p that makes {pk: key, p, ...} exactly `finished` bytes,
    where the other attributes take `other` bytes."""
    return "x" * (finished - (2 + len(key)) - 1 - other)


def _base(client, table: str, key: str, **attrs: str) -> None:
    item = {"pk": {"S": key}, "p": {"S": "a"}}
    item.update({name: {"S": value} for name, value in attrs.items()})
    client.put_item(TableName=table, Item=item)


def _stored(client, table: str, key: str) -> int | None:
    item = client.get_item(TableName=table, Key={"pk": {"S": key}}, ConsistentRead=True).get("Item")
    if item is None:
        return None
    return sum(len(name.encode()) + len(value["S"].encode()) for name, value in item.items())


def _update(client, table: str, key: str, value: str, sets: str = "p = ?", where: str = "",
            more: tuple = ()):
    return client.execute_statement(
        Statement=f'UPDATE "{table}" SET {sets} WHERE pk = ?{where}',
        Parameters=[{"S": value}, *more, {"S": key}],
    )


def _error(call) -> dict:
    with pytest.raises(ClientError) as e:
        call()
    return e.value.response["Error"]


def test_an_update_writes_a_finished_item_of_the_limit(dynamodb_client, table):
    _base(dynamodb_client, table, "e01")
    _update(dynamodb_client, table, "e01", _p("e01", LIMIT))
    assert _stored(dynamodb_client, table, "e01") == LIMIT


@refused_on_default_backends_only
def test_updateitem_refuses_the_same_finished_item(dynamodb_client, table):
    # The statement figure of SET p = :p is 23 bytes more than p, 17 more
    # than the finished item at this key, so UpdateItem stops 17 bytes short
    # of the limit.
    _base(dynamodb_client, table, "u01")
    err = _error(lambda: dynamodb_client.update_item(
        TableName=table,
        Key={"pk": {"S": "u01"}},
        UpdateExpression="SET p = :p",
        ExpressionAttributeValues={":p": {"S": _p("u01", LIMIT)}},
    ))
    assert (err["Code"], err["Message"]) == ("ValidationException", UPDATE_WORDING)
    assert _stored(dynamodb_client, table, "u01") == 7


@refused_on_default_backends_only
def test_an_update_one_byte_over_is_refused_with_the_update_wording(dynamodb_client, table):
    _base(dynamodb_client, table, "e02")
    err = _error(lambda: _update(dynamodb_client, table, "e02", _p("e02", LIMIT + 1)))
    assert (err["Code"], err["Message"]) == ("ValidationException", UPDATE_WORDING)
    assert _stored(dynamodb_client, table, "e02") == 7


@pytest.mark.parametrize(
    ("key", "sets"),
    [("e04", "p = ?, q = ?"), ("e05", "p = ?, q = if_not_exists(q, ?)")],
    ids=["two-clauses", "if-not-exists"],
)
def test_more_clauses_add_nothing_to_the_size(dynamodb_client, table, key, sets):
    _base(dynamodb_client, table, key, q="b" * 10)
    _update(dynamodb_client, table, key, _p(key, LIMIT, other=11), sets=sets,
            more=({"S": "c" * 10},))
    assert _stored(dynamodb_client, table, key) == LIMIT


@refused_on_default_backends_only
def test_two_clauses_one_byte_over_are_refused(dynamodb_client, table):
    _base(dynamodb_client, table, "e06", q="b" * 10)
    err = _error(lambda: _update(dynamodb_client, table, "e06", _p("e06", LIMIT + 1, other=11),
                                 sets="p = ?, q = ?", more=({"S": "c" * 10},)))
    assert (err["Code"], err["Message"]) == ("ValidationException", UPDATE_WORDING)
    assert _stored(dynamodb_client, table, "e06") == 18


def test_a_where_term_that_fails_is_answered_before_the_size(dynamodb_client, table):
    _base(dynamodb_client, table, "e07")
    err = _error(lambda: _update(dynamodb_client, table, "e07", _p("e07", LIMIT + 1),
                                 where=" AND q = 'zz'"))
    assert err["Code"] == "ConditionalCheckFailedException"
    assert _stored(dynamodb_client, table, "e07") == 7


def test_a_missing_item_is_answered_before_the_size(dynamodb_client, table):
    err = _error(lambda: _update(dynamodb_client, table, "e08", _p("e08", LIMIT + 1)))
    assert err["Code"] == "ConditionalCheckFailedException"
    assert _stored(dynamodb_client, table, "e08") is None


def test_returning_reads_back_an_update_at_the_limit(dynamodb_client, table):
    _base(dynamodb_client, table, "e10")
    resp = dynamodb_client.execute_statement(
        Statement=f'UPDATE "{table}" SET p = ? WHERE pk = ? RETURNING MODIFIED OLD *',
        Parameters=[{"S": _p("e10", LIMIT)}, {"S": "e10"}],
    )
    assert resp["Items"] == [{"p": {"S": "a"}}]
    assert _stored(dynamodb_client, table, "e10") == LIMIT


def _batch(client, table: str, key: str, value: str) -> dict:
    return client.batch_execute_statement(Statements=[{
        "Statement": f'UPDATE "{table}" SET p = ? WHERE pk = ?',
        "Parameters": [{"S": value}, {"S": key}],
    }])["Responses"][0]


def test_a_batch_update_writes_a_finished_item_of_the_limit(dynamodb_client, table):
    _base(dynamodb_client, table, "b01")
    assert "Error" not in _batch(dynamodb_client, table, "b01", _p("b01", LIMIT))
    assert _stored(dynamodb_client, table, "b01") == LIMIT


@refused_on_default_backends_only
def test_a_batch_update_one_byte_over_is_its_validation_error_entry(dynamodb_client, table):
    _base(dynamodb_client, table, "b02")
    entry = _batch(dynamodb_client, table, "b02", _p("b02", LIMIT + 1))
    assert entry["Error"] == {"Code": "ValidationError", "Message": UPDATE_WORDING}
    assert _stored(dynamodb_client, table, "b02") == 7


def _transact(client, table: str, key: str, value: str):
    return client.execute_transaction(TransactStatements=[{
        "Statement": f'UPDATE "{table}" SET p = ? WHERE pk = ?',
        "Parameters": [{"S": value}, {"S": key}],
    }])


def test_a_transacted_update_writes_a_finished_item_of_the_limit(dynamodb_client, table):
    _base(dynamodb_client, table, "t01")
    _transact(dynamodb_client, table, "t01", _p("t01", LIMIT))
    assert _stored(dynamodb_client, table, "t01") == LIMIT


@refused_on_default_backends_only
def test_a_transacted_update_one_byte_over_cancels_the_transaction(dynamodb_client, table):
    _base(dynamodb_client, table, "t02")
    with pytest.raises(ClientError) as e:
        _transact(dynamodb_client, table, "t02", _p("t02", LIMIT + 1))
    resp = e.value.response
    assert resp["Error"]["Code"] == "TransactionCanceledException"
    assert [(r.get("Code"), r.get("Message")) for r in resp["CancellationReasons"]] == [
        ("ValidationError", UPDATE_WORDING)
    ]
    assert _stored(dynamodb_client, table, "t02") == 7
