# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""BatchWriteItem rejects a faulty batch before it applies any request.

The BatchWriteItem API reference has DynamoDB reject the whole batch, with a
400, for a request fault (a key that does not match the table's schema,
duplicate keys, an item over 400 KB, a table that does not exist). None of the
batch's requests is applied, wherever the faulty request sits: the checks run
for the whole batch before the first write, so a valid request listed ahead of
the faulty one is not applied either.

The tests need no contention: a valid request first and a faulty one second
is enough to tell a handler that checks the whole batch from one that checks
each request just before it writes it.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

# A table that is never created.
ABSENT_TABLE = f"extenddb-absent-{uuid.uuid4().hex[:12]}"

# The server visits the tables of one batch in an unspecified order, so a
# cross-table batch is sent several times, each time with fresh keys.
ROUNDS = 8


@pytest.fixture(scope="module")
def table_a(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def table_b(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _put(pk: str) -> dict:
    return {"PutRequest": {"Item": {"pk": {"S": pk}}}}


def _put_with_wrong_key_type() -> dict:
    """A put whose partition key is a number, against tables keyed by a string."""
    return {"PutRequest": {"Item": {"pk": {"N": "1"}}}}


def _delete(pk: str) -> dict:
    return {"DeleteRequest": {"Key": {"pk": {"S": pk}}}}


def _put_over_the_item_size_limit(pk: str) -> dict:
    """A put of 401 KB, past the item limit of 400 KB (409,600 bytes)."""
    item = {"pk": {"S": pk}, "pad": {"S": "x" * (401 * 1024)}}
    return {"PutRequest": {"Item": item}}


def _delete_with_an_extra_key_attribute(pk: str) -> dict:
    """A delete whose key carries an attribute that is not part of the key."""
    return {"DeleteRequest": {"Key": {"pk": {"S": pk}, "extra": {"S": "x"}}}}


def _exists(client, table: str, pk: str) -> bool:
    resp = client.get_item(
        TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True
    )
    return "Item" in resp


def _rejected(client, request_items: dict) -> str:
    """Send the batch, require a ClientError and return its code."""
    with pytest.raises(ClientError) as exc_info:
        client.batch_write_item(RequestItems=request_items)
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    return exc_info.value.response["Error"]["Code"]


def test_valid_put_before_an_invalid_put_is_not_applied(dynamodb_client, table_a):
    pk = f"valid-{uuid.uuid4().hex[:8]}"

    code = _rejected(
        dynamodb_client, {table_a: [_put(pk), _put_with_wrong_key_type()]}
    )

    assert code == "ValidationException"
    assert not _exists(dynamodb_client, table_a, pk)


def test_valid_delete_before_an_invalid_put_is_not_applied(dynamodb_client, table_a):
    pk = f"keep-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=table_a, Item={"pk": {"S": pk}})

    code = _rejected(
        dynamodb_client, {table_a: [_delete(pk), _put_with_wrong_key_type()]}
    )

    assert code == "ValidationException"
    assert _exists(dynamodb_client, table_a, pk)


def test_valid_put_in_one_table_with_an_invalid_put_in_another_is_not_applied(
    dynamodb_client, table_a, table_b
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"

        code = _rejected(
            dynamodb_client,
            {table_a: [_put(pk)], table_b: [_put_with_wrong_key_type()]},
        )

        assert code == "ValidationException"
        assert not _exists(dynamodb_client, table_a, pk)


def test_valid_put_with_a_put_to_a_missing_table_is_not_applied(
    dynamodb_client, table_a
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"

        code = _rejected(
            dynamodb_client, {table_a: [_put(pk)], ABSENT_TABLE: [_put(pk)]}
        )

        assert code == "ResourceNotFoundException"
        assert not _exists(dynamodb_client, table_a, pk)


def test_valid_put_with_duplicate_keys_in_another_table_is_not_applied(
    dynamodb_client, table_a, table_b
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"
        dup = f"dup-{uuid.uuid4().hex[:8]}"

        code = _rejected(
            dynamodb_client,
            {table_a: [_put(pk)], table_b: [_put(dup), _put(dup)]},
        )

        assert code == "ValidationException"
        assert not _exists(dynamodb_client, table_a, pk)


@pytest.mark.parametrize(
    "faulty_request",
    [_put_over_the_item_size_limit, _delete_with_an_extra_key_attribute],
    ids=["item_over_400_kb", "delete_with_an_extra_key_attribute"],
)
def test_valid_put_before_an_oversized_item_or_a_malformed_delete_is_not_applied(
    dynamodb_client, table_a, faulty_request
):
    pk = f"valid-{uuid.uuid4().hex[:8]}"
    faulty_pk = f"faulty-{uuid.uuid4().hex[:8]}"

    code = _rejected(
        dynamodb_client, {table_a: [_put(pk), faulty_request(faulty_pk)]}
    )

    assert code == "ValidationException"
    assert not _exists(dynamodb_client, table_a, pk)
