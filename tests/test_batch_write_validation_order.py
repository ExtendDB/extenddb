# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""BatchWriteItem rejects a faulty batch before it applies any request.

The BatchWriteItem API reference has DynamoDB reject the whole batch, with a
400, for a request fault (a key that does not match the table's schema,
duplicate keys, an item over 400 KB, a table that does not exist). None of the
batch's requests is applied, wherever the faulty request sits: the checks run
for the whole batch before the first write, so a valid request listed ahead of
the faulty one is not applied either. A put whose global secondary index key
has the wrong type or is an empty string is rejected the same way, although
the reference does not list that fault.

Each test checks the error code and the message. The messages are the ones
the service answered to these batches (us-east-1, 2026-11-10, two sends
each).

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

KEY_MISMATCH = "The provided key element does not match the schema"
TABLE_NOT_FOUND = "Requested resource not found"
DUPLICATE_KEYS = "Provided list of item keys contains duplicates"
ITEM_TOO_LARGE = "Item size has exceeded the maximum allowed size"
GSI_KEY_TYPE_MISMATCH = (
    "One or more parameter values were invalid: Type mismatch for Index Key g "
    "Expected: S Actual: N IndexName: by-g"
)
GSI_KEY_EMPTY = (
    "One or more parameter values are not valid. A value specified for a "
    "secondary index key is not supported. The AttributeValue for a key "
    "attribute cannot contain an empty string value. IndexName: by-g, IndexKey: g"
)


@pytest.fixture(scope="module")
def table_a(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def table_b(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture(scope="module")
def table_with_gsi(dynamodb_client):
    """Keyed on pk (S), with a global secondary index by-g keyed on g (S)."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-g",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


def _put(pk: str, **attributes) -> dict:
    return {"PutRequest": {"Item": {"pk": {"S": pk}, **attributes}}}


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


def _put_with_a_number_gsi_key(pk: str) -> dict:
    return _put(pk, g={"N": "1"})


def _put_with_an_empty_gsi_key(pk: str) -> dict:
    return _put(pk, g={"S": ""})


def _exists(client, table: str, pk: str) -> bool:
    resp = client.get_item(
        TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True
    )
    return "Item" in resp


def _rejected(client, request_items: dict) -> tuple[str, str]:
    """Send the batch, require a 400 and return its error code and message."""
    with pytest.raises(ClientError) as exc_info:
        client.batch_write_item(RequestItems=request_items)
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    error = exc_info.value.response["Error"]
    return error["Code"], error["Message"]


def test_valid_put_before_an_invalid_put_is_not_applied(dynamodb_client, table_a):
    pk = f"valid-{uuid.uuid4().hex[:8]}"

    code, message = _rejected(
        dynamodb_client, {table_a: [_put(pk), _put_with_wrong_key_type()]}
    )

    assert code == "ValidationException"
    assert message == KEY_MISMATCH
    assert not _exists(dynamodb_client, table_a, pk)


def test_valid_delete_before_an_invalid_put_is_not_applied(dynamodb_client, table_a):
    pk = f"keep-{uuid.uuid4().hex[:8]}"
    dynamodb_client.put_item(TableName=table_a, Item={"pk": {"S": pk}})

    code, message = _rejected(
        dynamodb_client, {table_a: [_delete(pk), _put_with_wrong_key_type()]}
    )

    assert code == "ValidationException"
    assert message == KEY_MISMATCH
    assert _exists(dynamodb_client, table_a, pk)


def test_valid_put_in_one_table_with_an_invalid_put_in_another_is_not_applied(
    dynamodb_client, table_a, table_b
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"

        code, message = _rejected(
            dynamodb_client,
            {table_a: [_put(pk)], table_b: [_put_with_wrong_key_type()]},
        )

        assert code == "ValidationException"
        assert message == KEY_MISMATCH
        assert not _exists(dynamodb_client, table_a, pk)


def test_valid_put_with_a_put_to_a_missing_table_is_not_applied(
    dynamodb_client, table_a
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"

        code, message = _rejected(
            dynamodb_client, {table_a: [_put(pk)], ABSENT_TABLE: [_put(pk)]}
        )

        assert code == "ResourceNotFoundException"
        assert message == TABLE_NOT_FOUND
        assert not _exists(dynamodb_client, table_a, pk)


def test_valid_put_with_duplicate_keys_in_another_table_is_not_applied(
    dynamodb_client, table_a, table_b
):
    for _ in range(ROUNDS):
        pk = f"valid-{uuid.uuid4().hex[:8]}"
        dup = f"dup-{uuid.uuid4().hex[:8]}"

        code, message = _rejected(
            dynamodb_client,
            {table_a: [_put(pk)], table_b: [_put(dup), _put(dup)]},
        )

        assert code == "ValidationException"
        assert message == DUPLICATE_KEYS
        assert not _exists(dynamodb_client, table_a, pk)


@pytest.mark.parametrize(
    ("faulty_request", "expected_message"),
    [
        (_put_over_the_item_size_limit, ITEM_TOO_LARGE),
        (_delete_with_an_extra_key_attribute, KEY_MISMATCH),
    ],
    ids=["item_over_400_kb", "delete_with_an_extra_key_attribute"],
)
def test_valid_put_before_an_oversized_item_or_a_malformed_delete_is_not_applied(
    dynamodb_client, table_a, faulty_request, expected_message
):
    pk = f"valid-{uuid.uuid4().hex[:8]}"
    faulty_pk = f"faulty-{uuid.uuid4().hex[:8]}"

    code, message = _rejected(
        dynamodb_client, {table_a: [_put(pk), faulty_request(faulty_pk)]}
    )

    assert code == "ValidationException"
    assert message == expected_message
    assert not _exists(dynamodb_client, table_a, pk)


@pytest.mark.parametrize(
    ("faulty_request", "expected_message"),
    [
        (_put_with_a_number_gsi_key, GSI_KEY_TYPE_MISMATCH),
        (_put_with_an_empty_gsi_key, GSI_KEY_EMPTY),
    ],
    ids=["gsi_key_as_a_number", "gsi_key_as_an_empty_string"],
)
def test_valid_put_before_a_faulty_gsi_key_is_not_applied(
    dynamodb_client, table_with_gsi, faulty_request, expected_message
):
    pk = f"valid-{uuid.uuid4().hex[:8]}"
    faulty_pk = f"faulty-{uuid.uuid4().hex[:8]}"

    code, message = _rejected(
        dynamodb_client,
        {table_with_gsi: [_put(pk, g={"S": "ok"}), faulty_request(faulty_pk)]},
    )

    assert code == "ValidationException"
    assert message == expected_message
    assert not _exists(dynamodb_client, table_with_gsi, pk)
