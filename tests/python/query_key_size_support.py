# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Constants, helpers and table fixtures shared by the Query key size wire files.

The wire files are ``test_query_key_size.py`` (the key size rows),
``test_query_key_size_values.py`` (multi-byte values, ordered BETWEEN bounds,
undefined references, a hash-only table) and ``test_query_key_size_order.py``
(where the size refusal sits among the other Query refusals). Fixtures are
imported by name into each file so each file gets its own module-scoped tables.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

from helpers import assert_validation, unique_name, wait_for_active, wait_for_deleted

HASH_MESSAGE = (
    "One or more parameter values were invalid: "
    "Size of hashkey has exceeded the maximum size limit of2048 bytes"
)
HASH_TOTAL_MESSAGE = (
    "One or more parameter values were invalid: "
    "Aggregated size of all hash keys has exceeded the size limit of 2048 bytes"
)
RANGE_MESSAGE = (
    "One or more parameter values were invalid: "
    "Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
)


def S(length: int, fill: str = "x") -> dict:
    """A string AttributeValue of exactly ``length`` bytes (ASCII fill)."""
    return {"S": fill * length}


def B(length: int) -> dict:
    """A binary AttributeValue of exactly ``length`` bytes."""
    return {"B": b"\x01" * length}


def attr(name: str, attribute_type: str = "S") -> dict:
    return {"AttributeName": name, "AttributeType": attribute_type}


def key(name: str, key_type: str) -> dict:
    return {"AttributeName": name, "KeyType": key_type}


def drop_table(client, name: str) -> None:
    try:
        client.delete_table(TableName=name)
    except client.exceptions.ResourceNotFoundException:
        return
    wait_for_deleted(client, name)


@contextmanager
def make_table(client, prefix: str, **create_args):
    """Create an ACTIVE pay-per-request table, yield its name, drop it on exit."""
    name = unique_name(prefix)
    client.create_table(TableName=name, BillingMode="PAY_PER_REQUEST", **create_args)
    try:
        wait_for_active(client, name)
        yield name
    finally:
        drop_table(client, name)


def refused(client, message: str, code: str = "ValidationException", **query) -> None:
    """Run the Query and assert a 400 with ``code`` and exactly ``message``."""
    with pytest.raises(ClientError) as exc_info:
        client.query(**query)
    assert_validation(exc_info.value, message, code)


def refused_ending(client, suffix: str, **query) -> None:
    """Run the Query and assert a 400 ValidationException whose text ends with ``suffix``.

    For refusals whose leading label differs between the service and ExtendDB
    (a pre-existing wording gap); the size sentences never come back.
    """
    with pytest.raises(ClientError) as exc_info:
        client.query(**query)
    err = exc_info.value.response
    assert err["Error"]["Code"] == "ValidationException", err["Error"]["Message"]
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Error"]["Message"].endswith(suffix), err["Error"]["Message"]


FIRST_RANGE_ONLY_MARK = "applies a key condition to the first RANGE attribute"


def require_served(client, **query) -> None:
    """Skip unless the backend serves ``query``'s condition on a later RANGE attribute.

    The MongoDB and Cassandra backends apply a key condition to the first RANGE
    attribute of a multi-attribute index only, and refuse a condition on a later
    one with a ValidationException that says so. The size checks run before that
    refusal, so only the rows that expect a Query to be served call this. Sends
    ``query`` with every value cut to its first byte.
    """
    probe = dict(query)
    probe["ExpressionAttributeValues"] = {
        name: {"S": value["S"][:1]} for name, value in query["ExpressionAttributeValues"].items()
    }
    try:
        client.query(**probe)
    except ClientError as exc:
        if FIRST_RANGE_ONLY_MARK in exc.response["Error"]["Message"]:
            pytest.skip("this backend applies key conditions to the first RANGE attribute only")


@pytest.fixture(scope="module")
def gsi_table(dynamodb_client):
    """Base HASH ``pk``; index ``idx`` on HASH ``h`` and RANGE ``a``, ``b``, ``c``, ``d``."""
    with make_table(
        dynamodb_client,
        "qks-gsi",
        AttributeDefinitions=[attr(n) for n in ("pk", "h", "a", "b", "c", "d")],
        KeySchema=[key("pk", "HASH")],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "idx",
                "KeySchema": [
                    key("h", "HASH"),
                    key("a", "RANGE"),
                    key("b", "RANGE"),
                    key("c", "RANGE"),
                    key("d", "RANGE"),
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def two_hash_table(dynamodb_client):
    """Base HASH ``pk``; index ``two-hash`` on HASH ``h1``, ``h2`` and RANGE ``a``."""
    with make_table(
        dynamodb_client,
        "qks-2h",
        AttributeDefinitions=[attr(n) for n in ("pk", "h1", "h2", "a")],
        KeySchema=[key("pk", "HASH")],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "two-hash",
                "KeySchema": [key("h1", "HASH"), key("h2", "HASH"), key("a", "RANGE")],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def base_table(dynamodb_client):
    """HASH ``pk`` and RANGE ``sk``, both S."""
    with make_table(
        dynamodb_client,
        "qks-base",
        AttributeDefinitions=[attr("pk"), attr("sk")],
        KeySchema=[key("pk", "HASH"), key("sk", "RANGE")],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def hash_only_table(dynamodb_client):
    """HASH ``pk`` only."""
    with make_table(
        dynamodb_client,
        "qks-hash",
        AttributeDefinitions=[attr("pk")],
        KeySchema=[key("pk", "HASH")],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def binary_table(dynamodb_client):
    """HASH ``pk`` and RANGE ``sk``, both B."""
    with make_table(
        dynamodb_client,
        "qks-bin",
        AttributeDefinitions=[attr("pk", "B"), attr("sk", "B")],
        KeySchema=[key("pk", "HASH"), key("sk", "RANGE")],
    ) as name:
        yield name


@pytest.fixture(scope="module")
def index_table(dynamodb_client):
    """HASH ``pk`` and RANGE ``sk``; LSI ``lsi`` on RANGE ``l``; GSI ``gsi`` on ``g`` and ``gs``.

    Two KEYS_ONLY GSIs as well: ``gko`` on HASH ``g`` and ``ngsi`` on HASH ``n`` (N).
    """
    keys_only = {"ProjectionType": "KEYS_ONLY"}
    with make_table(
        dynamodb_client,
        "qks-idx",
        AttributeDefinitions=[attr(n) for n in ("pk", "sk", "l", "g", "gs")] + [attr("n", "N")],
        KeySchema=[key("pk", "HASH"), key("sk", "RANGE")],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi",
                "KeySchema": [key("pk", "HASH"), key("l", "RANGE")],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi",
                "KeySchema": [key("g", "HASH"), key("gs", "RANGE")],
                "Projection": {"ProjectionType": "ALL"},
            },
            {"IndexName": "gko", "KeySchema": [key("g", "HASH")], "Projection": keys_only},
            {"IndexName": "ngsi", "KeySchema": [key("n", "HASH")], "Projection": keys_only},
        ],
    ) as name:
        yield name
