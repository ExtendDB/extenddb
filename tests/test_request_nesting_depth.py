# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A deeply nested request body is parsed and answered by the depth that
applies, not refused by the JSON parser.

The JSON deserializer used to refuse any body nested past 128 structural levels
with a SerializationException, so a request whose attribute nested past that
never reached the nesting-depth check. Amazon DynamoDB parses such a request and
answers with the document nesting "Nesting Levels have exceeded supported
limits..." ValidationException, and only a body nested past about 2000 wrappers
(roughly 4000 JSON levels) is refused, then with a message-less
SerializationException. Measured against the service (us-east-1, 2026-11-02).

Dual-target: runs against the service and against ExtendDB on every backend.

Bodies shallow enough for the client library to serialize without hitting its
own recursion limit use boto3 directly; deeper bodies are built as raw JSON and
signed, so no client library recurses to produce them.
"""

from __future__ import annotations

import json
import sys

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table
from raw_signed_request import signed_post

# botocore serializes a nested dict recursively, so lift CPython's own limit for
# the boto3 cases below (the raw-JSON cases do not recurse and do not need it).
sys.setrecursionlimit(10_000)

NEST = "Nesting Levels have exceeded supported limits"
NEST_FULL = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
SERIALIZATION_TYPE = "com.amazon.coral.service#SerializationException"


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _deep(wrappers: int) -> dict:
    """`wrappers` single-key maps around a string leaf: depth 1 is
    `{"M":{"n":{"S":"leaf"}}}`."""
    value: dict = {"S": "leaf"}
    for _ in range(wrappers):
        value = {"M": {"n": value}}
    return value


def _put_error(client, table_name: str, data: dict) -> dict:
    with pytest.raises(ClientError) as exc_info:
        client.put_item(TableName=table_name, Item={"pk": {"S": "k"}, "data": data})
    return exc_info.value.response["Error"]


def _raw_put_body(table_name: str, pk: str, wrappers: int) -> bytes:
    """A PutItem body whose `data` attribute is `wrappers` single-key maps around
    a string leaf, assembled as raw JSON so nothing recurses to build it."""
    data = '{"M":{"n":' * wrappers + '{"S":"leaf"}' + "}}" * wrappers
    return (
        '{"TableName":'
        + json.dumps(table_name)
        + ',"Item":{"pk":{"S":'
        + json.dumps(pk)
        + '},"data":'
        + data
        + "}}"
    ).encode("utf-8")


def _raw_put(dynamodb_client, table_name: str, pk: str, wrappers: int):
    """Sign and POST a raw deep PutItem body with the caller's own credentials."""
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    return signed_post(
        dynamodb_client.meta.endpoint_url,
        frozen.access_key,
        frozen.secret_key,
        dynamodb_client.meta.region_name,
        "PutItem",
        _raw_put_body(table_name, pk, wrappers),
        session_token=frozen.token,
    )


def test_an_attribute_within_the_limit_is_stored(dynamodb_client, table):
    # Positive control: 31 wrappers (leaf at level 32) is within the document
    # limit and is stored, so the parse change did not break the accept path.
    dynamodb_client.put_item(
        TableName=table, Item={"pk": {"S": "ok31"}, "data": _deep(31)}
    )


def test_a_63_level_attribute_is_a_nesting_validation_error(dynamodb_client, table):
    # 63 wrappers nests the request just past the 128 structural level parse
    # limit. The server must read it and answer with the nesting
    # ValidationException, not a SerializationException.
    err = _put_error(dynamodb_client, table, _deep(63))
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == NEST_FULL, err


def test_an_attribute_far_past_the_parse_limit_still_validates(dynamodb_client, table):
    # Well past the old parse limit and within the request depth ceiling: still
    # a nesting ValidationException, never a parse failure.
    err = _put_error(dynamodb_client, table, _deep(120))
    assert err["Code"] == "ValidationException", err
    assert NEST in err["Message"], err


@pytest.mark.parametrize("wrappers", [300, 1000, 2000])
def test_a_deep_body_within_the_ceiling_is_a_nesting_validation_error(
    dynamodb_client, table, wrappers
):
    # The service parses a body nested far past the old 128 level parse limit,
    # up to at least 2000 wrappers, and answers with the document nesting
    # ValidationException. Built as raw JSON so the client does not recurse.
    resp = _raw_put(dynamodb_client, table, f"deep{wrappers}", wrappers)
    assert resp.status_code == 400, resp.text
    err = resp.json()
    assert err["__type"].endswith("ValidationException"), err
    assert err["message"] == NEST_FULL, err


def test_a_body_far_past_the_ceiling_is_a_messageless_serialization_error(
    dynamodb_client, table
):
    # Far past any depth the service parses. It answers HTTP 400 with the
    # SerializationException type and no message field at all.
    resp = _raw_put(dynamodb_client, table, "huge", 20_000)
    assert resp.status_code == 400, resp.text
    err = resp.json()
    assert err["__type"] == SERIALIZATION_TYPE, err
    assert "message" not in err, err
