# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A deeply nested request body is parsed and answered by the depth that
applies, not refused by the JSON parser.

The JSON deserializer used to refuse any body nested 128 or more structural
levels deep with a SerializationException, so a request whose attribute nested
past that never reached the nesting-depth check. Amazon DynamoDB parses such a
request and answers with the document nesting "Nesting Levels have exceeded
supported limits..." ValidationException. It has no fixed ceiling: a PutItem
attribute of up to 1500 single-key wrappers (about 3000 JSON levels) got that
ValidationException on every send that was a 400, from 1700 wrappers the answer
varied from request to request between it and a message-less
SerializationException, and 5000 wrappers and deeper got the
SerializationException on every send. A few sends at 1500 wrappers and deeper
were a 500 InternalFailure instead (us-east-1, 2026-11-12 and 2026-11-13, 24 or
more sends per depth across several windows).

Dual-target: runs against the service and against ExtendDB on every backend.

Bodies shallow enough for the client library to serialize without hitting its
own recursion limit use boto3 directly; deeper bodies are built as raw JSON and
signed, so no client library recurses to produce them.
"""

from __future__ import annotations

import json
import sys
import time

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table
from raw_signed_request import signed_post

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


@pytest.fixture
def deep_recursion():
    """botocore serializes a nested dict recursively; lift CPython's limit for a
    case that builds a deep Item through boto3, and restore it afterwards."""
    previous = sys.getrecursionlimit()
    sys.setrecursionlimit(10_000)
    yield
    sys.setrecursionlimit(previous)


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
    """Sign and POST a raw deep PutItem body with the caller's own credentials.

    Sent up to three times while the answer is a 500. The service gives a deep
    body a 500 InternalFailure now and then (1500 wrappers: 2 of 56 sends; 2000:
    1 of 37; 2598: 2 of 48; us-east-1, 2026-11-13), and the client library
    retries a 500 by default, so the boto3 cases in this file never see one.
    """
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    body = _raw_put_body(table_name, pk, wrappers)
    for attempt in range(3):
        if attempt:
            time.sleep(attempt)
        resp = signed_post(
            dynamodb_client.meta.endpoint_url,
            frozen.access_key,
            frozen.secret_key,
            dynamodb_client.meta.region_name,
            "PutItem",
            body,
            session_token=frozen.token,
        )
        if resp.status_code != 500:
            break
    return resp


def _is_nesting_validation_error(err: dict) -> bool:
    return err["__type"].endswith("ValidationException") and err["message"] == NEST_FULL


def _is_messageless_serialization_error(err: dict) -> bool:
    return err["__type"] == SERIALIZATION_TYPE and "message" not in err


def test_an_attribute_within_the_limit_is_stored(dynamodb_client, table):
    # Positive control: 31 wrappers (leaf at level 32) is within the document
    # limit and is stored, so the parse change did not break the accept path.
    dynamodb_client.put_item(
        TableName=table, Item={"pk": {"S": "ok31"}, "data": _deep(31)}
    )


def test_a_63_level_attribute_is_a_nesting_validation_error(
    dynamodb_client, table, deep_recursion
):
    # 63 wrappers nests the request just past the 128 structural level parse
    # limit. The server must read it and answer with the nesting
    # ValidationException, not a SerializationException.
    err = _put_error(dynamodb_client, table, _deep(63))
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == NEST_FULL, err


def test_an_attribute_far_past_the_parse_limit_still_validates(
    dynamodb_client, table, deep_recursion
):
    # Well past the old parse limit and within the request depth ceiling: still
    # a nesting ValidationException, never a parse failure.
    err = _put_error(dynamodb_client, table, _deep(120))
    assert err["Code"] == "ValidationException", err
    assert NEST in err["Message"], err


@pytest.mark.parametrize("wrappers", [300, 1000, 1500])
def test_a_deep_body_the_service_always_validates_is_a_nesting_error(
    dynamodb_client, table, wrappers
):
    # Up to 1500 wrappers the service's 400 was the document nesting
    # ValidationException on every send (1500 wrappers: 102 of 104 sends across
    # 2026-11-12 and 2026-11-13, the other 2 a 500 that _raw_put retries); from
    # 1700 wrappers the answer varies. Built as raw JSON so the client does not
    # recurse.
    resp = _raw_put(dynamodb_client, table, f"deep{wrappers}", wrappers)
    assert resp.status_code == 400, resp.text
    assert _is_nesting_validation_error(resp.json()), resp.text


@pytest.mark.parametrize("wrappers", [2000, 2598])
def test_a_body_in_the_varying_band_gets_one_of_the_two_answers(
    dynamodb_client, table, wrappers
):
    # From 1700 wrappers the service's answer varies per request: the nesting
    # ValidationException or the message-less SerializationException (2000
    # wrappers: 23 and 1 of 24 in one set of windows, 17 and 14 of 32 in
    # another). 2598 wrappers is the deepest body the server's ceiling admits;
    # it parses it and answers the ValidationException.
    resp = _raw_put(dynamodb_client, table, f"deep{wrappers}", wrappers)
    assert resp.status_code == 400, resp.text
    err = resp.json()
    assert _is_nesting_validation_error(err) or _is_messageless_serialization_error(
        err
    ), err


def test_a_body_far_past_the_ceiling_is_a_messageless_serialization_error(
    dynamodb_client, table
):
    # Far past any depth the service parses (5000 wrappers and deeper were
    # refused on every send). It answers HTTP 400 with the SerializationException
    # type and no message field at all.
    resp = _raw_put(dynamodb_client, table, "huge", 20_000)
    assert resp.status_code == 400, resp.text
    assert _is_messageless_serialization_error(resp.json()), resp.text
