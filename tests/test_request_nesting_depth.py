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

Before authentication the service checks only a body's syntax, at every depth:
a deep body with a syntax fault is a SerializationException unsigned or badly
signed, and a complete deep body gets the authentication error (us-east-1,
2026-11-19). That check is looser than JSON's grammar in three lexemes: a
number such as `01`, `1.`, `1e` or `-`, the escape `\\u12G4`, and NUL, VT or FF
between tokens all reach authentication (us-east-1, 2026-11-24).
"""

from __future__ import annotations

import json
import sys
import time

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from conftest import scoped_table
from raw_signed_request import signed_post

NEST = "Nesting Levels have exceeded supported limits"
NEST_FULL = (
    "Nesting Levels have exceeded supported limits: "
    "Attributes in the item have nested levels beyond supported limit"
)
SERIALIZATION_TYPE = "com.amazon.coral.service#SerializationException"
LEAF = '{"S":"leaf"}'

# Leaves the service's check before authentication passes where JSON's grammar
# refuses them: the numbers `01`, `1.`, `1e` and `-`, the escape `\u12G4`, and
# form feed, NUL and vertical tab before a value (us-east-1, 2026-11-24, at 63
# and 20000 wrappers, 2 of 2 sends each: unsigned
# MissingAuthenticationTokenException, badly signed InvalidSignatureException).
LOOSE_LEAVES = {
    "number-01": '{"S":01}',
    "number-1.": '{"S":1.}',
    "number-1e": '{"S":1e}',
    "number-minus": '{"S":-}',
    "escape-u12G4": '{"S":"le\\u12G4af"}',
    "form-feed-before-value": '{"S":\x0c"leaf"}',
    "nul-before-value": '{"S":\x00"leaf"}',
    "vertical-tab-before-value": '{"S":\x0b"leaf"}',
}


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


def _raw_put_body(table_name: str, pk: str, wrappers: int, leaf: str = LEAF) -> bytes:
    """A PutItem body whose `data` attribute is `wrappers` single-key maps around
    `leaf`, a string leaf unless a row puts a fault there, assembled as raw JSON
    so nothing recurses to build it."""
    data = '{"M":{"n":' * wrappers + leaf + "}}" * wrappers
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


def _unauthenticated_put(dynamodb_client, endpoint_url, body: bytes, signing: str):
    """POST a raw PutItem body with no Authorization header ("unsigned"), or
    signed with the caller's access key and a wrong secret key
    ("bad-signature")."""
    endpoint = dynamodb_client.meta.endpoint_url
    if signing == "unsigned":
        return requests.post(
            endpoint,
            data=body,
            headers={
                "X-Amz-Target": "DynamoDB_20120810.PutItem",
                "Content-Type": "application/x-amz-json-1.0",
            },
            # ExtendDB serves a self-signed certificate; the service does not.
            verify=endpoint_url is None,
            timeout=30,
        )
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    secret = frozen.secret_key
    wrong_secret = secret[:-1] + ("A" if secret[-1] != "A" else "B")
    return signed_post(
        endpoint,
        frozen.access_key,
        wrong_secret,
        dynamodb_client.meta.region_name,
        "PutItem",
        body,
        session_token=frozen.token,
    )


@pytest.mark.parametrize("signing", ["unsigned", "bad-signature"])
@pytest.mark.parametrize(
    "wrappers, leaf",
    [
        pytest.param(None, None, id="x-then-6000-openers"),
        pytest.param(63, None, id="truncated-63"),
        pytest.param(200, None, id="truncated-200"),
        pytest.param(2000, None, id="truncated-2000"),
        pytest.param(20000, None, id="truncated-20000"),
        pytest.param(200, '{"S":"le\\u"}', id="u-before-closing-quote"),
        pytest.param(200, '{"S":\x01"leaf"}', id="soh-before-value"),
        pytest.param(200, '{"S":tru}', id="tru"),
        pytest.param(200, '{"S" "leaf"}', id="missing-colon"),
        pytest.param(200, '{"S":"leaf" "T":"x"}', id="missing-comma"),
        pytest.param(200, '{"S":"le\x01af"}', id="control-byte-in-string"),
        pytest.param(200, '{"S":"le\\qaf"}', id="unknown-escape"),
    ],
)
def test_a_malformed_deep_body_is_refused_before_authentication(
    dynamodb_client, endpoint_url, table, wrappers, leaf, signing
):
    # Each body nests past the 127 levels ExtendDB parses before
    # authentication and has a syntax fault: `x` before the openers, the body
    # without its last byte, or the fault in the leaf. The service answers
    # each with a 400 SerializationException and no message field, unsigned
    # and badly signed (2 of 2 sends per body and signing, us-east-1: the
    # first five rows 2026-11-19, the control byte and the unknown escape
    # 2026-11-20, the other five 2026-11-24).
    if wrappers is None:
        body = b"x" + b"[" * 6000
    elif leaf is None:
        body = _raw_put_body(table, "malformed", wrappers)[:-1]
    else:
        body = _raw_put_body(table, "malformed", wrappers, leaf)
    resp = _unauthenticated_put(dynamodb_client, endpoint_url, body, signing)
    assert resp.status_code == 400, resp.text
    assert _is_messageless_serialization_error(resp.json()), resp.text


@pytest.mark.parametrize("signing", ["unsigned", "bad-signature"])
@pytest.mark.parametrize(
    "wrappers, leaf, tail",
    [
        pytest.param(63, LEAF, b"", id="complete-63"),
        pytest.param(2000, LEAF, b"", id="complete-2000"),
        pytest.param(20000, LEAF, b"", id="complete-20000"),
        pytest.param(2000, LEAF, b"x", id="complete-2000-then-x"),
    ]
    + [
        pytest.param(wrappers, leaf, b"", id=f"{name}-{wrappers}")
        for wrappers in (63, 20000)
        for name, leaf in LOOSE_LEAVES.items()
    ],
)
def test_a_complete_deep_body_reaches_authentication(
    dynamodb_client, endpoint_url, table, wrappers, leaf, tail, signing
):
    # The check before authentication reads one complete JSON value at any
    # depth and ignores bytes after it, as the service's does, and passes each
    # leaf of LOOSE_LEAVES: unsigned, each body is
    # MissingAuthenticationTokenException, and badly signed
    # InvalidSignatureException (2 of 2 sends each, us-east-1, 2026-11-19 for
    # the first four rows and 2026-11-24 for the rest). ExtendDB's type for the
    # first is MissingAuthenticationToken (#1075).
    body = _raw_put_body(table, "unauthenticated", wrappers, leaf) + tail
    resp = _unauthenticated_put(dynamodb_client, endpoint_url, body, signing)
    assert resp.status_code == 400, resp.text
    expected = (
        "MissingAuthenticationToken"
        if signing == "unsigned"
        else "InvalidSignatureException"
    )
    assert resp.json()["__type"].startswith(
        f"com.amazon.coral.service#{expected}"
    ), resp.text
