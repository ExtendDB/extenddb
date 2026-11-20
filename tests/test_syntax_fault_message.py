# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A request body with a JSON syntax fault gets a SerializationException with
no message field.

Amazon DynamoDB answers each body below that it does not read with HTTP 400 and
``{"__type":"com.amazon.coral.service#SerializationException"}`` alone. It reads
the two bodies that are one object, with or without white space after it, and
answers them with ResourceNotFoundException, since the table named does not
exist. Each body was sent twice, signed, in us-east-1 on 2026-11-20, with the
same answer both times.

Dual-target: runs against the service and against ExtendDB.
"""

from __future__ import annotations

import uuid

import boto3
import pytest

from raw_signed_request import signed_post
from test_request_nesting_depth import (
    _is_messageless_serialization_error,
    _raw_put_body,
    _unauthenticated_post,
)

MISSING = f"extenddb-test-{uuid.uuid4().hex[:12]}".encode()
NAMED = b'{"TableName": "' + MISSING + b'"}'
TRUNCATED_10 = _raw_put_body(MISSING.decode(), "k", 10)[:-1]


def _item(s: bytes) -> bytes:
    """A PutItem body whose key attribute is the string `s`, as written."""
    return b'{"TableName": "' + MISSING + b'", "Item": {"pk": {"S": "' + s + b'"}}}'


# Rows: the operation, the body, and whether the service reads the body.
ROWS = {
    "one-object": ("DescribeTable", NAMED, True),
    "white-space-after-the-object": ("DescribeTable", NAMED + b"  \n", True),
    "empty": ("DescribeTable", b"", False),
    "white-space-only": ("DescribeTable", b"   ", False),
    "not-json": ("DescribeTable", b"{not json", False),
    "byte-order-mark": ("DescribeTable", b"\xef\xbb\xbf" + NAMED, False),
    "trailing-comma": ("DescribeTable", NAMED[:-1] + b",}", False),
    "nan": ("DescribeTable", NAMED[:-1] + b', "Limit": NaN}', False),
    "bad-literal": ("DescribeTable", NAMED[:-1] + b', "x": tru}', False),
    "control-byte": ("DescribeTable", NAMED[:-1] + b', "x": "\x01"}', False),
    "x-then-200-openers": ("PutItem", b"x" + b"[" * 200, False),
    "truncated-10": ("PutItem", TRUNCATED_10, False),
    "bracket-for-brace-10": ("PutItem", TRUNCATED_10 + b"]", False),
    "bad-escape": ("PutItem", _item(b"a\\qb"), False),
    "control-byte-in-item": ("PutItem", _item(b"a\x01b"), False),
}


@pytest.mark.parametrize("operation, body, read", list(ROWS.values()), ids=list(ROWS))
def test_a_body_with_a_syntax_fault_gets_no_message(
    dynamodb_client, operation, body, read
):
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    resp = signed_post(
        dynamodb_client.meta.endpoint_url,
        frozen.access_key,
        frozen.secret_key,
        dynamodb_client.meta.region_name,
        operation,
        body,
        session_token=frozen.token,
    )
    assert resp.status_code == 400, resp.text
    err = resp.json()
    if read:
        assert err["__type"].endswith("#ResourceNotFoundException"), resp.text
    else:
        assert err == {
            "__type": "com.amazon.coral.service#SerializationException"
        }, resp.text


# Rows: the body, and whether the service refuses it before authentication.
# Each body went to DescribeTable twice with no signature and twice signed for
# an access key that does not exist, in us-east-1 on 2026-11-20, with the same
# answer each time. A body refused first gets the SerializationException with
# no message. Every other body, the empty one included, gets the
# authentication error.
UNAUTHENTICATED_ROWS = {
    "empty": (b"", False),
    "one-space": (b" ", True),
    "white-space": (b"   \n\t", True),
    "newline": (b"\n", True),
    "open-brace": (b"{", True),
    "letter": (b"x", True),
    "byte-order-mark-alone": (b"\xef\xbb\xbf", True),
    "not-json": (b"{not json", True),
    "nul-byte": (b"\x00", True),
    "null": (b"null", False),
    "empty-array": (b"[]", False),
    "one-object": (NAMED, False),
    "string": (b'"x"', False),
    "number": (b"5", False),
}

# The type of the authentication error for each signing. ExtendDB's type for a
# missing signature is MissingAuthenticationToken (#1075), so the type after
# the "#" is compared by its start.
AUTHENTICATION_ERRORS = {
    "unsigned": "MissingAuthenticationToken",
    "unknown-key": "UnrecognizedClientException",
}


@pytest.mark.parametrize("signing", list(AUTHENTICATION_ERRORS))
@pytest.mark.parametrize(
    "body, refused", list(UNAUTHENTICATED_ROWS.values()), ids=list(UNAUTHENTICATED_ROWS)
)
def test_which_bodies_are_refused_before_authentication(
    dynamodb_client, endpoint_url, body, refused, signing
):
    resp = _unauthenticated_post(
        dynamodb_client, endpoint_url, body, signing, "DescribeTable"
    )
    assert resp.status_code == 400, resp.text
    err = resp.json()
    if refused:
        assert _is_messageless_serialization_error(err), resp.text
    else:
        error_type = err["__type"].rpartition("#")[2]
        assert error_type.startswith(AUTHENTICATION_ERRORS[signing]), resp.text
