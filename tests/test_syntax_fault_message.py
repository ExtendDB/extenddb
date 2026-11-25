# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A request body with a JSON syntax fault gets a SerializationException with
no message field.

Amazon DynamoDB answers each body below that it does not read with HTTP 400 and
``{"__type":"com.amazon.coral.service#SerializationException"}`` alone. It reads
the two bodies that are one object, with or without white space after it, and
answers them with ResourceNotFoundException, since the table named does not
exist. Each body was sent twice, signed, in us-east-1 on 2026-11-20, with the
same answer both times. It also reads a body with a number written with
leading zeros, as the decimal number the digits spell (the ``leading-zero``
rows, measured the same way on 2026-11-21), a number that ends in a point and
a minus sign on its own (the ``trailing-point`` and ``lone-minus`` rows, the
same day). It refuses ``+1``, ``.5`` and ``0x10`` as it refuses a syntax fault
(the ``plus-sign``, ``point-first`` and ``hex`` rows, the same day).

Dual-target: runs against the service and against ExtendDB.
"""

from __future__ import annotations

import uuid

import boto3
import pytest

from raw_signed_request import signed_post
from test_request_nesting_depth import _raw_put_body

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
    "leading-zero": ("DescribeTable", NAMED[:-1] + b', "x": 01}', True),
    "leading-zero-zero": ("DescribeTable", NAMED[:-1] + b', "x": 00}', True),
    "leading-zero-negative": ("DescribeTable", NAMED[:-1] + b', "x": -01}', True),
    "leading-zero-fraction": ("DescribeTable", NAMED[:-1] + b', "x": 01.5}', True),
    "leading-zero-zero-fraction": ("DescribeTable", NAMED[:-1] + b', "x": 00.5}', True),
    "leading-zero-exponent": ("DescribeTable", NAMED[:-1] + b', "x": 01e1}', True),
    "leading-zero-three": ("DescribeTable", NAMED[:-1] + b', "x": 0001}', True),
    "leading-zero-negative-zero": ("DescribeTable", NAMED[:-1] + b', "x": -00}', True),
    "leading-zero-list": ("DescribeTable", NAMED[:-1] + b', "x": [01, 02]}', True),
    "leading-zero-map": ("DescribeTable", NAMED[:-1] + b', "x": {"y": 01}}', True),
    "leading-zero-first": ("DescribeTable", b'{"x": 01, ' + NAMED[1:], True),
    "leading-zero-limit": ("Scan", NAMED[:-1] + b', "Limit": 01}', True),
    "trailing-point": ("DescribeTable", NAMED[:-1] + b', "x": 1.}', True),
    "trailing-point-limit": ("Scan", NAMED[:-1] + b', "Limit": 1.}', True),
    "lone-minus": ("DescribeTable", NAMED[:-1] + b', "x": -}', True),
    "plus-sign": ("DescribeTable", NAMED[:-1] + b', "x": +1}', False),
    "point-first": ("DescribeTable", NAMED[:-1] + b', "x": .5}', False),
    "hex": ("DescribeTable", NAMED[:-1] + b', "x": 0x10}', False),
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
