# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A B value that does not decode as base64: a B, a BS member, a value in an
M or an L, a key, an ExpressionAttributeValues entry and a PartiQL parameter.

The service refuses it with a SerializationException ahead of the table
lookup. The text depends on the value's length in UTF-8 bytes. One byte is
too short, and a length that is not a multiple of 4 is named, except for
"==", which gets no message. A value whose bytes ahead of its closing run of
"=" are all base64 characters has an invalid last character, and any other
value gets no message. Every answer below was measured against the service
in us-east-1 on 2026-11-20 and 2026-11-25, each request twice. The table
does not exist, so a value that decodes gets ResourceNotFoundException.
boto3 encodes a binary value itself, so the bodies go out as written.

Dual-target: runs against the service and against extenddb with the same
assertions.
"""

from __future__ import annotations

import json
import uuid

import pytest

from raw_signed_request import client_post

MISSING = f"extenddb-test-missing-{uuid.uuid4().hex[:12]}"

SHORT = ("SerializationException", "Buffer is too short for base64 encoding")
LAST = ("SerializationException", "Invalid last non-pad Base64 character dectected")
NO_MESSAGE = ("SerializationException", None)
NOT_FOUND = ("ResourceNotFoundException", "Requested resource not found")


def length(n: int) -> tuple[str, str]:
    return ("SerializationException",
            f"Base64 encoded length is expected a multiple of 4 bytes but found: {n}")


def put(value: dict) -> tuple[str, str, dict]:
    body = {"TableName": MISSING, "Item": {"pk": {"S": "k"}, "b": value}}
    return f"PutItem {json.dumps(value)}", "PutItem", body


def get(value: dict) -> tuple[str, str, dict]:
    return f"GetItem key {json.dumps(value)}", "GetItem", {"TableName": MISSING,
                                                           "Key": {"pk": value}}


def query(value: dict, **extra) -> tuple[str, str, dict]:
    body = {"TableName": MISSING, "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": value}, **extra}
    return f"Query {json.dumps(value)} {json.dumps(extra)}", "Query", body


def statement(value: dict) -> tuple[str, str, dict]:
    body = {"Statement": f'SELECT * FROM "{MISSING}" WHERE pk = ?', "Parameters": [value]}
    return f"ExecuteStatement {json.dumps(value)}", "ExecuteStatement", body


# (request, the service's error code and message, None for no message).
ROWS = [
    (put({"B": "YWJj"}), NOT_FOUND),
    (put({"B": "YQ=="}), NOT_FOUND),
    (put({"B": "YWE="}), NOT_FOUND),
    (put({"B": "a"}), SHORT),
    (put({"B": "Y"}), SHORT),
    (put({"B": "!"}), SHORT),
    (put({"B": "!!"}), length(2)),
    (put({"B": "YR"}), length(2)),
    (put({"B": "\u00e9"}), length(2)),
    (put({"B": "YQ==\n"}), length(5)),
    (put({"B": "Y Q=="}), length(5)),
    (put({"B": "abcde"}), length(5)),
    (put({"B": "YWJjZ"}), length(5)),
    (put({"B": "YQ==="}), length(5)),
    (put({"B": "YQ==YQ"}), length(6)),
    (put({"B": "Y==="}), LAST),
    (put({"B": "YR=="}), LAST),
    (put({"B": "YWF="}), LAST),
    (put({"B": "YWJjYR=="}), LAST),
    (put({"B": "YWJj===="}), LAST),
    # Measured 2026-11-25: a "/" or a "+" as the last character ahead of the
    # padding.
    (put({"B": "//=="}), LAST),
    (put({"B": "++=="}), LAST),
    (put({"B": "!!!!"}), NO_MESSAGE),
    (put({"B": "=="}), NO_MESSAGE),
    (put({"B": "!!=="}), NO_MESSAGE),
    (put({"B": "YQ!="}), NO_MESSAGE),
    (put({"B": "YQ=A"}), NO_MESSAGE),
    (put({"B": "YWJ!"}), NO_MESSAGE),
    (put({"B": "YW!j"}), NO_MESSAGE),
    (put({"B": "!WJj"}), NO_MESSAGE),
    (put({"B": "-_-_"}), NO_MESSAGE),
    (put({"B": "YQ==YQ=="}), NO_MESSAGE),
    (put({"B": "YWJjYWJj!!!!"}), NO_MESSAGE),
    (put({"B": "\u00e9\u00e9"}), NO_MESSAGE),
    (put({"B": "\U0001F600"}), NO_MESSAGE),
    (put({"B": "YQ\u00e9"}), NO_MESSAGE),
    (put({"BS": ["YQ==", "!!"]}), length(2)),
    (put({"M": {"x": {"B": "!!"}}}), length(2)),
    (put({"L": [{"B": "!!"}]}), length(2)),
    (get({"B": "!!"}), length(2)),
    (query({"B": "!!"}), length(2)),
    (query({"B": "!!!!"}), NO_MESSAGE),
    (query({"B": "!!"}, Select="BOGUS"), length(2)),
    (query({"B": "!!", "N": "bob"}), length(2)),
    (query({"N": "bob", "B": "!!"}), length(2)),
    (statement({"B": "!!"}), length(2)),
    (statement({"B": "!!!!"}), NO_MESSAGE),
]


@pytest.mark.parametrize("row,expected", ROWS, ids=[row[0] for row, _ in ROWS])
def test_the_answer_names_what_does_not_decode(dynamodb_client, row, expected):
    _, operation, body = row
    status, payload = client_post(dynamodb_client, operation, body)
    assert status == 400, payload
    code = payload.get("__type", "").rsplit("#", 1)[-1]
    assert (code, payload.get("message", payload.get("Message"))) == expected, payload
