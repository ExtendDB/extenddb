# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A B value that the standard base64 decoder refuses: a B, a BS member, a
value in an M or an L, a key, an ExpressionAttributeValues entry and a
PartiQL parameter.

The service reads such a value a second time, ahead of the table lookup,
and refuses it with a SerializationException or reads bytes from it. One
byte is too short, and a length in UTF-8 bytes that is not a multiple of 4
is named, except for "==", which gets no message. Up to two "=" at the end
are the padding. In the last group of four, a byte from "(" to DEL outside
the alphabet reads as all bits set, so Y=Q= is the bytes FF F4, and the
groups ahead of it must hold base64 characters only. A last character with
bits past the last byte is invalid, and any other value gets no message.
An item and an expression attribute value may leave the "=" padding out or
cut it short; a PartiQL parameter may not. Every answer below was measured
against the service in us-east-1 on 2026-11-20 and 2026-11-21, each request
twice. The table does not exist, so a value the service reads gets
ResourceNotFoundException. On 2026-11-21 each value with "=" or another
byte inside, or with more "=" at the end than the padding, went out as
PutItem both to this missing table and to a table that exists. Each got
the same answer from both, apart from ResourceNotFoundException for a value
the service reads. boto3 encodes a binary value itself, so the bodies go
out as written.

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
    (put({"B": "YQ"}), NOT_FOUND),
    (put({"B": "YQ="}), NOT_FOUND),
    (put({"B": "YWJjZA"}), NOT_FOUND),
    (put({"B": "YWJjZA="}), NOT_FOUND),
    (put({"B": "YWJ"}), length(3)),
    (query({"B": "YQ"}), NOT_FOUND),
    (statement({"B": "YQ"}), length(2)),
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
    # Measured 2026-11-21: "=" or another byte outside the alphabet inside a
    # value.
    (query({"B": "Y=Q="}), NOT_FOUND),
    (statement({"B": "Y=Q="}), NOT_FOUND),
    (put({"B": "=YQ="}), NOT_FOUND),
    (put({"B": ",YQ="}), NOT_FOUND),
    (put({"B": "@YQ="}), NOT_FOUND),
    (put({"B": "`YQ="}), NOT_FOUND),
    (put({"B": "=A=="}), NOT_FOUND),
    (put({"B": "=AA="}), NOT_FOUND),
    (put({"B": "Y=Q="}), NOT_FOUND),
    (put({"B": "A=Q="}), NOT_FOUND),
    (put({"B": "==Q="}), NOT_FOUND),
    (put({"B": "Y(Q="}), NOT_FOUND),
    (put({"B": "Y)Q="}), NOT_FOUND),
    (put({"B": "Y*Q="}), NOT_FOUND),
    (put({"B": "Y,Q="}), NOT_FOUND),
    (put({"B": "Y-Q="}), NOT_FOUND),
    (put({"B": "Y.Q="}), NOT_FOUND),
    (put({"B": "Y:Q="}), NOT_FOUND),
    (put({"B": "Y;Q="}), NOT_FOUND),
    (put({"B": "Y<Q="}), NOT_FOUND),
    (put({"B": "Y>Q="}), NOT_FOUND),
    (put({"B": "Y?Q="}), NOT_FOUND),
    (put({"B": "Y@Q="}), NOT_FOUND),
    (put({"B": "Y[Q="}), NOT_FOUND),
    (put({"B": "Y\\Q="}), NOT_FOUND),
    (put({"B": "Y]Q="}), NOT_FOUND),
    (put({"B": "Y^Q="}), NOT_FOUND),
    (put({"B": "Y_Q="}), NOT_FOUND),
    (put({"B": "Y`Q="}), NOT_FOUND),
    (put({"B": "Y{Q="}), NOT_FOUND),
    (put({"B": "Y|Q="}), NOT_FOUND),
    (put({"B": "Y}Q="}), NOT_FOUND),
    (put({"B": "Y~Q="}), NOT_FOUND),
    (put({"B": "Y\x7fQ="}), NOT_FOUND),
    (put({"B": "A=A="}), NOT_FOUND),
    (put({"B": "/=A="}), NOT_FOUND),
    (put({"B": "==A="}), NOT_FOUND),
    (put({"B": "YWJj=YQ="}), NOT_FOUND),
    (put({"B": "YWJj,YQ="}), NOT_FOUND),
    (put({"B": "YWJjY=Q="}), NOT_FOUND),
    (put({"B": "YWJjY-Q="}), NOT_FOUND),
    (put({"B": "YWJjYWJjY=Q="}), NOT_FOUND),
    (put({"B": "=Y"}), length(2)),
    (put({"B": "Y-"}), length(2)),
    (put({"B": "=YQ"}), length(3)),
    (put({"B": "Y=Q"}), length(3)),
    (put({"B": "Y-Q"}), length(3)),
    (put({"B": "YW=Jj"}), length(5)),
    (put({"B": "Y=Q=\n"}), length(5)),
    (put({"B": "\u00e9YQ="}), length(5)),
    (put({"B": "YWJjY="}), length(6)),
    (put({"B": "YWJj=YQ"}), length(7)),
    (put({"B": "YWJjY=Q"}), length(7)),
    (put({"B": "=Y=="}), LAST),
    (put({"B": "YW=="}), LAST),
    (put({"B": "A=B="}), LAST),
    (put({"B": "Y=R="}), LAST),
    (put({"B": "Y,=="}), LAST),
    (put({"B": "A,=="}), LAST),
    (put({"B": "A@=="}), LAST),
    (put({"B": "YQ,="}), LAST),
    (put({"B": "YQ@="}), LAST),
    (put({"B": "=YQA"}), NO_MESSAGE),
    (put({"B": "=AAA"}), NO_MESSAGE),
    (put({"B": "=///"}), NO_MESSAGE),
    (put({"B": "Y=QA"}), NO_MESSAGE),
    (put({"B": "A=AA"}), NO_MESSAGE),
    (put({"B": "A=//"}), NO_MESSAGE),
    (put({"B": "==QA"}), NO_MESSAGE),
    (put({"B": "==AA"}), NO_MESSAGE),
    (put({"B": "YQ=Q"}), NO_MESSAGE),
    (put({"B": "AA=A"}), NO_MESSAGE),
    (put({"B": "Y,QA"}), NO_MESSAGE),
    (put({"B": "YW-j"}), NO_MESSAGE),
    (put({"B": "Y=QAYWJj"}), NO_MESSAGE),
    (put({"B": "=YQAYWJj"}), NO_MESSAGE),
    (put({"B": "Y=QA=YQ="}), NO_MESSAGE),
    (put({"B": "YWJjY=QA"}), NO_MESSAGE),
    (put({"B": "YWJj=YQA"}), NO_MESSAGE),
    (put({"B": "Y=Q=YWJj"}), NO_MESSAGE),
    (put({"B": "Y-QAYWJj"}), NO_MESSAGE),
    (put({"B": "YWJjY,QA"}), NO_MESSAGE),
    (put({"B": "Y Q="}), NO_MESSAGE),
    (put({"B": "Y!Q="}), NO_MESSAGE),
    (put({"B": "Y\"Q="}), NO_MESSAGE),
    (put({"B": "Y#Q="}), NO_MESSAGE),
    (put({"B": "Y$Q="}), NO_MESSAGE),
    (put({"B": "Y%Q="}), NO_MESSAGE),
    (put({"B": "Y&Q="}), NO_MESSAGE),
    (put({"B": "Y'Q="}), NO_MESSAGE),
    (put({"B": "!YQ="}), NO_MESSAGE),
    (put({"B": "Y\x00Q="}), NO_MESSAGE),
    (put({"B": "Y\tQ="}), NO_MESSAGE),
    (put({"B": "Y\nQ="}), NO_MESSAGE),
    (put({"B": "Y\u00e9="}), NO_MESSAGE),
    # Measured 2026-11-21: more "=" at the end than the padding.
    (put({"B": "YQ======"}), NO_MESSAGE),
    (put({"B": "========"}), NO_MESSAGE),
    (put({"B": "YWJjYQ======"}), NO_MESSAGE),
    (put({"B": "Y======="}), NO_MESSAGE),
    (put({"B": "AAAA===="}), LAST),
    (put({"B": "YWJjYWJj===="}), LAST),
    (put({"B": "YQ====="}), length(7)),
]


@pytest.mark.parametrize("row,expected", ROWS, ids=[row[0] for row, _ in ROWS])
def test_the_answer_names_what_does_not_decode(dynamodb_client, row, expected):
    _, operation, body = row
    status, payload = client_post(dynamodb_client, operation, body)
    assert status == 400, payload
    code = payload.get("__type", "").rsplit("#", 1)[-1]
    assert (code, payload.get("message", payload.get("Message"))) == expected, payload
