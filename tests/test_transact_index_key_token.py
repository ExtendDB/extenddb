# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A ClientRequestToken beside a key fault in TransactWriteItems, as measured on
DynamoDB (us-east-1, 2026-11-01, each sequence sent twice with a fresh token,
identical answers).

Table: pk S; GSI gsi_s on sidx S; GSI gsi_b on bidx B. Items r = {pk: r, n: N 5,
e: S ""} and bp = {pk: bp}; b0 and c are absent.

- The request-level checks come before the token check: a wrong-typed literal
  on an index key, a wrong-typed table key on a Put or a Delete, and a Put
  item's empty index key are answered even when the token belongs to another
  request (O rows), and they leave the token unused: another request with the
  token then succeeds (U rows).
- A fault found after them leaves the token used: an Update's empty literal,
  an empty or wrong-typed value written from the item, and a failed
  condition. Another request with the token then answers
  IdempotentParameterMismatchException and is not applied (O and U rows), and
  the same request with its token answers what it answered the first time (R
  rows).

A second measurement on 2026-11-05 (each sequence sent twice with a fresh
token and fresh keys, identical answers) found that every cancellation keeps
the token used against another request: an Update that grows the item past
400 KB (Q2a), an action that cannot apply (Q3a), and a failed condition on a
ConditionCheck, a Delete or a Put member (Q5a to Q5c). The same request sent
again runs again (Q3b). A request with the token of a cancelled request and
other ReturnConsumedCapacity answers the mismatch (Q7a). Items a = {pk: a}
and big = {pk: big, blob: S of 300 000 bytes} take part in these rows.

The mismatch is compared by its error code, as
tests/test_idempotency_account_scope.py does.

Run with EXTENDDB_TEST_ENDPOINT unset to assert the same against the service.
"""

from __future__ import annotations

import os
import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

TYPE_WORDING = (
    "One or more parameter values were invalid: Type mismatch for Index Key sidx "
    "Expected: S Actual: N IndexName: gsi_s"
)
PUT_KEY_TYPE_WORDING = "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N"
KEY_SCHEMA_WORDING = "The provided key element does not match the schema"
COMPUTED_WORDING = "The update expression attempted to update the secondary index key to unsupported type"
EMPTY_ITEM_WORDING = (
    "One or more parameter values are not valid. A value specified for a secondary index key "
    "is not supported. The AttributeValue for a key attribute cannot contain an empty string "
    "value. IndexName: gsi_s, IndexKey: sidx"
)
EMPTY_UPDATE_WORDING = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty string value."
)
CCF_WORDING = "The conditional request failed"
UPDATE_SIZE_WORDING = "Item size to update has exceeded the maximum allowed size"
ABSENT_OPERAND_WORDING = "The provided expression refers to an attribute that does not exist in the item"
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
MISMATCH = "IdempotentParameterMismatchException"

# UpdateItem and TransactWriteItems size an Update on the finished item on
# Cassandra and apply no item-size limit to updates on MongoDB (#818, #761),
# so the 400 KB row runs on the other backends and against the service only,
# as in tests/test_item_size_at_limit.py.
ON_DEFAULT_BACKENDS_ONLY = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_BACKEND", "") in {"mongodb", "cassandra"},
    reason="an Update is sized on the finished item (Cassandra) or unbounded (MongoDB): #818, #761",
)
# The fingerprint covers TransactItems only, where the service's covers
# ReturnConsumedCapacity too (#514).
XFAIL_ON_EXTENDDB_514 = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()),
    reason="#514: the token fingerprint does not cover ReturnConsumedCapacity",
    raises=AssertionError,
    strict=False,
)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """The measured table: pk S, GSI gsi_s on sidx S, GSI gsi_b on bidx B."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sidx", "AttributeType": "S"},
            {"AttributeName": "bidx", "AttributeType": "B"},
        ],
        key_schema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi_s",
                "KeySchema": [{"AttributeName": "sidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "gsi_b",
                "KeySchema": [{"AttributeName": "bidx", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    ) as name:
        yield name


ITEM_R = {"pk": {"S": "r"}, "n": {"N": "5"}, "e": {"S": ""}}
ITEM_BP = {"pk": {"S": "bp"}}
ITEM_A = {"pk": {"S": "a"}}
ITEM_BIG = {"pk": {"S": "big"}, "blob": {"S": "x" * 300_000}}
SEEDED = {"a": ITEM_A, "b0": None, "bp": ITEM_BP, "c": None, "r": ITEM_R}


@pytest.fixture(autouse=True)
def seeded(dynamodb_client, table):
    """Items r, bp and a present, b0 and c absent, before every case."""
    for item in (ITEM_R, ITEM_BP, ITEM_A):
        dynamodb_client.put_item(TableName=table, Item=item)
    for pk in ("b0", "c"):
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})


def _update(table: str, pk: str, expr: str, names: dict, values: dict | None = None, cond: str | None = None) -> dict:
    member = {
        "TableName": table,
        "Key": {"pk": {"S": pk}},
        "UpdateExpression": expr,
        "ExpressionAttributeNames": names,
    }
    if values:
        member["ExpressionAttributeValues"] = values
    if cond:
        member["ConditionExpression"] = cond
    return {"Update": member}


def faults(table: str) -> dict:
    """The one-member requests of the measurement, by fault."""
    return {
        # Update b0 SET sidx = :n, :n = N 1: a wrong-typed literal on an index key.
        "lit-type": _update(table, "b0", "SET #s = :n", {"#s": "sidx"}, {":n": {"N": "1"}}),
        # Put {pk: N 1} and Delete {pk: N 1}: a wrong-typed table key.
        "key-type-put": {"Put": {"TableName": table, "Item": {"pk": {"N": "1"}}}},
        "key-type-del": {"Delete": {"TableName": table, "Key": {"pk": {"N": "1"}}}},
        # Put {pk: c, sidx: S ""}: an empty index key in the item.
        "put-empty": {"Put": {"TableName": table, "Item": {"pk": {"S": "c"}, "sidx": {"S": ""}}}},
        # Update b0 SET sidx = :e, :e = S "": an empty literal on an index key.
        "lit-empty": _update(table, "b0", "SET #s = :e", {"#s": "sidx"}, {":e": {"S": ""}}),
        # Update r SET sidx = n: the stored N 5 written to the string key.
        "comp-type": _update(table, "r", "SET #s = #n", {"#s": "sidx", "#n": "n"}),
        # Update r SET sidx = e: the stored empty string written to the key.
        "comp-empty": _update(table, "r", "SET #s = #e", {"#s": "sidx", "#e": "e"}),
        # Update bp with attribute_not_exists(pk): the condition fails.
        "ccf": _update(
            table, "bp", "SET #v = :v", {"#v": "v", "#p": "pk"}, {":v": {"S": "x"}}, "attribute_not_exists(#p)"
        ),
        # 2026-11-05. Update big SET blob2 = :b: 300 000 and 150 000 bytes,
        # past 400 KB.
        "size": _update(table, "big", "SET #b = :b", {"#b": "blob2"}, {":b": {"S": "y" * 150_000}}),
        # Update a SET v = v + :one: a has no v, so the action cannot apply.
        "cannot-apply": _update(table, "a", "SET #v = #v + :one", {"#v": "v"}, {":one": {"N": "1"}}),
        # attribute_not_exists(pk) on bp, which exists, on a ConditionCheck,
        # a Delete and a Put member.
        "check-ccf": {
            "ConditionCheck": {
                "TableName": table,
                "Key": {"pk": {"S": "bp"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"},
            }
        },
        "delete-ccf": {
            "Delete": {
                "TableName": table,
                "Key": {"pk": {"S": "bp"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"},
            }
        },
        "put-ccf": {
            "Put": {
                "TableName": table,
                "Item": {"pk": {"S": "bp"}, "v": {"S": "x"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"},
            }
        },
    }


# The answer to each fault alone: a tuple of (Code, Message) cancellation
# reasons, or the message of a top-level ValidationException.
ANSWERS = {
    "lit-type": (("ValidationError", TYPE_WORDING),),
    "key-type-put": (("ValidationError", PUT_KEY_TYPE_WORDING),),
    "key-type-del": (("ValidationError", KEY_SCHEMA_WORDING),),
    "put-empty": EMPTY_ITEM_WORDING,
    "lit-empty": EMPTY_UPDATE_WORDING,
    "comp-type": (("ValidationError", COMPUTED_WORDING),),
    "comp-empty": EMPTY_UPDATE_WORDING,
    "ccf": (("ConditionalCheckFailed", CCF_WORDING),),
    "size": (("ValidationError", UPDATE_SIZE_WORDING),),
    "cannot-apply": (("ValidationError", ABSENT_OPERAND_WORDING),),
    "check-ccf": (("ConditionalCheckFailed", CCF_WORDING),),
    "delete-ccf": (("ConditionalCheckFailed", CCF_WORDING),),
    "put-ccf": (("ConditionalCheckFailed", CCF_WORDING),),
}
# Answered before the token is checked; the token stays unused.
BEFORE_TOKEN = ["lit-type", "key-type-put", "key-type-del", "put-empty"]
# Found after the token is checked; the token stays used.
AFTER_TOKEN = ["lit-empty", "comp-type", "comp-empty", "ccf"]
# The cancellations of the 2026-11-05 measurement.
CANCELLATIONS = [
    pytest.param("size", id="Q2a", marks=ON_DEFAULT_BACKENDS_ONLY),
    pytest.param("cannot-apply", id="Q3a"),
    pytest.param("check-ccf", id="Q5a"),
    pytest.param("delete-ccf", id="Q5b"),
    pytest.param("put-ccf", id="Q5c"),
]


def _token() -> str:
    return f"t416-{uuid.uuid4().hex[:30]}"


def _send(client, items: list, token: str, **params):
    """Send TransactWriteItems with the token (and `params`) and return the
    answer: "OK", a tuple of (Code, Message) reasons, a ValidationException
    message, or the error code of anything else."""
    try:
        client.transact_write_items(TransactItems=items, ClientRequestToken=token, **params)
    except ClientError as exc:
        resp = exc.response
        code = resp["Error"]["Code"]
        if code == "TransactionCanceledException":
            reasons = tuple((r["Code"], r.get("Message")) for r in resp["CancellationReasons"])
            assert resp["Error"]["Message"] == f"{CANCELLED} [{', '.join(c for c, _ in reasons)}]"
            return reasons
        if code == "ValidationException":
            assert "CancellationReasons" not in resp
            return resp["Error"]["Message"]
        return code
    return "OK"


def _item(client, table: str, pk: str):
    return client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True).get("Item")


def _seeded_unchanged(client, table: str) -> bool:
    return {pk: _item(client, table, pk) for pk in SEEDED} == SEEDED


def _fresh_put(table: str) -> tuple[str, dict]:
    pk = f"t416-{uuid.uuid4().hex[:12]}"
    return pk, {"Put": {"TableName": table, "Item": {"pk": {"S": pk}}}}


@pytest.mark.parametrize("fault", BEFORE_TOKEN + AFTER_TOKEN, ids=[f"O-{f}" for f in BEFORE_TOKEN + AFTER_TOKEN])
def test_a_token_a_committed_request_used(dynamodb_client, table, fault):
    """A Put commits with the token, then a request with the fault and the same
    token: the request-level faults answer themselves, the later ones the
    mismatch."""
    token = _token()
    pk, put = _fresh_put(table)
    try:
        assert _send(dynamodb_client, [put], token) == "OK"
        expected = ANSWERS[fault] if fault in BEFORE_TOKEN else MISMATCH
        assert _send(dynamodb_client, [faults(table)[fault]], token) == expected
        assert _seeded_unchanged(dynamodb_client, table)
    finally:
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})


@pytest.mark.parametrize("fault", BEFORE_TOKEN + AFTER_TOKEN, ids=[f"U-{f}" for f in BEFORE_TOKEN + AFTER_TOKEN])
def test_a_token_a_cancelled_request_used(dynamodb_client, table, fault):
    """A request with the fault is cancelled with the token, then a Put with
    the same token: it succeeds after a request-level fault and answers the
    mismatch, writing nothing, after a later fault."""
    token = _token()
    pk, put = _fresh_put(table)
    try:
        assert _send(dynamodb_client, [faults(table)[fault]], token) == ANSWERS[fault]
        if fault in BEFORE_TOKEN:
            assert _send(dynamodb_client, [put], token) == "OK"
            assert _item(dynamodb_client, table, pk) is not None
        else:
            assert _send(dynamodb_client, [put], token) == MISMATCH
            assert _item(dynamodb_client, table, pk) is None
        assert _seeded_unchanged(dynamodb_client, table)
    finally:
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})


@pytest.mark.parametrize(
    "fault", AFTER_TOKEN + ["cannot-apply"], ids=[f"R-{f}" for f in AFTER_TOKEN] + ["Q3b"]
)
def test_the_same_cancelled_request_with_its_token(dynamodb_client, table, fault):
    """A request cancelled after the request-level checks, sent again with its
    token, answers what it answered the first time."""
    token = _token()
    request = [faults(table)[fault]]
    assert _send(dynamodb_client, request, token) == ANSWERS[fault]
    assert _send(dynamodb_client, request, token) == ANSWERS[fault]
    assert _seeded_unchanged(dynamodb_client, table)


@pytest.mark.parametrize("fault", CANCELLATIONS)
def test_a_cancellation_keeps_the_token_used(dynamodb_client, table, fault):
    """A request is cancelled with the token, then a Put with the same token
    answers the mismatch and writes nothing, whatever cancelled the request."""
    token = _token()
    pk, put = _fresh_put(table)
    if fault == "size":
        dynamodb_client.put_item(TableName=table, Item=ITEM_BIG)
    try:
        assert _send(dynamodb_client, [faults(table)[fault]], token) == ANSWERS[fault]
        assert _send(dynamodb_client, [put], token) == MISMATCH
        assert _item(dynamodb_client, table, pk) is None
        assert _seeded_unchanged(dynamodb_client, table)
        if fault == "size":
            assert _item(dynamodb_client, table, "big") == ITEM_BIG
    finally:
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})
        if fault == "size":
            dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": "big"}})


@XFAIL_ON_EXTENDDB_514
def test_a_cancelled_token_with_other_return_parameters(dynamodb_client, table):
    """Q7a: the cancelled request sent again with ReturnConsumedCapacity is
    another request for the token."""
    token = _token()
    request = [faults(table)["ccf"]]
    assert _send(dynamodb_client, request, token) == ANSWERS["ccf"]
    assert _send(dynamodb_client, request, token, ReturnConsumedCapacity="TOTAL") == MISMATCH
    assert _seeded_unchanged(dynamodb_client, table)
