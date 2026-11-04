"""TransactWriteItems with a bad secondary-index key on one member and another
fault on the other, in both member orders, as measured on DynamoDB (us-east-1,
2026-10-30, two tables, every request sent twice with identical answers).

Table: pk S; GSI gsi_s on sidx S; GSI gsi_b on bidx B. Items: a = {pk: a, n: N 5},
bp = {pk: bp}; b0 and c are absent. Three kinds of index-key fault appear:

- a wrong type the Update computes from the stored item (SET sidx = n): a
  ValidationError at that member with the computed-value wording, and every
  other member keeps its own outcome (A2: [ValidationError,
  ConditionalCheckFailed]);
- a wrong-typed value supplied in the request, in a Put item or as an Update's
  literal operand: ValidationError at that member and None at every other
  member, ahead of a failed condition (E1) and ahead of a computed wrong type
  (F1, F2);
- an empty value supplied in the request, in a Put item or as a literal
  operand: a top-level ValidationException with no CancellationReasons, in
  either member order, ahead of a computed wrong type (B1, C1) and ahead of a
  failed condition (D1).

Nothing is written in any case. S1 to S5 are the single-request controls for
the same values.

Rows G to J were measured on 2026-10-31 (us-east-1, one table of the same
shape, 337 requests, every request sent twice with identical answers), with
two more items: r = {pk: r, n: N 5, l: L [S x], e: S "", s: S x} and d0
absent. They put the empty and wrong-typed values supplied in the request
against each other:

- a Put item's empty index key against a wrong type supplied in the request
  (a literal, list_append of literals, ADD, DELETE, arithmetic of literals, a
  wrong-typed table key): the first in request order decides (G rows);
- a Put item's empty index key against an Update member's empty value, a
  literal or one the statement writes from the stored item or from
  if_not_exists: the Put item's message, in either order (H rows);
- an Update member's empty literal ahead of a wrong-typed literal on another
  member: the wrong type (J1 to J3); an empty value the statement writes
  from the stored item against a failed condition: the empty key (J4).

The same measurement typed list_append by its result: SET sidx =
list_append(l, :l), which reads the item, is a wrong type supplied in the
request beside every partner (L rows) and ahead of the condition on
UpdateItem (la1, la2). A third measurement on 2026-10-31 (15 requests, each
sent twice) found an Update's empty index-key literal reported on the
statement, ahead of the member's own failing condition and of a wrong type
computed from the item in the same statement, on UpdateItem (s1 to s9) and
in TransactWriteItems (t1 to t6); between two such members the first in
request order decides (K1, K1r). The rows with the list_append member second
(L4r, L9r, L10r) answer the first member's fault.

A fourth measurement on 2026-11-01 (each request sent twice) put an Update's
empty literal against an empty value another member writes from the item:
the first empty value in request order decides, literal or not (U3a, U3b,
U3c); within one statement the literal comes first (U3f); and a member whose
own condition fails writes nothing, so its empty if_not_exists fallback is
never reported (U1a to U1c). Two empty literals of different kinds in one
statement had no fixed order on the service, so no row here has that shape.

Run with EXTENDDB_TEST_ENDPOINT unset to assert the same against the service.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

COMPUTED_WORDING = "The update expression attempted to update the secondary index key to unsupported type"
TYPE_WORDING = (
    "One or more parameter values were invalid: Type mismatch for Index Key sidx "
    "Expected: S Actual: N IndexName: gsi_s"
)
EMPTY_UPDATE_WORDING = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty string value."
)
EMPTY_ITEM_WORDING = (
    "One or more parameter values are not valid. A value specified for a secondary index "
    "key is not supported. The AttributeValue for a key attribute cannot contain an empty "
    "string value. IndexName: gsi_s, IndexKey: sidx"
)
TYPE_WORDING_L = (
    "One or more parameter values were invalid: Type mismatch for Index Key sidx "
    "Expected: S Actual: L IndexName: gsi_s"
)
TYPE_WORDING_SS = (
    "One or more parameter values were invalid: Type mismatch for Index Key sidx "
    "Expected: S Actual: SS IndexName: gsi_s"
)
KEY_SCHEMA_WORDING = "The provided key element does not match the schema"
EMPTY_UPDATE_WORDING_B = (
    "One or more parameter values are not valid. The update expression attempted to "
    "update a secondary index key to a value that is not supported. The AttributeValue "
    "for a key attribute cannot contain an empty binary value."
)
CANCELLED = "Transaction cancelled, please refer cancellation reasons for specific reasons"
CCF_WORDING = "The conditional request failed"


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


ITEM_A = {"pk": {"S": "a"}, "n": {"N": "5"}}
ITEM_BP = {"pk": {"S": "bp"}}
ITEM_R = {"pk": {"S": "r"}, "n": {"N": "5"}, "l": {"L": [{"S": "x"}]}, "e": {"S": ""}, "s": {"S": "x"}}


@pytest.fixture(autouse=True)
def seeded(dynamodb_client, table):
    """Items a, bp and r present, b0, c and d0 absent, before every case."""
    for item in (ITEM_A, ITEM_BP, ITEM_R):
        dynamodb_client.put_item(TableName=table, Item=item)
    for pk in ("b0", "c", "d0"):
        dynamodb_client.delete_item(TableName=table, Key={"pk": {"S": pk}})


def _update(
    table: str,
    key: str | dict,
    expr: str,
    names: dict,
    values: dict | None = None,
    cond: str | None = None,
) -> dict:
    member = {
        "TableName": table,
        "Key": {"pk": key if isinstance(key, dict) else {"S": key}},
        "UpdateExpression": expr,
        "ExpressionAttributeNames": names,
    }
    if values:
        member["ExpressionAttributeValues"] = values
    if cond:
        member["ConditionExpression"] = cond
    return {"Update": member}


def _put(table: str, item: dict) -> dict:
    return {"Put": {"TableName": table, "Item": item}}


def members(table: str) -> dict:
    """The member shapes of the measurement, by name."""
    return {
        # Update a SET sidx = n: sidx (S) gets the stored N 5.
        "set_sidx_n": _update(table, "a", "SET #s = #n", {"#s": "sidx", "#n": "n"}),
        # Update a SET bidx = n: bidx (B) gets the stored N 5.
        "set_bidx_n": _update(table, "a", "SET #b = #n", {"#b": "bidx", "#n": "n"}),
        # Update b0 (absent) with attribute_not_exists(pk): the condition holds.
        "cond_absent": _update(
            table, "b0", "SET #v = :v", {"#v": "v", "#p": "pk"}, {":v": {"S": "x"}}, "attribute_not_exists(#p)"
        ),
        # Update bp (present) with attribute_not_exists(pk): the condition fails.
        "cond_present": _update(
            table, "bp", "SET #v = :v", {"#v": "v", "#p": "pk"}, {":v": {"S": "x"}}, "attribute_not_exists(#p)"
        ),
        # Update b0 SET sidx = :e, :e = S "": an empty literal on an index key.
        "set_sidx_empty": _update(table, "b0", "SET #s = :e", {"#s": "sidx"}, {":e": {"S": ""}}),
        # Put {pk: c, sidx: S ""}: an empty index key in the item.
        "put_empty": _put(table, {"pk": {"S": "c"}, "sidx": {"S": ""}}),
        # Update a SET sidx = :n, :n = N 1: a wrong-typed literal on an index key.
        "set_sidx_lit_n": _update(table, "a", "SET #s = :n", {"#s": "sidx"}, {":n": {"N": "1"}}),
        # Update b0 SET sidx = :n, :n = N 1: the same on an absent item.
        "set_b0_lit_n": _update(table, "b0", "SET #s = :n", {"#s": "sidx"}, {":n": {"N": "1"}}),
        # Put {pk: c, sidx: N 1}: a wrong-typed index key in the item.
        "put_wrong": _put(table, {"pk": {"S": "c"}, "sidx": {"N": "1"}}),
        # Update d0 SET sidx = :n, :n = N 1: a wrong-typed literal on an absent item.
        "set_d0_lit_n": _update(table, "d0", "SET #s = :n", {"#s": "sidx"}, {":n": {"N": "1"}}),
        # Update r SET sidx = list_append(:l, :l): a list built from literals.
        "set_sidx_list_lit": _update(
            table, "r", "SET #s = list_append(:l, :l)", {"#s": "sidx"}, {":l": {"L": [{"S": "y"}]}}
        ),
        # Update r ADD sidx :one: a number added onto a string key.
        "add_sidx_one": _update(table, "r", "ADD #s :one", {"#s": "sidx"}, {":one": {"N": "1"}}),
        # Update r DELETE sidx :ss: a set removed from a string key.
        "delete_sidx_ss": _update(table, "r", "DELETE #s :ss", {"#s": "sidx"}, {":ss": {"SS": ["a"]}}),
        # Update r SET sidx = :one + :one: arithmetic of literals.
        "set_sidx_sum_lit": _update(table, "r", "SET #s = :one + :one", {"#s": "sidx"}, {":one": {"N": "1"}}),
        # Update with Key {pk: N 1}: a wrong-typed table key.
        "bad_table_key": _update(table, {"N": "1"}, "SET #v = :v", {"#v": "v"}, {":v": {"S": "x"}}),
        # Update b0 SET bidx = :e, :e = B "": an empty binary literal on an index key.
        "set_bidx_empty": _update(table, "b0", "SET #b = :e", {"#b": "bidx"}, {":e": {"B": b""}}),
        # Update r SET sidx = e: the stored empty string written to an index key.
        "set_sidx_stored_empty": _update(table, "r", "SET #s = #e", {"#s": "sidx", "#e": "e"}),
        # Update r SET sidx = if_not_exists(nope, :e): the empty fallback written.
        "set_sidx_fallback_empty": _update(
            table, "r", "SET #s = if_not_exists(#x, :e)", {"#s": "sidx", "#x": "nope"}, {":e": {"S": ""}}
        ),
        # Update r SET sidx = list_append(l, :l): a list built from the stored l.
        "set_sidx_list_ref": _update(
            table, "r", "SET #s = list_append(#l, :l)", {"#s": "sidx", "#l": "l"}, {":l": {"L": [{"S": "y"}]}}
        ),
        # Update b0 SET v = v + :one: arithmetic on an attribute b0 lacks.
        "cannot_apply": _update(table, "b0", "SET #v = #v + :one", {"#v": "v"}, {":one": {"N": "1"}}),
        # ConditionCheck bp with attribute_not_exists(pk): the condition fails.
        "check_present": {
            "ConditionCheck": {
                "TableName": table,
                "Key": {"pk": {"S": "bp"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"},
            }
        },
        # Delete bp with attribute_not_exists(pk): the condition fails.
        "delete_present": {
            "Delete": {
                "TableName": table,
                "Key": {"pk": {"S": "bp"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"},
            }
        },
        # Update r SET sidx = n, bidx = :eb: a computed wrong type and an empty
        # binary literal in one statement.
        "set_sidx_n_bidx_empty": _update(
            table, "r", "SET #s = #n, #b = :eb", {"#s": "sidx", "#n": "n", "#b": "bidx"}, {":eb": {"B": b""}}
        ),
        # The 2026-10-31 statement rows on r, each with the failing condition
        # attribute_not_exists(pk) where named so, and a plain Update of a.
        "r_sidx_empty_failing": _update(
            table, "r", "SET #s = :e", {"#s": "sidx", "#p": "pk"}, {":e": {"S": ""}}, "attribute_not_exists(#p)"
        ),
        "r_bidx_empty_failing": _update(
            table, "r", "SET #b = :eb", {"#b": "bidx", "#p": "pk"}, {":eb": {"B": b""}}, "attribute_not_exists(#p)"
        ),
        "r_sidx_n_bidx_empty_failing": _update(
            table,
            "r",
            "SET #s = #n, #b = :eb",
            {"#s": "sidx", "#n": "n", "#b": "bidx", "#p": "pk"},
            {":eb": {"B": b""}},
            "attribute_not_exists(#p)",
        ),
        "r_sidx_empty_bidx_pk": _update(
            table, "r", "SET #s = :e, #b = #p", {"#s": "sidx", "#b": "bidx", "#p": "pk"}, {":e": {"S": ""}}
        ),
        "plain_a": _update(table, "a", "SET #v = :v", {"#v": "v"}, {":v": {"S": "x"}}),
        # The 2026-11-01 rows: Update r SET sidx = e, bidx = :eb, the stored
        # empty string and an empty binary literal in one statement; and the
        # empty if_not_exists fallback with the failing condition.
        "set_sidx_e_bidx_empty": _update(
            table, "r", "SET #s = #e, #b = :eb", {"#s": "sidx", "#e": "e", "#b": "bidx"}, {":eb": {"B": b""}}
        ),
        "r_fallback_empty_failing": _update(
            table,
            "r",
            "SET #s = if_not_exists(#x, :e)",
            {"#s": "sidx", "#x": "nope", "#p": "pk"},
            {":e": {"S": ""}},
            "attribute_not_exists(#p)",
        ),
    }


# Cancellation reasons by position, as (Code, Message).
VE_COMPUTED = ("ValidationError", COMPUTED_WORDING)
VE_TYPE = ("ValidationError", TYPE_WORDING)
VE_TYPE_L = ("ValidationError", TYPE_WORDING_L)
CCF = ("ConditionalCheckFailed", CCF_WORDING)
NONE = ("None", None)

# Each row: id, member names in request order, expected answer. A tuple of
# reasons is a TransactionCanceledException with those CancellationReasons; a
# string is a top-level ValidationException with that message.
ROWS = [
    ("A1", ["set_sidx_n", "cond_absent"], (VE_COMPUTED, NONE)),
    ("A2", ["set_sidx_n", "cond_present"], (VE_COMPUTED, CCF)),
    ("A2r", ["cond_present", "set_sidx_n"], (CCF, VE_COMPUTED)),
    ("B1", ["set_sidx_n", "set_sidx_empty"], EMPTY_UPDATE_WORDING),
    ("B1r", ["set_sidx_empty", "set_sidx_n"], EMPTY_UPDATE_WORDING),
    ("C1", ["put_empty", "set_bidx_n"], EMPTY_ITEM_WORDING),
    ("C1r", ["set_bidx_n", "put_empty"], EMPTY_ITEM_WORDING),
    ("D1", ["cond_present", "set_sidx_empty"], EMPTY_UPDATE_WORDING),
    ("E1", ["set_sidx_lit_n", "cond_present"], (VE_TYPE, NONE)),
    ("E1r", ["cond_present", "set_sidx_lit_n"], (NONE, VE_TYPE)),
    ("E2", ["put_wrong", "cond_present"], (VE_TYPE, NONE)),
    ("E2r", ["cond_present", "put_wrong"], (NONE, VE_TYPE)),
    ("F1", ["set_sidx_n", "put_wrong"], (NONE, VE_TYPE)),
    ("F1r", ["put_wrong", "set_sidx_n"], (VE_TYPE, NONE)),
    ("F2", ["set_sidx_n", "set_b0_lit_n"], (NONE, VE_TYPE)),
    ("F2r", ["set_b0_lit_n", "set_sidx_n"], (VE_TYPE, NONE)),
    # 2026-10-31. A Put item's empty index key against a wrong type supplied
    # in the request: the first in request order decides.
    ("G1", ["put_empty", "set_d0_lit_n"], EMPTY_ITEM_WORDING),
    ("G1r", ["set_d0_lit_n", "put_empty"], (VE_TYPE, NONE)),
    ("G2", ["put_empty", "set_sidx_list_lit"], EMPTY_ITEM_WORDING),
    ("G2r", ["set_sidx_list_lit", "put_empty"], (("ValidationError", TYPE_WORDING_L), NONE)),
    ("G3", ["put_empty", "add_sidx_one"], EMPTY_ITEM_WORDING),
    ("G3r", ["add_sidx_one", "put_empty"], (VE_TYPE, NONE)),
    ("G4", ["put_empty", "delete_sidx_ss"], EMPTY_ITEM_WORDING),
    ("G5", ["put_empty", "set_sidx_sum_lit"], EMPTY_ITEM_WORDING),
    ("G6", ["put_empty", "bad_table_key"], EMPTY_ITEM_WORDING),
    ("G6r", ["bad_table_key", "put_empty"], (("ValidationError", KEY_SCHEMA_WORDING), NONE)),
    # A Put item's empty index key against an Update member's empty value:
    # the Put item's message in either order.
    ("H1", ["set_sidx_empty", "put_empty"], EMPTY_ITEM_WORDING),
    ("H1r", ["put_empty", "set_sidx_empty"], EMPTY_ITEM_WORDING),
    ("H2", ["set_bidx_empty", "put_empty"], EMPTY_ITEM_WORDING),
    ("H3", ["set_sidx_stored_empty", "put_empty"], EMPTY_ITEM_WORDING),
    ("H4", ["set_sidx_fallback_empty", "put_empty"], EMPTY_ITEM_WORDING),
    # An Update member's empty literal against a wrong-typed literal
    # elsewhere: the wrong type. A written empty value against a failed
    # condition: the empty key.
    ("J1", ["set_sidx_empty", "set_sidx_list_lit"], (NONE, ("ValidationError", TYPE_WORDING_L))),
    ("J2", ["set_sidx_empty", "add_sidx_one"], (NONE, VE_TYPE)),
    ("J3", ["set_sidx_empty", "delete_sidx_ss"], (NONE, ("ValidationError", TYPE_WORDING_SS))),
    ("J4", ["set_sidx_stored_empty", "cond_present"], EMPTY_UPDATE_WORDING),
    # list_append that reads the item is typed by its result, a list, and
    # ranks as a wrong type supplied in the request beside every partner.
    ("L1", ["set_sidx_list_ref"], (VE_TYPE_L,)),
    ("L2", ["set_sidx_list_ref", "cond_present"], (VE_TYPE_L, NONE)),
    ("L2r", ["cond_present", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L3", ["set_sidx_list_ref", "set_sidx_empty"], (VE_TYPE_L, NONE)),
    ("L3r", ["set_sidx_empty", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L4", ["set_sidx_list_ref", "set_d0_lit_n"], (VE_TYPE_L, NONE)),
    ("L4r", ["set_d0_lit_n", "set_sidx_list_ref"], (VE_TYPE, NONE)),
    ("L5", ["set_sidx_list_ref", "set_sidx_n"], (VE_TYPE_L, NONE)),
    ("L5r", ["set_sidx_n", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L6", ["set_sidx_list_ref", "cannot_apply"], (VE_TYPE_L, NONE)),
    ("L6r", ["cannot_apply", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L7", ["set_sidx_list_ref", "check_present"], (VE_TYPE_L, NONE)),
    ("L7r", ["check_present", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L8", ["set_sidx_list_ref", "delete_present"], (VE_TYPE_L, NONE)),
    ("L8r", ["delete_present", "set_sidx_list_ref"], (NONE, VE_TYPE_L)),
    ("L9", ["set_sidx_list_ref", "put_empty"], (VE_TYPE_L, NONE)),
    ("L9r", ["put_empty", "set_sidx_list_ref"], EMPTY_ITEM_WORDING),
    ("L10", ["set_sidx_list_ref", "put_wrong"], (VE_TYPE_L, NONE)),
    ("L10r", ["put_wrong", "set_sidx_list_ref"], (VE_TYPE, NONE)),
    # An Update member's empty literal is checked before any member runs,
    # below the faults above: ahead of the member's own failing condition
    # (t1, t2, t5, t6) and of a wrong type computed from the item in the same
    # statement (t3, t4), and the first such member in request order decides
    # (K1, K1r).
    ("t1", ["r_sidx_empty_failing", "plain_a"], EMPTY_UPDATE_WORDING),
    ("t2", ["r_bidx_empty_failing", "plain_a"], EMPTY_UPDATE_WORDING_B),
    ("t3", ["r_sidx_n_bidx_empty_failing"], EMPTY_UPDATE_WORDING_B),
    ("t4", ["r_sidx_empty_bidx_pk"], EMPTY_UPDATE_WORDING),
    ("t5", ["r_sidx_empty_failing"], EMPTY_UPDATE_WORDING),
    ("t6", ["r_bidx_empty_failing"], EMPTY_UPDATE_WORDING_B),
    ("K1", ["set_sidx_n_bidx_empty", "set_sidx_empty"], EMPTY_UPDATE_WORDING_B),
    ("K1r", ["set_sidx_empty", "set_sidx_n_bidx_empty"], EMPTY_UPDATE_WORDING),
    # 2026-11-01. Across members the first empty value in request order
    # decides, an empty literal or an empty value written from the item
    # (U3a, U3b, U3c); within one statement the literal comes first (U3f). A
    # member whose own condition fails writes nothing, so its empty
    # if_not_exists fallback is never reported (U1a to U1c).
    ("U3a", ["set_sidx_stored_empty", "set_bidx_empty"], EMPTY_UPDATE_WORDING),
    ("U3b", ["set_sidx_fallback_empty", "set_bidx_empty"], EMPTY_UPDATE_WORDING),
    ("U3c", ["set_bidx_empty", "set_sidx_stored_empty"], EMPTY_UPDATE_WORDING_B),
    ("U3f", ["set_sidx_e_bidx_empty"], EMPTY_UPDATE_WORDING_B),
    ("U1a", ["r_fallback_empty_failing"], (CCF,)),
    ("U1b", ["r_fallback_empty_failing", "plain_a"], (CCF, NONE)),
    ("U1c", ["plain_a", "r_fallback_empty_failing"], (NONE, CCF)),
]


def _items(client, table: str) -> dict:
    return {
        pk: client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True).get("Item")
        for pk in ("a", "b0", "bp", "c", "d0", "r")
    }


SEEDED = {"a": ITEM_A, "b0": None, "bp": ITEM_BP, "c": None, "d0": None, "r": ITEM_R}


@pytest.mark.parametrize("row_id, names, expected", ROWS, ids=[r[0] for r in ROWS])
def test_transaction_answers_as_the_service_does(dynamodb_client, table, row_id, names, expected):
    shapes = members(table)
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(TransactItems=[shapes[n] for n in names])
    resp = exc_info.value.response
    if isinstance(expected, str):
        assert resp["Error"]["Code"] == "ValidationException", resp["Error"]
        assert resp["Error"]["Message"] == expected
        assert "CancellationReasons" not in resp
    else:
        assert resp["Error"]["Code"] == "TransactionCanceledException", resp["Error"]
        codes = [code for code, _ in expected]
        assert resp["Error"]["Message"] == f"{CANCELLED} [{', '.join(codes)}]"
        got = [(r["Code"], r.get("Message")) for r in resp["CancellationReasons"]]
        assert got == list(expected)
    assert _items(dynamodb_client, table) == SEEDED


SINGLE = [
    ("S1", "update", "set_sidx_n", COMPUTED_WORDING),
    ("S2", "update", "set_sidx_empty", EMPTY_UPDATE_WORDING),
    ("S3", "put", "put_empty", EMPTY_ITEM_WORDING),
    ("S4", "update", "set_sidx_lit_n", TYPE_WORDING),
    ("S5", "put", "put_wrong", TYPE_WORDING),
]


@pytest.mark.parametrize("row_id, kind, name, message", SINGLE, ids=[r[0] for r in SINGLE])
def test_single_request_controls(dynamodb_client, table, row_id, kind, name, message):
    shape = members(table)[name]
    with pytest.raises(ClientError) as exc_info:
        if kind == "update":
            dynamodb_client.update_item(**shape["Update"])
        else:
            dynamodb_client.put_item(**shape["Put"])
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == message
    assert _items(dynamodb_client, table) == SEEDED


# UpdateItem on r, measured on 2026-10-31: an empty index-key literal is
# reported on the statement, after the literal type check and ahead of the
# condition and of a wrong type computed from the item (s1 to s9), and
# list_append that reads the item is a wrong type supplied in the request,
# ahead of the condition (la1, la2). Each row: id, UpdateExpression, names,
# values, whether the failing condition attribute_not_exists(pk) is attached,
# and the ValidationException message.
STATEMENT_ROWS = [
    ("s1", "SET #s = :e", {"#s": "sidx"}, {":e": {"S": ""}}, True, EMPTY_UPDATE_WORDING),
    ("s2", "SET #b = :eb", {"#b": "bidx"}, {":eb": {"B": b""}}, True, EMPTY_UPDATE_WORDING_B),
    ("s3", "SET #s = #n, #b = :eb", {"#s": "sidx", "#n": "n", "#b": "bidx"}, {":eb": {"B": b""}}, True, EMPTY_UPDATE_WORDING_B),
    ("s4", "SET #b = :eb, #s = #n", {"#s": "sidx", "#n": "n", "#b": "bidx"}, {":eb": {"B": b""}}, True, EMPTY_UPDATE_WORDING_B),
    ("s5", "SET #s = :e, #b = #k", {"#s": "sidx", "#b": "bidx", "#k": "pk"}, {":e": {"S": ""}}, True, EMPTY_UPDATE_WORDING),
    ("s6", "SET #s = #n, #b = :eb", {"#s": "sidx", "#n": "n", "#b": "bidx"}, {":eb": {"B": b""}}, False, EMPTY_UPDATE_WORDING_B),
    ("s7", "SET #s = :e, #b = #k", {"#s": "sidx", "#b": "bidx", "#k": "pk"}, {":e": {"S": ""}}, False, EMPTY_UPDATE_WORDING),
    ("s8", "SET #s = :e", {"#s": "sidx"}, {":e": {"S": ""}}, False, EMPTY_UPDATE_WORDING),
    ("s9", "SET #b = :eb", {"#b": "bidx"}, {":eb": {"B": b""}}, False, EMPTY_UPDATE_WORDING_B),
    ("la1", "SET #s = list_append(#l, :l)", {"#s": "sidx", "#l": "l"}, {":l": {"L": [{"S": "y"}]}}, False, TYPE_WORDING_L),
    ("la2", "SET #s = list_append(#l, :l)", {"#s": "sidx", "#l": "l"}, {":l": {"L": [{"S": "y"}]}}, True, TYPE_WORDING_L),
]


@pytest.mark.parametrize(
    "row_id, expr, names, values, failing, message", STATEMENT_ROWS, ids=[r[0] for r in STATEMENT_ROWS]
)
def test_update_item_answers_as_the_service_does(dynamodb_client, table, row_id, expr, names, values, failing, message):
    request = {
        "TableName": table,
        "Key": {"pk": {"S": "r"}},
        "UpdateExpression": expr,
        "ExpressionAttributeNames": dict(names),
        "ExpressionAttributeValues": values,
    }
    if failing:
        request["ExpressionAttributeNames"]["#p"] = "pk"
        request["ConditionExpression"] = "attribute_not_exists(#p)"
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.update_item(**request)
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == message
    assert _items(dynamodb_client, table) == SEEDED
