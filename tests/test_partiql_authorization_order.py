# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Where the authorization of a PartiQL statement sits among its checks, and
the pages of a read by key whose refused disjuncts are held back.

Every expected answer here was measured against live DynamoDB and IAM on
2026-11-03, with the principals of test_partiql_authorization.py. B (pqb-*)
is outside the caller's policy, so there a check the service makes after
authorization is hidden by the refusal, and one it makes before is answered.

Measured model:
  - Before authorization: the parse of the statement, a VALUE that names an
    attribute twice among its refusals; the checks of the request (Limit,
    the number of statements, reads mixed with writes); fewer parameters than
    placeholders; ConsistentRead on a global secondary index; more than 100
    IN operands; more than 50 disjuncts; overlapping disjuncts; a path into a
    key attribute; a NextToken that does not decode; a DELETE RETURNING
    clause other than ALL OLD *; a BatchExecuteStatement read without the
    full primary key; SELECT COUNT(*); and in ExecuteTransaction the EXISTS
    form and an EXISTS on an index, on a table that exists.
  - After it: a parameter the statement does not use, ORDER BY on another
    attribute, a key value of the wrong type, an INSERT without the key, a
    key value over the size limit, SET of a key attribute, a missing index, a
    missing table. A transaction is cancelled for authorization before any
    condition is evaluated.
  - A refused disjunct of a read by key is held back: the request whose page
    visits only held-back disjuncts gets AccessDeniedException. Within a
    disjunct, authorization comes before the key size and key-attribute
    filter checks; across disjuncts, an authorization refusal ranks below the
    other refusals, and is the answer only when every held-back disjunct is
    refused for authorization.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
"""

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import ClientError

from partiql_authz_helpers import (  # noqa: F401 (the fixtures are used by name)
    AX,
    BX,
    PARTIQL,
    PartiqlAuthzCase,
    account_id,
    auth_env,
    batch,
    cancelled,
    keys_of,
    mgmt,
    region,
    select,
    statement_error,
    transaction_error,
)

FILTER = ("Filter Expression can only contain non-primary key attributes: Primary key "
          "attribute: sk")
LK = "ForAllValues:StringEquals"


def pages(client: Any, statement: str, **kwargs: Any) -> tuple[list[list[str]], dict | None]:
    """The keys of the items of each page of `statement`, and the error that
    ends the pages, if any."""
    out: list[list[str]] = []
    token = None
    for _ in range(8):
        args = dict(kwargs)
        if token:
            args["NextToken"] = token
        try:
            resp = client.execute_statement(Statement=statement, **args)
        except ClientError as exc:
            return out, exc.response["Error"]
        out.append(keys_of(resp["Items"]))
        token = resp.get("NextToken")
        if not token:
            return out, None
    raise AssertionError(f"more than 8 pages: {out}")


def values(n: int) -> str:
    return ", ".join(f"'v{i:03}'" for i in range(n))


def validation(message: str) -> dict:
    return {"Code": "ValidationException", "Message": message}


class TestPartiqlAuthorizationOrder(PartiqlAuthzCase):
    """Each statement is authorized at its place among its checks."""

    # -- the pages of a read with a refused disjunct ---------------------------------

    def test_the_request_whose_page_visits_only_refused_disjuncts_is_refused(self, r):
        # Measured 2026-11-03 (D1, D2, D4 to D6, D10 to D12).
        arn = self._arn(r.c)
        c, me = self._fenced(LK, ["a"])
        for where, first in (
                ("pk IN ['a', 'b']", ["a/x", "a/y"]),
                ("pk IN ['a', 'b', 'c']", ["a/x", "a/y"]),
                (f"({AX}) OR ({BX})", ["a/x"]),
                ("pk IN ['a', 'b'] ORDER BY pk DESC", ["a/x", "a/y"]),
                ("pk IN ['a', 'b'] AND a = '1'", ["a/x", "a/y"]),
        ):
            got, error = pages(c, select(r.c, where))
            assert got == [first], where
            self._denied(error, me, "PartiQLSelect", arn)
        got, error = pages(c, select(r.c, "pk IN ['b']"))
        assert got == []
        self._denied(error, me, "PartiQLSelect", arn)
        c, me = self._fenced(LK, ["b"])
        got, error = pages(c, select(r.c, "pk IN ['a', 'b']"))
        assert got == [["b/x"]]
        self._denied(error, me, "PartiQLSelect", arn)
        c, me = self._fenced(LK, ["a", "c"])
        got, error = pages(c, select(r.c, "pk IN ['a', 'b', 'c']"))
        assert got == [["a/x", "a/y", "c/x"]]
        self._denied(error, me, "PartiQLSelect", arn)

    def test_a_limit_pages_the_allowed_disjuncts_before_the_refusal(self, r):
        # Measured 2026-11-03 (D3): [a/x] | [a/y] | [] | AccessDenied. The
        # empty page is the end of a partition's answer, not asserted here.
        c, me = self._fenced(LK, ["a"])
        got, error = pages(c, select(r.c, "pk IN ['a', 'b']"), Limit=1)
        assert got[:2] == [["a/x"], ["a/y"]]
        assert all(page == [] for page in got[2:]), got
        self._denied(error, me, "PartiQLSelect", self._arn(r.c))

    def test_a_refused_disjunct_is_refused_for_authorization_before_its_checks(self, r):
        # Measured 2026-11-03: under ["a"] a refused disjunct past the key
        # size limit gets AccessDenied once the allowed one is read, last in
        # read order or first (D7, D8); under ["b"] so does one with a filter
        # on a key attribute (D15).
        arn = self._arn(r.c)
        c, me = self._fenced(LK, ["a"])
        big, big_first = "z" * 2049, "0" + "z" * 2048
        for where in (f"pk IN ['a', 'b', '{big}']", f"pk IN ['{big_first}', 'a', 'b']"):
            got, error = pages(c, select(r.c, where))
            assert got == [["a/x", "a/y"]]
            self._denied(error, me, "PartiQLSelect", arn)
        c, me = self._fenced(LK, ["b"])
        got, error = pages(c, select(r.c, "(pk = 'a' AND sk = a) OR pk = 'b' OR pk = 'c'"))
        assert got == [["b/x"]]
        self._denied(error, me, "PartiQLSelect", arn)

    def test_an_authorization_refusal_ranks_below_the_refusals_of_the_checks(self, r):
        # Measured 2026-11-03 under ["a"] (D13, D14): refused at once with the
        # filter refusal of the disjunct the policy allows, though b is read
        # later.
        c, _ = self._fenced(LK, ["a"])
        for where in ("(pk = 'a' AND sk = a) OR pk = 'b'",
                      "pk = 'b' OR (pk = 'a' AND sk = a)"):
            assert statement_error(c, select(r.c, where)) == validation(FILTER), where

    def test_a_read_of_an_index_holds_back_its_refused_disjuncts(self, r):
        # Measured 2026-11-03 under ["ga"] (D9): the refusal names the index.
        c, me = self._fenced(LK, ["ga"])
        got, error = pages(c, select(r.c, "g IN ['ga', 'gb']", index="gix"))
        assert got == [["a/x"]]
        self._denied(error, me, "PartiQLSelect", self._arn(r.c, "gix"))

    # -- the checks made before authorization ----------------------------------------

    def test_the_checks_made_before_authorization_answer_first(self, r):
        # Measured 2026-11-03 on B (O1 to O4, O6, O7, O15, O17, V11).
        c, _ = self._identity()
        b = r.b
        for statement, kwargs, message in (
                (select(b, "pk = ?"), {},
                 "Number of parameters in request and statement don't match."),
                (select(b, "g = 'ga'", index="gix"), {"ConsistentRead": True},
                 "Strongly consistent read is not supported on Global Secondary Indexes"),
                (select(b, f"pk IN [{values(101)}]"), {},
                 "The IN operator is provided with too many operands; number of operands: 101"),
                (select(b, f"pk IN [{values(51)}]"), {},
                 "Too many decomposed read operations for a given query."),
                (select(b, "pk = 'a' OR pk = 'a'"), {},
                 "Overlapping conditions with range keys are not supported in where clause"),
                (select(b, "pk.x = 1"), {},
                 "Key attributes must be scalars; list random access '[]' and map lookup '.' "
                 "are not allowed: Key pk"),
                (select(b, "pk = 'a'"), {"NextToken": "garbage"}, "Invalid NextToken"),
                (f"DELETE FROM \"{b}\" WHERE pk = 'a' RETURNING ALL NEW *", {},
                 "Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is "
                 "allowed in DELETE statements."),
                (select(b, "pk = 'a'", what="COUNT(*)"), {},
                 "Unexpected path component at 1:8:5"),
        ):
            assert statement_error(c, statement, **kwargs) == validation(message), statement

    def test_the_checks_of_the_request_are_made_before_authorization(self, r):
        # Measured 2026-11-03 on B (O8, O14, O19, O20).
        c, _ = self._identity(validate=False)
        b = r.b
        error = statement_error(c, select(b, "pk = 'a'"), Limit=0)
        assert error == validation(
            "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: "
            "Member must have value greater than or equal to 1")
        statements = [select(b, f"pk = 'v{i:03}'") for i in range(26)]
        with pytest.raises(ClientError) as exc:
            batch(c, statements)
        echo = ", ".join(
            f"BatchStatementRequest(statement={s}, parameters=null, consistentRead=null, "
            "returnValuesOnConditionCheckFailure=null)" for s in statements)
        assert exc.value.response["Error"] == validation(
            f"1 validation error detected: Value '[{echo}]' at 'statements' failed to satisfy "
            "constraint: Member must have length less than or equal to 25")
        mixed = [select(b, "pk = 'a'"), f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX}"]
        assert transaction_error(c, mixed) == validation(
            "ExecuteTransaction API does not support both read and write operations in the "
            "same request.")
        with pytest.raises(ClientError) as exc:
            batch(c, mixed)
        assert exc.value.response["Error"] == validation(
            "Read and write requests together in the same batch is not supported.")

    def test_a_batch_read_is_refused_its_form_before_authorization(self, r):
        # Measured 2026-11-03: a read without the full primary key and a
        # COUNT(*), on B and on a missing table outside the policy, are the
        # statement's ValidationError (O23, V12, V13).
        c, _ = self._identity()
        out = batch(c, [select(r.b, "g = 'ga'"),
                        select(r.b, "pk = 'a'", what="COUNT(*)"),
                        select(r.missing_b, "pk = 'a'", what="COUNT(*)")])
        assert out == [
            {"Error": {"Code": "ValidationError", "Message":
                       "Select statements within BatchExecuteStatement must specify the "
                       "primary key in the where clause."}},
            {"Error": {"Code": "ValidationError",
                       "Message": "Unexpected path component at 1:8:5"}},
            {"Error": {"Code": "ValidationError",
                       "Message": "Unexpected path component at 1:8:5"}},
        ]

    def test_a_transaction_member_is_refused_its_form_before_authorization(self, r):
        # Measured 2026-11-03 on B and a missing table outside the policy: an
        # EXISTS without a condition beyond the key, an EXISTS on an index and
        # a COUNT(*) (V7, V10, V14, V15).
        c, _ = self._identity()
        head = "Validation failed in TransactStatements[0]: "
        for statement, message in (
                (f"EXISTS(SELECT * FROM \"{r.b}\" WHERE pk = 'a')",
                 "EXISTS() must contain a single item read with additional condition"),
                (f"EXISTS(SELECT * FROM \"{r.b}\".\"gix\" WHERE g = 'ga' AND pk = 'a')",
                 "Operations on indices are not supported in transactions"),
                (select(r.b, "pk = 'a'", what="COUNT(*)"), "Unexpected path component at 1:8:5"),
                (select(r.missing_b, "pk = 'a'", what="COUNT(*)"),
                 "Unexpected path component at 1:8:5"),
        ):
            assert transaction_error(c, [statement]) == validation(head + message), statement

    # -- the checks made after authorization -----------------------------------------

    def test_the_checks_made_after_authorization_are_hidden_by_its_refusal(self, r):
        # Measured 2026-11-03 on B (O5, O9 to O11, O16, O18, O21).
        c, me = self._identity()
        b = r.b
        for statement, kwargs, action, resource in (
                (select(b, "pk = 'a' ORDER BY n"), {}, "PartiQLSelect", self._arn(b)),
                (f"INSERT INTO \"{b}\" VALUE {{'pk': 1}}", {}, "PartiQLInsert", self._arn(b)),
                (f"INSERT INTO \"{b}\" VALUE {{'x': 'y'}}", {}, "PartiQLInsert", self._arn(b)),
                (select(b, f"pk = '{'z' * 2049}'"), {}, "PartiQLSelect", self._arn(b)),
                (f"UPDATE \"{b}\" SET pk = 'z' WHERE pk = 'a'", {}, "PartiQLUpdate",
                 self._arn(b)),
                (select(b, "pk = 'a'"), {"Parameters": [{"S": "x"}]}, "PartiQLSelect",
                 self._arn(b)),
                (select(b, "g = 'ga'", index="nix"), {"ConsistentRead": True},
                 "PartiQLSelect", self._arn(b, "nix")),
        ):
            self._denied(statement_error(c, statement, **kwargs), me, action, resource)

    def test_a_transaction_is_cancelled_for_authorization_before_its_conditions(self, r):
        # Measured 2026-11-03: a condition that fails beside a refused member
        # has the reason None, and each refused member has its own refusal
        # (O12, O13, O22).
        c, me = self._identity()
        refused = f"UPDATE \"{r.b}\" SET q = 1 WHERE pk = 'a'"
        failing = f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX} AND a = 'nope'"
        self._cancelled_by(cancelled(c, [refused, failing]),
                           [("PartiQLUpdate", r.b), None], me)
        self._cancelled_by(cancelled(c, [failing, refused]),
                           [None, ("PartiQLUpdate", r.b)], me)
        self._cancelled_by(
            cancelled(c, [refused, f"UPDATE \"{r.b}\" SET q = 2 WHERE pk = 'b'"]),
            [("PartiQLUpdate", r.b), ("PartiQLUpdate", r.b)], me)

    def test_a_transaction_member_on_a_missing_table_is_authorized_first(self, r):
        # Measured 2026-11-03: the EXISTS form is not checked on a missing
        # table; the member is refused, or for a caller that may read the
        # table cancelled with ResourceNotFound (V8, V9).
        c, me = self._identity()
        bare = "EXISTS(SELECT * FROM \"{}\" WHERE pk = 'a')"
        self._cancelled_by(cancelled(c, [bare.format(r.missing_b)]),
                           [("PartiQLSelect", r.missing_b)], me)
        resp = cancelled(c, [bare.format(r.missing_a)])
        assert resp["Error"]["Message"].endswith("[ResourceNotFound]")
        assert resp["CancellationReasons"] == [
            {"Code": "ResourceNotFound", "Message": "Requested resource not found"}]

    def test_a_read_of_an_index_of_a_missing_table_is_authorized_against_the_table(self, r):
        # Measured 2026-11-03 (V16); an existing table's missing index is
        # authorized against the index (O21).
        c, me = self._identity()
        self._denied(statement_error(c, select(r.missing_b, "g = 'ga'", index="gix")),
                     me, "PartiQLSelect", self._arn(r.missing_b))

    def test_a_missing_table_or_index_carries_no_full_table_scan(self, r):
        # Measured 2026-11-03: a Deny of PartiQLSelect on FullTableScan "true"
        # does not refuse these, which get the answer of the missing table or
        # index (N23 to N25).
        c, _ = self._client(
            self._allow(PARTIQL),
            self._deny(["dynamodb:PartiQLSelect"], {"Bool": {"dynamodb:FullTableScan": "true"}}))
        not_found = {"Code": "ResourceNotFoundException",
                     "Message": "Requested resource not found"}
        assert statement_error(c, select(r.missing_a)) == not_found
        assert statement_error(c, select(r.missing_a, "pk = 'a'")) == not_found
        assert statement_error(c, select(r.c, index="nix")) == validation(
            "The table does not have the specified index")

    # -- a VALUE that names an attribute twice ----------------------------------------

    def test_a_value_that_names_an_attribute_twice_is_refused_before_authorization(self, r):
        # Measured 2026-11-03: refused at the second name, for any caller,
        # and nothing is written (V1 to V5). The table names are 12
        # characters long, as the measured ones were.
        c, _ = self._identity()
        lk_a, _ = self._fenced(LK, ["a"])
        table = f"\"{r.c}\""
        for client, value, refusal, keys in (
                (c, "{'pk': 'd1', 'pk': 'd2', 'sk': 's'}", "pk under root at 1:47:4",
                 [("d1", "s"), ("d2", "s")]),
                (lk_a, "{'pk': 'a', 'pk': 'b', 'sk': 'dup'}", "pk under root at 1:46:4",
                 [("a", "dup"), ("b", "dup")]),
                (lk_a, "{'pk': 'b', 'pk': 'a', 'sk': 'dup2'}", "pk under root at 1:46:4",
                 [("a", "dup2"), ("b", "dup2")]),
                (c, "{'pk': 'd3', 'sk': 's', 'v': '1', 'v': '2'}", "v under root at 1:68:3",
                 [("d3", "s")]),
                (c, "{'pk': 'd4', 'sk': 's1', 'sk': 's2'}", "sk under root at 1:59:4",
                 [("d4", "s1"), ("d4", "s2")]),
        ):
            error = statement_error(client, f"INSERT INTO {table} VALUE {value}")
            assert error == validation(f"Duplicate keys in tuple. Key: {refusal}"), value
            for pk, sk in keys:
                item = r.owner.get_item(TableName=r.c, ConsistentRead=True,
                                        Key={"pk": {"S": pk}, "sk": {"S": sk}})
                assert "Item" not in item, (value, pk, sk)

    def test_a_name_given_twice_in_a_batch_or_a_transaction(self, r):
        # Measured 2026-11-03: in BatchExecuteStatement the statement's
        # ValidationError (V6); in ExecuteTransaction the refusal names the
        # statement, as a parse refusal does (with a permitted caller, in
        # probe_dups).
        c, _ = self._identity()
        insert = f"INSERT INTO \"{r.c}\" VALUE {{'pk': 'd5', 'pk': 'd6', 'sk': 's'}}"
        refusal = "Duplicate keys in tuple. Key: pk under root at 1:47:4"
        assert batch(c, [insert]) == [
            {"Error": {"Code": "ValidationError", "Message": refusal}}]
        assert transaction_error(c, [insert]) == validation(
            f"Validation failed in TransactStatements[0]: {refusal}")
