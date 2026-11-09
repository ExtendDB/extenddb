# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The condition keys of a PartiQL statement, measured with the tables of
the second measurement.

Every expected answer here was measured against live DynamoDB and IAM on
2026-11-03, with the principals of test_partiql_authorization.py.

Measured model:
  - An EXISTS member of ExecuteTransaction is authorized as a read:
    dynamodb:PartiQLSelect on the table, with the LeadingKeys, Attributes and
    EnclosingOperation of its SELECT.
  - dynamodb:ReturnValues is "NONE" for an INSERT, UPDATE or DELETE without
    RETURNING, in all three operations; MODIFIED OLD is "UPDATED_OLD",
    MODIFIED NEW "UPDATED_NEW", ALL OLD "ALL_OLD".
  - dynamodb:Attributes always holds the table's key attribute names, with
    the top-level names the statement uses; `*` adds none.
  - dynamodb:Select is "ALL_PROJECTED_ATTRIBUTES" for `*` on an index,
    "ALL_ATTRIBUTES" for `*` on a table and "SPECIFIC_ATTRIBUTES" for a
    projection list, in all three operations.
  - Only the PartiQL actions allow a statement.
  - A number partition key is compared as Java's BigDecimal prints the text
    the statement or the parameter gives it: `10` and `010` are "10", `10.0`
    is "10.0", `1E1` is "1E+1".
  - A write carries no dynamodb:FullTableScan.
  - A read of an index that returns attributes the index does not hold is
    authorized against the index ARN alone.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
"""

from __future__ import annotations

from partiql_authz_helpers import (  # noqa: F401 (the fixtures are used by name)
    AX,
    BX,
    PARTIQL,
    PartiqlAuthzCase,
    account_id,
    auth_env,
    batch,
    cancelled,
    mgmt,
    region,
    select,
    statement_error,
    transaction,
)

WRITES = ["dynamodb:PartiQLUpdate", "dynamodb:PartiQLDelete", "dynamodb:PartiQLInsert"]
SELECT_ONLY = ["dynamodb:PartiQLSelect"]


def attributes(*names: str) -> dict:
    return {"ForAllValues:StringEquals": {"dynamodb:Attributes": list(names)}}


def absent(key: str) -> dict:
    return {"Null": {f"dynamodb:{key}": "true"}}


class TestPartiqlAuthorizationKeys(PartiqlAuthzCase):
    """The condition keys each statement is authorized with."""

    # -- EXISTS ------------------------------------------------------------------

    def test_an_exists_member_is_authorized_as_a_read(self, r):
        # Measured 2026-11-03 (X1 to X7).
        exists_ax = f"EXISTS(SELECT * FROM \"{r.c}\" WHERE {AX} AND a = '1')"
        exists_bx = f"EXISTS(SELECT * FROM \"{r.c}\" WHERE {BX} AND a = '1')"
        update_ax = f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX}"
        update_bx = f"UPDATE \"{r.c}\" SET q = 1 WHERE {BX}"
        c, _ = self._identity()
        assert transaction(c, [exists_ax, update_bx])["Responses"] == []
        c, me = self._with(SELECT_ONLY)
        self._cancelled_by(cancelled(c, [exists_ax, update_bx]),
                           [None, ("PartiQLUpdate", r.c)], me)
        c, me = self._with(["dynamodb:PartiQLUpdate"])
        self._cancelled_by(cancelled(c, [exists_ax, update_bx]),
                           [("PartiQLSelect", r.c), None], me)
        c, me = self._identity()
        exists_b = f"EXISTS(SELECT * FROM \"{r.b}\" WHERE pk = 'a' AND g = 'ga')"
        self._cancelled_by(cancelled(c, [exists_b, update_bx]),
                           [("PartiQLSelect", r.b), None], me)
        c, me = self._fenced("ForAllValues:StringEquals", ["a"])
        self._cancelled_by(cancelled(c, [exists_bx, update_ax]),
                           [("PartiQLSelect", r.c), None], me)
        c, me = self._with(PARTIQL, attributes("pk", "sk", "q"))
        self._cancelled_by(cancelled(c, [exists_ax, update_bx]),
                           [("PartiQLSelect", r.c), None], me)
        c, _ = self._with(PARTIQL, {"StringEquals": {
            "dynamodb:EnclosingOperation": "ExecuteTransaction"}})
        assert transaction(c, [exists_ax, update_bx])["Responses"] == []

    # -- dynamodb:ReturnValues -----------------------------------------------------

    def test_return_values_is_none_for_a_write_without_returning(self, r):
        # Measured 2026-11-03: an Allow on StringEquals "NONE" runs them (R1
        # to R5) and an Allow on Null "true" refuses them (R6 to R10).
        update = f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX}"
        delete = f"DELETE FROM \"{r.c}\" WHERE pk = 'zz' AND sk = 'zz'"
        c, _ = self._with(WRITES, {"StringEquals": {"dynamodb:ReturnValues": "NONE"}})
        assert c.execute_statement(Statement=update)["Items"] == []
        assert c.execute_statement(Statement=delete)["Items"] == []
        c.execute_statement(Statement=f"INSERT INTO \"{r.c}\" VALUE {{'pk': 'r1', 'sk': 's'}}")
        assert batch(c, [update]) == [{"TableName": r.c}]
        assert transaction(c, [update])["Responses"] == []
        c, me = self._with(WRITES, absent("ReturnValues"))
        arn = self._arn(r.c)
        self._denied(statement_error(c, update), me, "PartiQLUpdate", arn)
        self._denied(statement_error(c, delete), me, "PartiQLDelete", arn)
        self._denied(statement_error(
            c, f"INSERT INTO \"{r.c}\" VALUE {{'pk': 'r2', 'sk': 's'}}"),
            me, "PartiQLInsert", arn)
        self._denied_entry(batch(c, [update])[0], me, "PartiQLUpdate", r.c)
        self._cancelled_by(cancelled(c, [update]), [("PartiQLUpdate", r.c)], me)

    def test_return_values_names_each_returning_mode(self, r):
        # Measured 2026-11-03 (R11 to R14), on an item that holds q.
        r.owner.update_item(TableName=r.c, Key={"pk": {"S": "a"}, "sk": {"S": "x"}},
                            UpdateExpression="SET q = :v",
                            ExpressionAttributeValues={":v": {"N": "1"}})
        for value, statement, items in (
                ("UPDATED_OLD", f"UPDATE \"{r.c}\" SET q = 7 WHERE {AX} "
                                "RETURNING MODIFIED OLD *", 1),
                ("UPDATED_NEW", f"UPDATE \"{r.c}\" SET q = 8 WHERE {AX} "
                                "RETURNING MODIFIED NEW *", 1),
                ("ALL_OLD", f"UPDATE \"{r.c}\" SET q = 9 WHERE {AX} RETURNING ALL OLD *", 1),
                ("ALL_OLD", f"DELETE FROM \"{r.c}\" WHERE pk = 'zz' AND sk = 'zz' "
                            "RETURNING ALL OLD *", 0),
        ):
            c, _ = self._with(WRITES, {"StringEquals": {"dynamodb:ReturnValues": value}})
            assert len(c.execute_statement(Statement=statement)["Items"]) == items, statement

    # -- dynamodb:Attributes -------------------------------------------------------

    def test_attributes_always_hold_the_table_key(self, r):
        # Measured 2026-11-03: under Null "true" every statement is refused,
        # and a statement that does not name sk is refused under a set
        # without it (A3 to A5, A9, A10, A14 to A16, A18, N21, N22).
        arn = self._arn(r.c)
        c, me = self._with(PARTIQL, absent("Attributes"))
        for statement, action in (
                (select(r.c, "pk = 'a'"), "PartiQLSelect"),
                (select(r.c), "PartiQLSelect"),
                (select(r.c, "pk = 'a'", what="pk"), "PartiQLSelect"),
                (f"DELETE FROM \"{r.c}\" WHERE pk = 'zz' AND sk = 'zz'", "PartiQLDelete"),
                (f"UPDATE \"{r.c}\" SET q = 7 WHERE {AX}", "PartiQLUpdate"),
        ):
            self._denied(statement_error(c, statement), me, action, arn)
        self._denied_entry(batch(c, [select(r.c, AX)])[0], me, "PartiQLSelect", r.c)
        for names, statement in (
                (("pk", "a"), select(r.c, "a = '1'", what="pk")),
                (("pk",), select(r.c, "a = '1'", what="pk")),
                (("q",), select(r.c, "pk = 'a'", what="pk")),
                (("pk", "q"), select(r.c, "pk = 'a'", what="pk")),
                (("pk", "q"), select(r.c, "pk = 'a' ORDER BY sk", what="pk")),
        ):
            c, me = self._with(PARTIQL, attributes(*names))
            self._denied(statement_error(c, statement), me, "PartiQLSelect", arn)

    def test_attributes_hold_the_names_a_statement_uses(self, r):
        # Measured 2026-11-03 (A1, A2, A6 to A8, A11, A17, N20).
        arn = self._arn(r.c)
        c, me = self._with(PARTIQL, attributes("q"))
        self._denied(statement_error(c, f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX}"),
                     me, "PartiQLUpdate", arn)
        c, me = self._with(PARTIQL, attributes("a"))
        self._denied(statement_error(c, select(r.c, "pk = 'a'", what="a")),
                     me, "PartiQLSelect", arn)
        # A nested path counts by its top-level name.
        c, _ = self._with(PARTIQL, attributes("pk", "sk", "m"))
        assert len(c.execute_statement(
            Statement=select(r.c, "pk = 'a'", what="m.x"))["Items"]) == 2
        c.execute_statement(
            Statement=f"INSERT INTO \"{r.c}\" VALUE {{'pk': 'n7', 'sk': 's', 'm': {{'x': 1}}}}")
        # A condition, a REMOVE target and a name in a SET value count.
        c, me = self._with(PARTIQL, attributes("pk", "sk"))
        self._denied(statement_error(
            c, f"DELETE FROM \"{r.c}\" WHERE pk = 'zz' AND sk = 'zz' AND g = 'x'"),
            me, "PartiQLDelete", arn)
        c, me = self._with(PARTIQL, attributes("pk", "sk", "a"))
        self._denied(statement_error(c, f"UPDATE \"{r.c}\" REMOVE q WHERE {AX}"),
                     me, "PartiQLUpdate", arn)
        # `*` adds no name, though the items hold g.
        assert len(c.execute_statement(Statement=select(r.c, "pk = 'a'"))["Items"]) == 2
        c, me = self._with(PARTIQL, attributes("pk", "sk", "q"))
        self._denied(statement_error(c, f"UPDATE \"{r.c}\" SET q = g WHERE {AX}"),
                     me, "PartiQLUpdate", arn)

    def test_attributes_are_carried_by_each_statement(self, r):
        # Measured 2026-11-03 (A12, A13).
        c, me = self._with(PARTIQL, attributes("pk", "sk", "a", "q"))
        out = batch(c, [f"UPDATE \"{r.c}\" SET q = 5 WHERE {AX}",
                        f"UPDATE \"{r.c}\" SET g = 'gz' WHERE {BX}"])
        assert out[0] == {"TableName": r.c}
        self._denied_entry(out[1], me, "PartiQLUpdate", r.c)
        self._cancelled_by(cancelled(c, [f"UPDATE \"{r.c}\" SET q = 6 WHERE {AX}",
                                         f"UPDATE \"{r.c}\" SET g = 'gz' WHERE {BX}"]),
                           [None, ("PartiQLUpdate", r.c)], me)

    # -- dynamodb:Select -------------------------------------------------------------

    def test_select_names_how_a_statement_projects(self, r):
        # Measured 2026-11-03 (S1 to S13).
        def under(value: str) -> tuple:
            return self._with(SELECT_ONLY, {"StringEquals": {"dynamodb:Select": value}})

        on_gix = select(r.c, "g = 'ga'", index="gix")
        on_kix = select(r.k, "g = 'ga'", index="kix")
        c, me = under("ALL_PROJECTED_ATTRIBUTES")
        assert len(c.execute_statement(Statement=on_gix)["Items"]) == 1
        assert len(c.execute_statement(Statement=on_kix)["Items"]) == 1
        self._denied(statement_error(c, select(r.c, "pk = 'a'")),
                     me, "PartiQLSelect", self._arn(r.c))
        c, me = under("ALL_ATTRIBUTES")
        self._denied(statement_error(c, on_gix), me, "PartiQLSelect", self._arn(r.c, "gix"))
        self._denied(statement_error(c, on_kix), me, "PartiQLSelect", self._arn(r.k, "kix"))
        assert "Item" in batch(c, [select(r.c, AX)])[0]
        assert len(transaction(c, [select(r.c, AX)])["Responses"]) == 1
        c, _ = under("SPECIFIC_ATTRIBUTES")
        assert len(c.execute_statement(
            Statement=select(r.c, "g = 'ga'", what="pk", index="gix"))["Items"]) == 1
        assert "Item" in batch(c, [select(r.c, AX, what="pk")])[0]
        c, me = self._with(SELECT_ONLY, absent("Select"))
        self._denied_entry(batch(c, [select(r.c, AX)])[0], me, "PartiQLSelect", r.c)
        self._cancelled_by(cancelled(c, [select(r.c, AX)]), [("PartiQLSelect", r.c)], me)
        for statement in (select(r.c, "pk = 'a'"), select(r.c)):
            self._denied(statement_error(c, statement), me, "PartiQLSelect", self._arn(r.c))

    # -- the actions, number keys, writes and index reads ----------------------------

    def test_only_the_partiql_actions_allow_a_statement(self, r):
        # Measured 2026-11-03: a caller allowed the three operations' own
        # actions on every resource is refused every statement (N1 to N3).
        c, me = self._client({"Effect": "Allow", "Resource": "*", "Action": [
            "dynamodb:ExecuteStatement", "dynamodb:BatchExecuteStatement",
            "dynamodb:ExecuteTransaction"]})
        self._denied(statement_error(c, select(r.c, "pk = 'a'")),
                     me, "PartiQLSelect", self._arn(r.c))
        self._denied_entry(batch(c, [select(r.c, AX)])[0], me, "PartiQLSelect", r.c)
        self._cancelled_by(cancelled(c, [select(r.c, AX)]), [("PartiQLSelect", r.c)], me)

    def test_a_number_partition_value_keeps_the_scale_it_is_written_with(self, r):
        # Measured 2026-11-03 under ForAllValues LeadingKeys ["10"] and
        # ["10.0"] (N4 to N11).
        arn = self._arn(r.n)
        c, me = self._fenced("ForAllValues:StringEquals", ["10"])
        for literal in ("10", "010"):
            resp = c.execute_statement(Statement=select(r.n, f"pk = {literal}"))
            assert len(resp["Items"]) == 1, literal
        for literal in ("10.0", "1E1"):
            self._denied(statement_error(c, select(r.n, f"pk = {literal}")),
                         me, "PartiQLSelect", arn)
        self._denied(statement_error(c, select(r.n, "pk = ?"), Parameters=[{"N": "10.0"}]),
                     me, "PartiQLSelect", arn)
        for literal in ("10.0", "1E1"):
            self._denied(statement_error(
                c, f"UPDATE \"{r.n}\" SET q = 1 WHERE pk = {literal}"),
                me, "PartiQLUpdate", arn)
        self._denied_entry(batch(c, [select(r.n, "pk = 1E1")])[0], me, "PartiQLSelect", r.n)
        self._cancelled_by(cancelled(c, [select(r.n, "pk = 1E1")]),
                           [("PartiQLSelect", r.n)], me)
        c, me = self._fenced("ForAllValues:StringEquals", ["10.0"])
        assert len(c.execute_statement(Statement=select(r.n, "pk = 10.0"))["Items"]) == 1
        self._denied(statement_error(c, select(r.n, "pk = 10")), me, "PartiQLSelect", arn)

    def test_a_write_carries_no_full_table_scan(self, r):
        # Measured 2026-11-03: an Allow on Bool "false" refuses every write
        # (N12 to N14).
        arn = self._arn(r.c)
        c, me = self._with(PARTIQL, {"Bool": {"dynamodb:FullTableScan": "false"}})
        for statement, action in (
                (f"INSERT INTO \"{r.c}\" VALUE {{'pk': 'f1', 'sk': 's'}}", "PartiQLInsert"),
                (f"UPDATE \"{r.c}\" SET q = 1 WHERE {AX}", "PartiQLUpdate"),
                (f"DELETE FROM \"{r.c}\" WHERE pk = 'zz' AND sk = 'zz'", "PartiQLDelete"),
        ):
            self._denied(statement_error(c, statement), me, action, arn)

    def test_an_index_read_that_reads_the_table_is_authorized_against_the_index(self, r):
        # Measured 2026-11-03: a read of the KEYS_ONLY LSI that returns u,
        # which the index does not hold, runs for a caller allowed the
        # indexes alone, and is refused naming the index ARN for one allowed
        # the table alone (N15 to N19).
        on_lix = select(r.l, "pk = 'a'", what="u", index="lix")
        indexes = [self._tables_pattern()[1]]
        c, _ = self._with(SELECT_ONLY, resources=indexes)
        for what in ("u", "*", "l"):
            resp = c.execute_statement(Statement=select(r.l, "pk = 'a'", what=what, index="lix"))
            assert len(resp["Items"]) == 1, what
        c, me = self._with(SELECT_ONLY, resources=[self._arn(r.l)])
        self._denied(statement_error(c, on_lix), me, "PartiQLSelect", self._arn(r.l, "lix"))
        c, _ = self._identity()
        assert c.execute_statement(Statement=on_lix)["Items"] == [{"u": {"S": "hidden"}}]
