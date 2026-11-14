# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Per-statement authorization of ExecuteStatement, BatchExecuteStatement and
ExecuteTransaction.

A PartiQL request names its tables inside its statements. The service
authorizes each statement with the action of its type (dynamodb:PartiQLSelect,
dynamodb:PartiQLInsert, dynamodb:PartiQLUpdate, dynamodb:PartiQLDelete) against
the table it names, or for a read of an index against the index, and needs no
action for the operation itself.

Every expected answer here was measured against live DynamoDB and IAM on
2026-11-03, in a role session whose identity policy allowed dynamodb:* on the
tables named pqa-* and denied dynamodb:PartiQLDelete on pqa-deny-*, narrowed by
a session policy for most cases. These tests give an IAM user the same
permissions in its identity policy, so a refusal names the user and an
identity-based policy; the session policy cases use a role session.

Measured model:
  - The action is the statement's type. The classic item actions allow no
    statement, and a statement needs no other action.
  - The resource is the table ARN, or for a read of an index the index ARN
    alone.
  - A refused ExecuteStatement is AccessDeniedException. In a
    BatchExecuteStatement the refused statement's entry is
    {"Error": {"Code": "AccessDenied", "Message": ...}, "TableName": ...} and
    the others run. A refused ExecuteTransaction member cancels the
    transaction with AccessDenied for it and None for the others, and nothing
    is written.
  - Syntax, the EXISTS form and the table name rule are checked before
    authorization; a missing table, a missing index and the key checks after.
    test_partiql_authorization_order.py has the other checks on each side.
  - dynamodb:LeadingKeys holds the partition key values a statement reads or
    writes, the index's for a read of an index, and is absent for a scan. Each
    disjunct of a SELECT read by key is authorized by itself: a refused one is
    held back while the items of the others come back with a NextToken.
  - dynamodb:FullTableScan is "true" for an ExecuteStatement scan, "false" for
    an ExecuteStatement read by key, and absent in the other two operations.
  - dynamodb:EnclosingOperation is BatchExecuteStatement or ExecuteTransaction
    in those operations and absent in ExecuteStatement.
  - dynamodb:Attributes holds the table's key attributes and the top-level
    names a statement names, the WHERE included; dynamodb:Select is
    SPECIFIC_ATTRIBUTES for a projection list, ALL_ATTRIBUTES for `*` on a
    table and ALL_PROJECTED_ATTRIBUTES for `*` on an index;
    dynamodb:ReturnValues comes from RETURNING, and is NONE for a write
    without it.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
"""

from __future__ import annotations

import time
import uuid
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active
from partiql_authz_helpers import (  # noqa: F401 (the fixtures are used by name)
    ALLOW_ALL,
    AX,
    BX,
    CANCELLED,
    CLASSIC,
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
    transaction,
)


class TestPartiqlAuthorization(PartiqlAuthzCase):
    """Each statement of a PartiQL request is authorized by itself."""

    # -- tables ----------------------------------------------------------------

    @pytest.fixture()
    def t(self):
        """The measured tables: C (pk, sk, GSI gix on g) holding a/x (g ga),
        a/y and b/x; H and D (pqa-deny-*) keyed on pk; B outside the pqa-*
        pattern, keyed on pk with GSI gix on g."""
        self._run = uuid.uuid4().hex[:6]
        owner, _ = self._user(ALLOW_ALL)
        names = SimpleNamespace(
            c=f"pqa-{self._run}-c", h=f"pqa-{self._run}-h", d=f"pqa-deny-{self._run}",
            b=f"pqb-{self._run}", missing_a=f"pqa-{self._run}-missing",
            missing_b=f"pqb-{self._run}-missing", owner=owner)

        def create(name: str, attrs: list[str], keys: list[tuple[str, str]],
                   gsi: bool = False) -> None:
            extra: dict = {}
            if gsi:
                extra["GlobalSecondaryIndexes"] = [{
                    "IndexName": "gix",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}}]
            owner.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"}
                                      for a in attrs],
                KeySchema=[{"AttributeName": a, "KeyType": k} for a, k in keys],
                BillingMode="PAY_PER_REQUEST", **extra)
            wait_for_active(owner, name)

        def put(name: str, **attrs: str) -> None:
            owner.put_item(TableName=name, Item={k: {"S": v} for k, v in attrs.items()})

        create(names.c, ["pk", "sk", "g"], [("pk", "HASH"), ("sk", "RANGE")], gsi=True)
        create(names.h, ["pk"], [("pk", "HASH")])
        create(names.d, ["pk"], [("pk", "HASH")])
        create(names.b, ["pk", "g"], [("pk", "HASH")], gsi=True)
        put(names.c, pk="a", sk="x", g="ga", a="1")
        put(names.c, pk="a", sk="y", a="1")
        put(names.c, pk="b", sk="x", g="gb", a="1")
        put(names.h, pk="a")
        put(names.h, pk="b")
        put(names.d, pk="a")
        put(names.b, pk="a", g="ga")
        for name, count in ((names.c, 2), (names.b, 1)):
            deadline = time.monotonic() + 15
            while owner.scan(TableName=name, IndexName="gix",
                             Select="COUNT")["Count"] < count:
                assert time.monotonic() < deadline, f"index of {name} not filled"
                time.sleep(0.05)
        yield names
        for name in (names.c, names.h, names.d, names.b):
            try:
                owner.delete_table(TableName=name)
            except Exception:
                pass

    def _items(self, t, table: str) -> list[str]:
        return keys_of(t.owner.scan(TableName=table, ConsistentRead=True)["Items"])

    # -- 1. actions and resources ---------------------------------------------

    def test_each_statement_type_is_authorized_with_its_action(self, t):
        # Measured 2026-11-03: on a table outside the policy each statement
        # is refused naming the action of its type and the table's ARN; on a
        # table inside it a SELECT runs.
        c, me = self._identity()
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        for statement, action in [
            (select(t.b, "pk = 'a'"), "PartiQLSelect"),
            (f"INSERT INTO \"{t.b}\" VALUE {{'pk': 'z'}}", "PartiQLInsert"),
            (f"UPDATE \"{t.b}\" SET q = 1 WHERE pk = 'a'", "PartiQLUpdate"),
            (f"DELETE FROM \"{t.b}\" WHERE pk = 'zz'", "PartiQLDelete"),
        ]:
            self._denied(statement_error(c, statement), me, action, self._arn(t.b))
        assert self._items(t, t.b) == ["a"]

    def test_a_partiql_action_allows_every_operation_its_statement_runs_in(self, t):
        # Measured 2026-11-03: with dynamodb:PartiQLSelect alone a SELECT runs
        # in each of the three operations, and an INSERT or UPDATE is refused
        # naming its own action.
        c, me = self._client(self._allow(["dynamodb:PartiQLSelect"]))
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        assert len(c.execute_statement(
            Statement=select(t.c, "g = 'ga'", index="gix"))["Items"]) == 1
        assert len(c.execute_statement(Statement=select(t.c, index="gix"))["Items"]) == 2
        assert "Item" in batch(c, [select(t.c, AX)])[0]
        assert len(transaction(c, [select(t.c, AX)])["Responses"]) == 1
        self._denied(statement_error(
            c, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}"),
            me, "PartiQLInsert", self._arn(t.c))
        self._denied(statement_error(c, f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}"),
                     me, "PartiQLUpdate", self._arn(t.c))

    def test_the_classic_item_actions_allow_no_statement(self, t):
        # Measured 2026-11-03.
        c, me = self._client(self._allow(CLASSIC))
        self._denied(statement_error(c, select(t.c, "pk = 'a'")),
                     me, "PartiQLSelect", self._arn(t.c))
        self._denied(statement_error(
            c, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n3', 'sk': 's'}}"),
            me, "PartiQLInsert", self._arn(t.c))
        self._denied_entry(batch(c, [select(t.c, AX)])[0], me, "PartiQLSelect", t.c)
        self._cancelled_by(cancelled(c, [select(t.c, AX)]),
                           [("PartiQLSelect", t.c)], me)

    def test_an_index_read_is_authorized_against_the_index_alone(self, t):
        # Measured 2026-11-03: allowed on the indexes alone, a read of the
        # index runs and a read of the table is refused; a read of an index of
        # a table outside the policy is refused naming the index ARN, also for
        # an index the table does not have.
        idx = [f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/pqa-*/index/*"]
        c, me = self._client(self._allow(["dynamodb:PartiQLSelect"], resources=idx))
        assert len(c.execute_statement(
            Statement=select(t.c, "g = 'ga'", index="gix"))["Items"]) == 1
        self._denied(statement_error(c, select(t.c, "pk = 'a'")),
                     me, "PartiQLSelect", self._arn(t.c))
        c, me = self._identity()
        self._denied(statement_error(c, select(t.b, "g = 'ga'", index="gix")),
                     me, "PartiQLSelect", self._arn(t.b, "gix"))
        self._denied(statement_error(c, select(t.b, "g = 'ga'", index="nix")),
                     me, "PartiQLSelect", self._arn(t.b, "nix"))

    def test_returning_needs_no_other_action(self, t):
        # Measured 2026-11-03.
        c, _ = self._client(self._allow(["dynamodb:PartiQLUpdate"]))
        resp = c.execute_statement(
            Statement=f"UPDATE \"{t.c}\" SET q = 2 WHERE {AX} RETURNING ALL NEW *")
        assert len(resp["Items"]) == 1
        resp = c.execute_statement(
            Statement=f"UPDATE \"{t.c}\" SET q = 3 WHERE {AX} RETURNING MODIFIED OLD *")
        assert len(resp["Items"]) == 1
        c, _ = self._client(self._allow(["dynamodb:PartiQLDelete"]))
        resp = c.execute_statement(Statement=f"DELETE FROM \"{t.c}\" WHERE "
                                   f"pk = 'zz' AND sk = 'zz' RETURNING ALL OLD *")
        assert resp["Items"] == []

    def test_an_explicit_deny_on_one_action_leaves_the_others(self, t):
        # Measured 2026-11-03: with PartiQLDelete denied on D, a DELETE is
        # refused with the explicit deny wording and a SELECT and an UPDATE
        # run.
        c, me = self._identity()
        self._denied(statement_error(c, f"DELETE FROM \"{t.d}\" WHERE pk = 'zz'"),
                     me, "PartiQLDelete", self._arn(t.d), explicit=True)
        assert len(c.execute_statement(Statement=select(t.d, "pk = 'a'"))["Items"]) == 1
        assert c.execute_statement(
            Statement=f"UPDATE \"{t.d}\" SET q = 1 WHERE pk = 'a'")["Items"] == []

    # -- 2. the answers to a refused statement ----------------------------------

    def test_a_refused_batch_statement_is_its_entry_and_the_others_run(self, t):
        # Measured 2026-11-03.
        c, me = self._identity()
        out = batch(c, [select(t.c, AX), select(t.b, "pk = 'a'")])
        assert "Item" in out[0]
        self._denied_entry(out[1], me, "PartiQLSelect", t.b)
        out = batch(c, [select(t.b, "pk = 'a'"), select(t.c, AX)])
        self._denied_entry(out[0], me, "PartiQLSelect", t.b)
        assert "Item" in out[1]
        out = batch(c, [f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}",
                        f"UPDATE \"{t.b}\" SET q = 1 WHERE pk = 'a'"])
        assert out[0] == {"TableName": t.c}
        self._denied_entry(out[1], me, "PartiQLUpdate", t.b)
        out = batch(c, [f"DELETE FROM \"{t.d}\" WHERE pk = 'zz'",
                        f"UPDATE \"{t.c}\" SET q = 2 WHERE {AX}"])
        self._denied_entry(out[0], me, "PartiQLDelete", t.d, explicit=True)
        assert out[1] == {"TableName": t.c}
        item = t.owner.get_item(TableName=t.c, Key={"pk": {"S": "a"}, "sk": {"S": "x"}})
        assert item["Item"]["q"] == {"N": "2"}, "the allowed statements ran"

    def test_a_missing_table_entry_beside_a_refusal_has_no_table_name(self, t):
        # Measured 2026-11-03.
        c, me = self._identity()
        out = batch(c, [select(t.b, "pk = 'a'"), select(t.missing_a, "pk = 'a'")])
        self._denied_entry(out[0], me, "PartiQLSelect", t.b)
        assert out[1] == {"Error": {"Code": "ResourceNotFound",
                                    "Message": "Requested resource not found"}}

    def test_a_refused_member_cancels_the_transaction_and_nothing_is_written(self, t):
        # Measured 2026-11-03.
        c, me = self._identity()
        resp = cancelled(c, [f"UPDATE \"{t.c}\" SET q = 3 WHERE {AX}",
                             f"UPDATE \"{t.b}\" SET q = 3 WHERE pk = 'a'"])
        self._cancelled_by(resp, [None, ("PartiQLUpdate", t.b)], me)
        resp = cancelled(c, [select(t.c, AX), select(t.b, "pk = 'a'")])
        self._cancelled_by(resp, [None, ("PartiQLSelect", t.b)], me)
        resp = cancelled(c, [f"DELETE FROM \"{t.d}\" WHERE pk = 'zz'",
                             f"UPDATE \"{t.c}\" SET q = 5 WHERE {AX}"])
        self._cancelled_by(resp, [("PartiQLDelete", t.d, "explicit"), None], me)
        item = t.owner.get_item(TableName=t.c, Key={"pk": {"S": "a"}, "sk": {"S": "x"}})
        assert "q" not in item["Item"], "a cancelled transaction writes nothing"

    # -- 3. where authorization sits among the checks ---------------------------

    def test_the_form_of_a_statement_is_checked_before_authorization(self, t):
        # Measured 2026-11-03: a syntax error, an EXISTS without a condition
        # beyond the key and a table name that breaks the name rule are
        # refused whatever the caller may do.
        c, _ = self._identity()
        error = statement_error(c, f"SELEC * FROM \"{t.b}\"")
        assert error == {"Code": "ValidationException", "Message":
                         "Statement wasn't well formed, can't be processed: "
                         "Expected data manipulation"}
        with pytest.raises(ClientError) as exc:
            transaction(c, [f"EXISTS(SELECT * FROM \"{t.b}\" WHERE pk = 'a')",
                            f"UPDATE \"{t.c}\" SET q = 4 WHERE {AX}"])
        assert exc.value.response["Error"] == {
            "Code": "ValidationException",
            "Message": "Validation failed in TransactStatements[0]: EXISTS() must "
                       "contain a single item read with additional condition"}
        error = statement_error(c, select(self._arn(t.c), "pk = 'a'"))
        assert error == {"Code": "ValidationException", "Message":
                         "Table name should be within the length [3, 255] and only "
                         "contain 'a-z', 'A-Z', '0-9', '-', '_', '.'."}

    def test_the_table_the_index_and_the_keys_are_checked_after_authorization(self, t):
        # Measured 2026-11-03: a caller that may not read a table gets
        # AccessDenied for a missing table, a missing index, an UPDATE without
        # the full key and a key value of the wrong type; one that may gets
        # ResourceNotFoundException for a missing table.
        c, me = self._identity()
        self._denied(statement_error(c, select(t.missing_b, "pk = 'a'")),
                     me, "PartiQLSelect", self._arn(t.missing_b))
        error = statement_error(c, select(t.missing_a, "pk = 'a'"))
        assert error == {"Code": "ResourceNotFoundException",
                         "Message": "Requested resource not found"}
        self._denied(statement_error(c, f"UPDATE \"{t.b}\" SET q = 1 WHERE q = 2"),
                     me, "PartiQLUpdate", self._arn(t.b))
        self._denied(statement_error(c, select(t.b, "pk = 1")),
                     me, "PartiQLSelect", self._arn(t.b))
        self._denied(statement_error(c, select(t.b)),
                     me, "PartiQLSelect", self._arn(t.b))
        self._denied(statement_error(c, select(t.b, "pk = ?"),
                                     Parameters=[{"S": "a"}]),
                     me, "PartiQLSelect", self._arn(t.b))
        resp = cancelled(c, [f"UPDATE \"{t.b}\" SET q = 1 WHERE q = 2",
                             f"UPDATE \"{t.c}\" SET q = 6 WHERE {AX}"])
        self._cancelled_by(resp, [("PartiQLUpdate", t.b), None], me)

    def test_an_allowed_write_reaches_the_item_checks(self, t):
        # Measured 2026-11-03.
        c, _ = self._client(self._allow(["dynamodb:PartiQLUpdate"]))
        error = statement_error(
            c, f"UPDATE \"{t.c}\" SET q = 1 WHERE pk = 'zz' AND sk = 'zz'")
        assert error["Code"] == "ConditionalCheckFailedException"
        c, _ = self._client(self._allow(["dynamodb:PartiQLInsert"]))
        error = statement_error(c, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'a', 'sk': 'x'}}")
        assert error == {"Code": "DuplicateItemException",
                         "Message": "Duplicate primary key exists in table"}

    # -- 4. dynamodb:LeadingKeys -------------------------------------------------

    def test_a_read_by_key_is_fenced_on_its_partition_key(self, t):
        # Measured 2026-11-03 under ForAllValues:StringEquals ["a"].
        c, me = self._fenced("ForAllValues:StringEquals", ["a"])
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        assert len(c.execute_statement(Statement=select(t.c, AX))["Items"]) == 1
        assert len(c.execute_statement(Statement=select(t.c, "pk IN ['a']"))["Items"]) == 2
        resp = c.execute_statement(Statement=select(t.c, "pk = ?"),
                                   Parameters=[{"S": "a"}])
        assert len(resp["Items"]) == 2
        self._denied(statement_error(c, select(t.c, "pk = 'b'")),
                     me, "PartiQLSelect", self._arn(t.c))

    def test_each_disjunct_of_a_read_by_key_is_authorized_by_itself(self, t):
        # Measured 2026-11-03: the refused partition b is held back, and the
        # items of a come back with a NextToken.
        for operator in ("ForAllValues:StringEquals", "ForAnyValue:StringEquals"):
            c, _ = self._fenced(operator, ["a"])
            for where in ("pk IN ['a', 'b']", "pk = 'a' OR pk = 'b'"):
                resp = c.execute_statement(Statement=select(t.c, where))
                assert keys_of(resp["Items"]) == ["a/x", "a/y"], (operator, where)
                assert "NextToken" in resp, (operator, where)

    def test_an_index_read_is_fenced_on_the_index_partition_key(self, t):
        # Measured 2026-11-03.
        c, me = self._fenced("ForAllValues:StringEquals", ["a"])
        assert c.execute_statement(
            Statement=select(t.c, "g = 'a'", index="gix"))["Items"] == []
        self._denied(statement_error(c, select(t.c, "g = 'ga'", index="gix")),
                     me, "PartiQLSelect", self._arn(t.c, "gix"))

    def test_a_scan_carries_no_leading_keys(self, t):
        # Measured 2026-11-03: allowed under ForAllValues, refused under
        # ForAnyValue and under a bare StringEquals.
        c, _ = self._fenced("ForAllValues:StringEquals", ["a"])
        assert len(c.execute_statement(Statement=select(t.c))["Items"]) == 3
        assert len(c.execute_statement(Statement=select(t.c, "sk = 'x'"))["Items"]) == 2
        for operator, values in (("ForAnyValue:StringEquals", ["a"]),
                                 ("StringEquals", "a")):
            c, me = self._fenced(operator, values)
            self._denied(statement_error(c, select(t.c)),
                         me, "PartiQLSelect", self._arn(t.c))

    def test_a_bare_string_equals_never_matches_leading_keys(self, t):
        # Measured 2026-11-03.
        c, me = self._fenced("StringEquals", "a")
        for where in ("pk = 'a'", "pk IN ['a', 'b']"):
            self._denied(statement_error(c, select(t.c, where)),
                         me, "PartiQLSelect", self._arn(t.c))

    def test_a_write_is_fenced_on_its_item_partition_key(self, t):
        # Measured 2026-11-03.
        c, me = self._fenced("ForAllValues:StringEquals", ["a"])
        c.execute_statement(Statement=f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'a', 'sk': 'n4'}}")
        self._denied(statement_error(
            c, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'b', 'sk': 'n4'}}"),
            me, "PartiQLInsert", self._arn(t.c))
        c.execute_statement(Statement=f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}")
        self._denied(statement_error(c, f"UPDATE \"{t.c}\" SET q = 1 WHERE {BX}"),
                     me, "PartiQLUpdate", self._arn(t.c))
        self._denied(statement_error(
            c, f"DELETE FROM \"{t.c}\" WHERE pk = 'b' AND sk = 'zz'"),
            me, "PartiQLDelete", self._arn(t.c))
        c.execute_statement(Statement=f"DELETE FROM \"{t.c}\" WHERE pk = 'a' AND sk = 'zz'")
        assert self._items(t, t.c) == ["a/n4", "a/x", "a/y", "b/x"]

    def test_batch_and_transaction_statements_carry_their_own_keys(self, t):
        # Measured 2026-11-03.
        c, me = self._fenced("ForAllValues:StringEquals", ["a"])
        out = batch(c, [select(t.c, AX), select(t.c, BX)])
        assert "Item" in out[0]
        self._denied_entry(out[1], me, "PartiQLSelect", t.c)
        out = batch(c, [select(t.c, BX), select(t.c, AX)])
        self._denied_entry(out[0], me, "PartiQLSelect", t.c)
        assert "Item" in out[1]
        out = batch(c, [f"UPDATE \"{t.c}\" SET q = 2 WHERE {AX}",
                        f"UPDATE \"{t.c}\" SET q = 2 WHERE {BX}"])
        assert out[0] == {"TableName": t.c}
        self._denied_entry(out[1], me, "PartiQLUpdate", t.c)
        self._cancelled_by(cancelled(c, [select(t.c, AX), select(t.c, BX)]),
                           [None, ("PartiQLSelect", t.c)], me)
        self._cancelled_by(cancelled(c, [f"UPDATE \"{t.c}\" SET q = 3 WHERE {AX}",
                                         f"UPDATE \"{t.c}\" SET q = 3 WHERE {BX}"]),
                           [None, ("PartiQLUpdate", t.c)], me)

    # -- 5. the other condition keys ---------------------------------------------

    def test_full_table_scan_is_true_for_a_scan_and_false_for_a_read_by_key(self, t):
        # Measured 2026-11-03 with dynamodb:PartiQLSelect denied when
        # dynamodb:FullTableScan is true.
        c, me = self._client(
            self._allow(PARTIQL),
            self._deny(["dynamodb:PartiQLSelect"],
                       {"Bool": {"dynamodb:FullTableScan": "true"}}))
        for where in (None, "a = '1'", "pk = 'a' OR a = '1'"):
            self._denied(statement_error(c, select(t.c, where)),
                         me, "PartiQLSelect", self._arn(t.c), explicit=True)
        self._denied(statement_error(c, select(t.c, index="gix")),
                     me, "PartiQLSelect", self._arn(t.c, "gix"), explicit=True)
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        assert len(c.execute_statement(Statement=select(t.c, AX))["Items"]) == 1
        assert len(c.execute_statement(
            Statement=select(t.c, "g = 'ga'", index="gix"))["Items"]) == 1
        assert len(c.execute_statement(
            Statement=select(t.c, "pk IN ['a', 'b']"))["Items"]) == 3
        assert "Item" in batch(c, [select(t.c, AX)])[0]
        assert len(transaction(c, [select(t.c, AX)])["Responses"]) == 1
        c.execute_statement(Statement=f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}")

    def test_full_table_scan_false_allows_reads_by_key_only(self, t):
        # Measured 2026-11-03 with dynamodb:PartiQLSelect allowed only when
        # dynamodb:FullTableScan is false; the key is absent in a batch and a
        # transaction.
        c, me = self._client(self._allow(
            ["dynamodb:PartiQLSelect"], {"Bool": {"dynamodb:FullTableScan": "false"}}))
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        assert len(c.execute_statement(Statement=select(t.c, AX))["Items"]) == 1
        assert len(c.execute_statement(
            Statement=select(t.c, "g = 'ga'", index="gix"))["Items"]) == 1
        self._denied(statement_error(c, select(t.c)), me, "PartiQLSelect", self._arn(t.c))
        self._denied_entry(batch(c, [select(t.c, AX)])[0], me, "PartiQLSelect", t.c)
        self._cancelled_by(cancelled(c, [select(t.c, AX)]),
                           [("PartiQLSelect", t.c)], me)

    def test_enclosing_operation_names_the_batch_or_the_transaction(self, t):
        # Measured 2026-11-03: absent in ExecuteStatement, so a condition on
        # "ExecuteStatement" matches nothing.
        update = f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}"
        allowed = {
            "ExecuteTransaction": {"et"},
            "BatchExecuteStatement": {"bes"},
            "ExecuteStatement": set(),
        }
        for value, runs in allowed.items():
            c, me = self._client(self._allow(
                PARTIQL, {"StringEquals": {"dynamodb:EnclosingOperation": value}}))
            if "es" in runs:
                c.execute_statement(Statement=select(t.c, AX))
            else:
                self._denied(statement_error(c, select(t.c, AX)),
                             me, "PartiQLSelect", self._arn(t.c))
            for statement, action in ((select(t.c, AX), "PartiQLSelect"),
                                      (update, "PartiQLUpdate")):
                if "bes" in runs:
                    assert "Error" not in batch(c, [statement])[0], value
                else:
                    self._denied_entry(batch(c, [statement])[0], me, action, t.c)
                if "et" in runs:
                    transaction(c, [statement])
                else:
                    self._cancelled_by(cancelled(c, [statement]), [(action, t.c)], me)

    def test_enclosing_operation_is_absent_in_execute_statement(self, t):
        # Measured 2026-11-03.
        c, me = self._client(
            self._allow(PARTIQL),
            self._deny(PARTIQL, {"StringEquals": {
                "dynamodb:EnclosingOperation": "ExecuteTransaction"}}))
        assert len(c.execute_statement(Statement=select(t.c, AX))["Items"]) == 1
        assert "Item" in batch(c, [select(t.c, AX)])[0]
        self._cancelled_by(cancelled(c, [select(t.c, AX)]),
                           [("PartiQLSelect", t.c, "explicit")], me)
        c, me = self._client(self._allow(
            PARTIQL, {"Null": {"dynamodb:EnclosingOperation": "true"}}))
        assert len(c.execute_statement(Statement=select(t.c, AX))["Items"]) == 1
        self._denied_entry(batch(c, [select(t.c, AX)])[0], me, "PartiQLSelect", t.c)
        self._cancelled_by(cancelled(c, [select(t.c, AX)]),
                           [("PartiQLSelect", t.c)], me)

    def test_attributes_are_the_names_a_statement_names(self, t):
        # Measured 2026-11-03 under ForAllValues:StringEquals
        # dynamodb:Attributes [pk, sk, a, q]: a name in the WHERE counts as
        # one in the projection or a SET does.
        c, me = self._client(self._allow(PARTIQL, {
            "ForAllValues:StringEquals": {"dynamodb:Attributes": ["pk", "sk", "a", "q"]}}))
        assert len(c.execute_statement(
            Statement=select(t.c, "pk = 'a'", what="pk, a"))["Items"]) == 2
        assert len(c.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        for statement in (select(t.c, "pk = 'a'", what="g"),
                          select(t.c, "pk = 'a' AND g = 'ga'", what="pk")):
            self._denied(statement_error(c, statement), me, "PartiQLSelect", self._arn(t.c))
        c.execute_statement(
            Statement=f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n5', 'sk': 's', 'a': '1'}}")
        self._denied(statement_error(
            c, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n6', 'sk': 's', 'g': 'x'}}"),
            me, "PartiQLInsert", self._arn(t.c))
        c.execute_statement(Statement=f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}")
        for statement in (f"UPDATE \"{t.c}\" SET g = 'ga' WHERE {AX}",
                          f"UPDATE \"{t.c}\" SET q = 2 WHERE {AX} AND g = 'ga'"):
            self._denied(statement_error(c, statement), me, "PartiQLUpdate", self._arn(t.c))
        c.execute_statement(Statement=f"DELETE FROM \"{t.c}\" WHERE pk = 'zz' AND sk = 'zz'")

    def test_select_is_how_a_statement_projects(self, t):
        # Measured 2026-11-03.
        for value, runs, refused in (
                ("SPECIFIC_ATTRIBUTES", "pk", "*"),
                ("ALL_ATTRIBUTES", "*", "pk")):
            c, me = self._client(self._allow(
                ["dynamodb:PartiQLSelect"], {"StringEquals": {"dynamodb:Select": value}}))
            resp = c.execute_statement(Statement=select(t.c, "pk = 'a'", what=runs))
            assert len(resp["Items"]) == 2, value
            self._denied(statement_error(c, select(t.c, "pk = 'a'", what=refused)),
                         me, "PartiQLSelect", self._arn(t.c))

    def test_return_values_come_from_returning(self, t):
        # Measured 2026-11-03.
        c, me = self._client(self._allow(
            ["dynamodb:PartiQLUpdate", "dynamodb:PartiQLDelete"],
            {"StringEqualsIfExists": {"dynamodb:ReturnValues": "NONE"}}))
        c.execute_statement(Statement=f"UPDATE \"{t.c}\" SET q = 3 WHERE {AX}")
        self._denied(statement_error(
            c, f"UPDATE \"{t.c}\" SET q = 4 WHERE {AX} RETURNING ALL NEW *"),
            me, "PartiQLUpdate", self._arn(t.c))
        self._denied(statement_error(
            c, f"DELETE FROM \"{t.c}\" WHERE pk = 'zz' AND sk = 'zz' RETURNING ALL OLD *"),
            me, "PartiQLDelete", self._arn(t.c))
        c, me = self._client(self._allow(
            ["dynamodb:PartiQLUpdate"],
            {"StringEquals": {"dynamodb:ReturnValues": "ALL_NEW"}}))
        resp = c.execute_statement(
            Statement=f"UPDATE \"{t.c}\" SET q = 5 WHERE {AX} RETURNING ALL NEW *")
        assert len(resp["Items"]) == 1
        self._denied(statement_error(c, f"UPDATE \"{t.c}\" SET q = 6 WHERE {AX}"),
                     me, "PartiQLUpdate", self._arn(t.c))

    # -- session policies ----------------------------------------------------------

    def test_a_session_policy_narrows_each_statement(self, t):
        # Measured 2026-11-03: a session allowed PartiQLSelect alone is
        # refused an INSERT; one whose session policy denies H is refused H in
        # each operation while C runs.
        s = self._session({"Version": "2012-10-17", "Statement": [
            self._allow(["dynamodb:PartiQLSelect"], resources=[self._tables_pattern()[0]])]})
        assert len(s.execute_statement(Statement=select(t.c, "pk = 'a'"))["Items"]) == 2
        error = statement_error(s, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}")
        assert error["Code"] == "AccessDeniedException"
        assert (f" is not authorized to perform: dynamodb:PartiQLInsert on resource: "
                f"{self._arn(t.c)} ") in error["Message"]
        s = self._session({"Version": "2012-10-17", "Statement": [
            self._allow(["dynamodb:PartiQLSelect"]),
            self._deny(["dynamodb:PartiQLSelect"], resources=[self._arn(t.h)])]})
        error = statement_error(s, select(t.h, "pk = 'a'"))
        assert error["Code"] == "AccessDeniedException"
        out = batch(s, [select(t.c, AX), select(t.h, "pk = 'a'")])
        assert "Item" in out[0]
        assert out[1]["Error"]["Code"] == "AccessDenied"
        assert out[1]["TableName"] == t.h
        resp = cancelled(s, [select(t.c, AX), select(t.h, "pk = 'a'")])
        assert resp["Error"]["Message"] == CANCELLED + "[None, AccessDenied]"
        assert [r["Code"] for r in resp["CancellationReasons"]] == ["None", "AccessDenied"]

    @pytest.mark.xfail(
        reason="the shared authorization message names a role session "
               "arn:aws:iam::<acct>:assumed-role/... and an identity-based policy for "
               "every refusal; the service names arn:aws:sts::<acct>:assumed-role/... "
               "and the session policy",
        raises=AssertionError,
        strict=True,
    )
    def test_a_session_policy_refusal_names_the_session_and_its_policy(self, t):
        # Measured 2026-11-03.
        s = self._session({"Version": "2012-10-17", "Statement": [
            self._allow(["dynamodb:PartiQLSelect"]),
            self._deny(["dynamodb:PartiQLSelect"], resources=[self._arn(t.h)])]})
        roles = [r for r in self._roles]
        principal = f"arn:aws:sts::{self.account_id}:assumed-role/{roles[-1]}/pq-authz"
        head = f"User: {principal} is not authorized to perform: "
        error = statement_error(s, f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}")
        assert error["Message"] == (
            f"{head}dynamodb:PartiQLInsert on resource: {self._arn(t.c)} because no "
            f"session policy allows the dynamodb:PartiQLInsert action")
        error = statement_error(s, select(t.h, "pk = 'a'"))
        assert error["Message"] == (
            f"{head}dynamodb:PartiQLSelect on resource: {self._arn(t.h)} with an "
            f"explicit deny in a session policy")

    # -- every path asks for authorization ------------------------------------------

    def test_every_statement_path_is_authorized(self, t):
        # Each way a statement reads or writes items is refused for a caller
        # the policy gives no PartiQL action, and nothing is written.
        c, me = self._client(self._allow(CLASSIC))
        before = self._items(t, t.c)
        arn = self._arn(t.c)
        for statement in (
                select(t.c),
                select(t.c, "a = '1'"),
                select(t.c, AX),
                select(t.c, "pk = 'a'"),
                select(t.c, "pk IN ['a', 'b']"),
                select(t.c, "pk = 'a' AND sk IN ['x', 'y']"),
                select(t.c, "pk IN ['a', 'b'] ORDER BY pk DESC"),
        ):
            self._denied(statement_error(c, statement), me, "PartiQLSelect", arn)
        for where in ("g = 'ga'", "g IN ['ga', 'gb']", None):
            self._denied(statement_error(c, select(t.c, where, index="gix")),
                         me, "PartiQLSelect", self._arn(t.c, "gix"))
        for statement, action in (
                (f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}", "PartiQLInsert"),
                (f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}", "PartiQLUpdate"),
                (f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX} RETURNING ALL NEW *", "PartiQLUpdate"),
                (f"DELETE FROM \"{t.c}\" WHERE {AX}", "PartiQLDelete"),
                (f"DELETE FROM \"{t.c}\" WHERE {AX} RETURNING ALL OLD *", "PartiQLDelete"),
        ):
            self._denied(statement_error(c, statement), me, action, arn)
        out = batch(c, [select(t.c, AX), select(t.c, BX)])
        for entry in out:
            self._denied_entry(entry, me, "PartiQLSelect", t.c)
        out = batch(c, [f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}",
                        f"UPDATE \"{t.c}\" SET q = 1 WHERE {AX}",
                        f"DELETE FROM \"{t.c}\" WHERE {BX}"])
        for entry, action in zip(out, ("PartiQLInsert", "PartiQLUpdate", "PartiQLDelete")):
            self._denied_entry(entry, me, action, t.c)
        self._cancelled_by(cancelled(c, [select(t.c, AX), select(t.c, BX)]),
                           [("PartiQLSelect", t.c), ("PartiQLSelect", t.c)], me)
        self._cancelled_by(
            cancelled(c, [f"EXISTS(SELECT * FROM \"{t.c}\" WHERE {AX} AND a = '1')",
                          f"INSERT INTO \"{t.c}\" VALUE {{'pk': 'n', 'sk': 'n'}}",
                          f"UPDATE \"{t.c}\" SET q = 1 WHERE {BX}",
                          f"DELETE FROM \"{t.c}\" WHERE pk = 'a' AND sk = 'y'"]),
            [("PartiQLSelect", t.c), ("PartiQLInsert", t.c), ("PartiQLUpdate", t.c),
             ("PartiQLDelete", t.c)], me)
        assert self._items(t, t.c) == before

    def test_every_page_is_authorized(self, t):
        # A NextToken carries no permission: the request that presents it is
        # authorized as the first one was.
        owner = t.owner
        c, me = self._client(self._allow(CLASSIC))
        for statement in (select(t.c), select(t.c, "pk IN ['a', 'b']")):
            resp = owner.execute_statement(Statement=statement, Limit=1)
            assert "NextToken" in resp, statement
            self._denied(statement_error(c, statement, NextToken=resp["NextToken"]),
                         me, "PartiQLSelect", self._arn(t.c))
        fenced, fenced_arn = self._fenced("ForAllValues:StringEquals", ["b"])
        statement = select(t.c, "pk = 'a'")
        resp = owner.execute_statement(Statement=statement, Limit=1)
        self._denied(statement_error(fenced, statement, NextToken=resp["NextToken"]),
                     fenced_arn, "PartiQLSelect", self._arn(t.c))
