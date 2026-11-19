# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Syntax error wording: the token and the window of a repeated or misplaced
operator or keyword, measured against Amazon DynamoDB (us-east-1).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from expression_fault_helpers import (
    NAMES, VALUES, _after_prefix, _validation, put, query, scan, syntax, table,
)


class TestSyntaxErrorWording:
    """Token and window for a repeated or misplaced operator or keyword."""

    @pytest.mark.parametrize(
        "case, expr, token, near",
        [
            ("d01", "#v = = :v", "=", "= = :v"),
            ("d02", "attribute_not_exists(pk) OR #v = = :v", "=", "= = :v"),
            ("d13", "attribute_exists(pk) AND #v = :v AND #v = = :v", "=", "= = :v"),
            ("d03", "= #v = :v", "=", "= #v"),
            ("d04", "#v = :v =", "=", ":v ="),
            ("d05", "#v = :v AND", "<EOF>", "AND"),
            ("d06", "#v = :v OR", "<EOF>", "OR"),
            ("d07", "#v = :v AND AND #v = :v", "AND", "AND AND #v"),
            ("d08", "#v = :v OR OR #v = :v", "OR", "OR OR #v"),
            ("d09", "AND #v = :v", "AND", "AND #v"),
            ("d10", "#v < > :v", ">", "< > :v"),
            ("d11", "#v = :v #v = :v", "#v", ":v #v ="),
            ("d14", "#v <> <> :v", "<>", "<> <> :v"),
            ("d15", "#v >= >= :v", ">=", ">= >= :v"),
            ("d16", "#v = = = :v", "=", "= = ="),
            ("d17", "#v==:v", "=", "==:v"),
            ("d18", "#v =\t= :v", "=", "=\t= :v"),
            ("d19", "#v = :v AND NOT", "<EOF>", "NOT"),
            ("d29", "=", "=", "="),
            ("d30", "= =", "=", "= ="),
            ("lower-case and", "#v = :v and and #v = :v", "and", "and and #v"),
        ],
    )
    def test_put_item_condition(self, dynamodb_client, table, case, expr, token, near):
        msg = _validation(put(dynamodb_client, table, expr))
        assert _after_prefix(msg) == syntax("ConditionExpression", token, near), case

    def test_put_item_long_tokens(self, dynamodb_client, table):
        # [d12]
        msg = _validation(
            put(
                dynamodb_client,
                table,
                "#longname = = :longvalue",
                ExpressionAttributeNames={"#longname": "v"},
                ExpressionAttributeValues={":longvalue": {"S": "x"}},
            )
        )
        assert _after_prefix(msg) == syntax("ConditionExpression", "=", "= = :longvalue")

    def test_scan_filter(self, dynamodb_client, table):
        # [d20], [d21]: no prefix on Scan.
        assert _validation(scan(dynamodb_client, table, "#v = = :v")) == syntax(
            "FilterExpression", "=", "= = :v"
        )
        assert _validation(scan(dynamodb_client, table, "#v = :v AND")) == syntax(
            "FilterExpression", "<EOF>", "AND"
        )
        # Measured 2026-10-25: a lower-case keyword is shown as written.
        assert _validation(scan(dynamodb_client, table, "#v = :v and and #v = :v")) == syntax(
            "FilterExpression", "and", "and and #v"
        )

    def test_fault_at_the_first_token_keeps_leading_whitespace(self, dynamodb_client, table):
        # Measured 2026-10-25: the window starts at the start of the expression.
        for expr, token, near in [
            ("  = #v = :v", "=", "  = #v"),
            ("\t= #v = :v", "=", "\t= #v"),
            ("  AND #v = :v", "AND", "  AND #v"),
            ("= #v = :v", "=", "= #v"),
        ]:
            assert _validation(scan(dynamodb_client, table, expr)) == syntax(
                "FilterExpression", token, near
            ), expr
        assert _validation(query(dynamodb_client, table, "  = pk = :v")) == syntax(
            "KeyConditionExpression", "=", "  = pk"
        )
        msg = _validation(
            lambda: dynamodb_client.get_item(
                TableName=table,
                Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}},
                ProjectionExpression="  , #v",
                ExpressionAttributeNames=NAMES,
            )
        )
        assert msg == syntax("ProjectionExpression", ",", "  , #v")

    def test_query_key_condition(self, dynamodb_client, table):
        # [d22], [d23]
        assert _validation(query(dynamodb_client, table, "pk = = :v")) == syntax(
            "KeyConditionExpression", "=", "= = :v"
        )
        assert _validation(query(dynamodb_client, table, "pk = :v AND")) == syntax(
            "KeyConditionExpression", "<EOF>", "AND"
        )
        # Measured 2026-10-25: a lower-case keyword in a key condition.
        lower = query(
            dynamodb_client,
            table,
            "pk = :p and and sk = :s",
            ExpressionAttributeValues={":p": {"S": "p1"}, ":s": {"S": "a"}, ":v": {"S": "x"}},
        )
        assert _validation(lower) == syntax("KeyConditionExpression", "and", "and and sk")

    @pytest.mark.parametrize(
        "case, expr, token, near",
        [
            ("d24", "SET #v = = :v", "=", "= = :v"),
            ("d25", "SET #v = :v + + :v", "+", "+ + :v"),
            ("d26", "SET #v = :v,", "<EOF>", ","),
            ("b07", "SET #v = :v + :v + :v", "+", ":v + :v"),
            ("u08", "SET #v = :v +", "<EOF>", "+"),
            ("u10", "SET #v = :v REMOVE", "<EOF>", "REMOVE"),
            ("u11", "SET = :v", "=", "SET = :v"),
            ("u12", "SET #v :v", ":v", "#v :v"),
        ],
    )
    def test_update_expression(self, dynamodb_client, table, case, expr, token, near):
        msg = _validation(
            lambda: dynamodb_client.update_item(
                TableName=table,
                Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                UpdateExpression=expr,
                ExpressionAttributeNames=NAMES,
                ExpressionAttributeValues=VALUES,
            )
        )
        assert _after_prefix(msg) == syntax("UpdateExpression", token, near), case

    def test_projection_expression(self, dynamodb_client, table):
        # [d27], [d28]
        for expr, token, near in [("#v, , #v", ",", ", , #v"), ("#v,", "<EOF>", ",")]:
            msg = _validation(
                lambda expr=expr: dynamodb_client.get_item(
                    TableName=table,
                    Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}},
                    ProjectionExpression=expr,
                    ExpressionAttributeNames=NAMES,
                )
            )
            assert msg == syntax("ProjectionExpression", token, near), expr

    def test_syntax_error_beats_the_other_expression_checks(self, dynamodb_client, table):
        # [e06] undefined name, [e07] undefined value, [e08] unused value,
        # [e09] unused name, [e10] operand type.
        rows = [
            ("#w = = :v", ":v", {}),
            ("#v = = :u", ":u", {}),
            ("#v = = :v", ":v", {"ExpressionAttributeValues": {":v": {"S": "x"}, ":u": {"S": "y"}}}),
            ("#v = = :v", ":v", {"ExpressionAttributeNames": {"#v": "v", "#x": "x"}}),
            (
                "begins_with(#v, :n) AND #v = = :v",
                ":v",
                {"ExpressionAttributeValues": {":v": {"S": "x"}, ":n": {"N": "1"}}},
            ),
        ]
        for condition, last, extra in rows:
            msg = _validation(put(dynamodb_client, table, condition, **extra))
            assert _after_prefix(msg) == syntax("ConditionExpression", "=", f"= = {last}"), condition


INVALID_KEY_OPERATOR = "Invalid KeyConditionExpression: Invalid operator used in KeyConditionExpression"


class TestSyntaxFaultForms:
    """Shapes measured 2026-11-19 (us-east-1); probe case ids on each row."""

    @staticmethod
    def _update(client, table, expr, names=None, values=None):
        args = {
            "TableName": table,
            "Key": {"pk": {"S": "absent"}, "sk": {"S": "absent"}},
            "UpdateExpression": expr,
            "ExpressionAttributeNames": names or NAMES,
        }
        if values is not None:
            args["ExpressionAttributeValues"] = values
        return lambda: client.update_item(**args)

    @staticmethod
    def _get(client, table, projection, names=None):
        return lambda: client.get_item(
            TableName=table,
            Key={"pk": {"S": "p1"}, "sk": {"S": "s1"}},
            ProjectionExpression=projection,
            ExpressionAttributeNames=names or NAMES,
        )

    def test_end_of_input_keeps_the_trailing_whitespace(self, dynamodb_client, table):
        values = {":p": {"S": "p1"}, ":v": {"S": "x"}}
        rows = [
            ("x01", scan(dynamodb_client, table, "#v = :v AND   "), "Filter", "AND   "),
            ("x04", scan(dynamodb_client, table, "#v = :v AND\t"), "Filter", "AND\t"),
            ("e10", scan(dynamodb_client, table, "  NOT"), "Filter", "NOT"),
            (
                "x05",
                query(dynamodb_client, table, "pk = :p AND   ", ExpressionAttributeValues=values),
                "KeyCondition",
                "AND   ",
            ),
            ("x06", self._get(dynamodb_client, table, "#v,   "), "Projection", ",   "),
            (
                "x02",
                self._update(dynamodb_client, table, "SET #v = :v,   ", values=VALUES),
                "Update",
                ",   ",
            ),
        ]
        for case, request, kind, near in rows:
            msg = _after_prefix(_validation(request))
            assert msg == syntax(f"{kind}Expression", "<EOF>", near), case

    def test_add_and_delete_values(self, dynamodb_client, table):
        both = {"#v": "v", "#w": "w"}
        rows = [
            ("x07", "ADD #v = :v", None, VALUES, "=", "#v = :v"),
            ("x08", "DELETE #v = :v", None, VALUES, "=", "#v = :v"),
            ("x09", "ADD #v", None, None, "<EOF>", "#v"),
            ("x10", "DELETE #v", None, None, "<EOF>", "#v"),
            ("x11", "ADD #v #w", both, None, "#w", "#v #w"),
        ]
        for case, expr, names, values, token, near in rows:
            request = self._update(dynamodb_client, table, expr, names=names, values=values)
            msg = _after_prefix(_validation(request))
            assert msg == syntax("UpdateExpression", token, near), case

    def test_update_window_is_found_by_token_position(self, dynamodb_client, table):
        # [x26] A search for the token's text would quote "+ :b, #w".
        values = {k: {"N": "1"} for k in (":a", ":b", ":c", ":d", ":e")}
        request = self._update(
            dynamodb_client,
            table,
            "SET #v = :a + :b, #w = :c + :d + :e",
            names={"#v": "v", "#w": "w"},
            values=values,
        )
        msg = _after_prefix(_validation(request))
        assert msg == syntax("UpdateExpression", "+", ":d + :e")

    def test_projection_faults(self, dynamodb_client, table):
        rows = [
            ("x13", "#v = #v", None, "=", "#v = #v"),
            ("x14", "#v AND #v", None, "AND", "#v AND #v"),
            ("x15", "#v.", None, "<EOF>", "."),
            ("j01", "#v #w", {"#v": "v", "#w": "w"}, "#w", "#v #w"),
            ("j06", "#v[1", None, "<EOF>", "1"),
        ]
        for case, projection, names, token, near in rows:
            msg = _validation(self._get(dynamodb_client, table, projection, names=names))
            assert msg == syntax("ProjectionExpression", token, near), case

    def test_in_without_a_parenthesis(self, dynamodb_client, table):
        rows = [
            ("x17", put(dynamodb_client, table, "#v IN IN (:v)"), "Condition", "IN", "IN IN ("),
            ("i01", put(dynamodb_client, table, "#v IN :v"), "Condition", ":v", "IN :v"),
            ("i02", put(dynamodb_client, table, "#v IN"), "Condition", "<EOF>", "IN"),
            ("x18", scan(dynamodb_client, table, "#v IN IN (:v)"), "Filter", "IN", "IN IN ("),
        ]
        for case, request, kind, token, near in rows:
            msg = _after_prefix(_validation(request))
            assert msg == syntax(f"{kind}Expression", token, near), case

    def test_key_condition_operators(self, dynamodb_client, table):
        p = {":p": {"S": "p1"}}
        ps = {":p": {"S": "p1"}, ":s": {"S": "s1"}}
        rows = [
            ("x20", "NOT pk = :p", p, INVALID_KEY_OPERATOR),
            ("x21", "pk = :p AND NOT sk = :s", ps, INVALID_KEY_OPERATOR),
            ("x25", "pk = :p OR sk = :s", ps, INVALID_KEY_OPERATOR),
            ("x22", "pk = :p AND NOT", p, syntax("KeyConditionExpression", "<EOF>", "NOT")),
            ("x23", "pk = :p OR", p, syntax("KeyConditionExpression", "<EOF>", "OR")),
            ("x24", "pk = :p =", p, syntax("KeyConditionExpression", "=", ":p =")),
            ("k01", "pk = :p sk = :s", ps, syntax("KeyConditionExpression", "sk", ":p sk =")),
            ("k05", "pk = :p OR OR sk = :s", ps, syntax("KeyConditionExpression", "OR", "OR OR sk")),
        ]
        for case, kce, values, want in rows:
            msg = _validation(
                lambda kce=kce, values=values: dynamodb_client.query(
                    TableName=table, KeyConditionExpression=kce, ExpressionAttributeValues=values
                )
            )
            assert msg == want, case
