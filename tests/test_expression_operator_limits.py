# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Expression operator cap.

Amazon DynamoDB refuses an expression with more than 300 operators:
``Invalid <Kind>: The expression contains too many operators; operator
count: <n>``, on ConditionExpression (PutItem, UpdateItem, DeleteItem),
FilterExpression (Scan, Query) and KeyConditionExpression (Query). Every
comparison operator, AND, OR, NOT, BETWEEN, IN and function call counts one;
parentheses, paths, placeholders and list elements count zero. An undefined
placeholder and an operand of the wrong type come before the count, and so
do the Scan Limit, Segment and TotalSegments ranges. PartiQL counts the
statement, not the text it writes for it.

The syntax error wording tests are in test_expression_syntax_wording.py and
the placeholder key length tests in test_expression_placeholder_key_length.py.

Every expected text was measured against Amazon DynamoDB on 2026-10-25,
2026-11-19, 2026-11-20 or 2026-11-23 (us-east-1); the probe case id is noted
on each assertion. The write paths carry a ``1 validation error detected: ``
prefix on the service that ExtendDB does not emit; those assertions compare
the text after the prefix.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from conftest import _is_real_dynamodb, configured_backend
from expression_fault_helpers import (
    NAMES, VALUES, _after_prefix, _error, _validation, chain, put, query, scan, syntax, table,
)

TOO_MANY = "The expression contains too many operators; operator count: "

# On MongoDB an UpdateItem condition is compiled to a filter whose OR and AND
# chains nest one level per operator, and a chain of 150 terms fails inside
# MongoDB with InternalServerError. This change does not touch that compiler.
# The backend is read as the other tests read it: EXTENDDB_TEST_BACKEND, else
# the server's config file.
# TODO: remove this marker once the MongoDB filter compiler flattens chains.
XFAIL_ON_MONGODB_DEEP_OR = pytest.mark.xfail(
    not _is_real_dynamodb() and configured_backend() == "mongodb",
    reason="MongoDB answers InternalServerError for a condition chain of 150 terms",
    raises=AssertionError,
    strict=True,
)


class TestOperatorCount:
    """More than 300 operators is refused with the full count."""

    @pytest.mark.parametrize(
        "case, expr, count",
        [
            ("a01", "attribute_not_exists(pk) OR " + chain("#v = :v", 300), 601),
            ("a02", chain("#v = :v", 300), 599),
            ("a03", chain("#v = :v", 300, " AND "), 599),
            ("a05", chain("#v <> :v", 300), 599),
            ("a09", chain("#v >= :v", 300), 599),
            ("a10", chain("NOT #v = :v", 200), 599),
            ("a22", chain("NOT NOT #v = :v", 200), 799),
            ("a12", chain("#v IN (:v)", 200), 399),
            ("a19", chain("(#v = :v)", 300), 599),
            ("a40", chain("#v BETWEEN :v AND :v", 160), 319),
            ("a41", chain("#v IN (:v, :v, :v)", 170), 339),
            ("a43", chain("attribute_exists(#v)", 170), 339),
            ("a45", chain("contains(#v, :v)", 200), 399),
            ("a47", chain("size(#v) = :v", 230), 689),
            ("a48", chain("#v = :v AND #v = :v", 170), 679),
            ("a51", chain("((#v = :v) OR (#v = :v))", 140), 559),
            ("a52", "attribute_exists(pk) AND NOT (" + chain("#v = :v", 150) + ")", 302),
            ("a53", chain("size(#v) BETWEEN :v AND :v", 130), 389),
            ("a30", chain("#v = :v", 151), 301),
            ("a50", "NOT attribute_not_exists(pk) OR " + chain("#v = :v", 150), 302),
        ],
    )
    def test_put_item_condition_count(self, dynamodb_client, table, case, expr, count):
        msg = _validation(put(dynamodb_client, table, expr))
        assert _after_prefix(msg) == f"Invalid ConditionExpression: {TOO_MANY}{count}", case

    @pytest.mark.parametrize(
        "case, expr",
        [
            ("a30", chain("#v = :v", 150)),  # 299
            ("a49", "NOT attribute_not_exists(pk) OR " + chain("#v = :v", 149)),  # 300
            ("a42", chain("#v IN (:v, :v, :v, :v, :v)", 130)),  # 259
            ("a54", chain("#v IN (:v)", 150)),  # 299
        ],
    )
    def test_put_item_condition_at_or_under_the_cap_is_evaluated(
        self, dynamodb_client, table, case, expr
    ):
        # The condition is false for the absent key, so nothing is written.
        code, _ = _error(put(dynamodb_client, table, expr))
        assert code == "ConditionalCheckFailedException", case

    def test_put_item_attribute_type_terms_are_evaluated(self, dynamodb_client, table):
        # [a46] 150 calls: 299. The type name must be one the service knows:
        # with "x" it answers "Invalid attribute type name found" (measured
        # 2026-11-19), so the value is "S".
        code, _ = _error(
            put(
                dynamodb_client,
                table,
                chain("attribute_type(#v, :t)", 150),
                ExpressionAttributeValues={":t": {"S": "S"}},
            )
        )
        assert code == "ConditionalCheckFailedException"

    def test_update_item_condition_count(self, dynamodb_client, table):
        # [u01] 599 refused; [u03] 300 accepted (condition evaluated).
        msg = _validation(
            lambda: dynamodb_client.update_item(
                TableName=table,
                Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                UpdateExpression="SET #w = :v",
                ConditionExpression=chain("#v = :v", 300),
                ExpressionAttributeNames={"#v": "v", "#w": "w"},
                ExpressionAttributeValues=VALUES,
            )
        )
        assert _after_prefix(msg) == f"Invalid ConditionExpression: {TOO_MANY}599"

    @XFAIL_ON_MONGODB_DEEP_OR
    def test_update_item_condition_at_the_cap_is_evaluated(self, dynamodb_client, table):
        # [u03] 300 accepted (condition evaluated).
        code, _ = _error(
            lambda: dynamodb_client.update_item(
                TableName=table,
                Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                UpdateExpression="SET #w = :v",
                ConditionExpression="NOT attribute_not_exists(pk) OR " + chain("#v = :v", 149),
                ExpressionAttributeNames={"#v": "v", "#w": "w"},
                ExpressionAttributeValues=VALUES,
            )
        )
        assert code == "ConditionalCheckFailedException"

    def test_delete_item_condition_count(self, dynamodb_client, table):
        # [d01]
        msg = _validation(
            lambda: dynamodb_client.delete_item(
                TableName=table,
                Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                ConditionExpression=chain("#v = :v", 300),
                ExpressionAttributeNames=NAMES,
                ExpressionAttributeValues=VALUES,
            )
        )
        assert _after_prefix(msg) == f"Invalid ConditionExpression: {TOO_MANY}599"

    def test_scan_filter_count(self, dynamodb_client, table):
        # [b01] 599 refused, no prefix; [b02] 299 accepted; [b03] 301 refused;
        # [a57] BETWEEN counts one on Scan too.
        msg = _validation(scan(dynamodb_client, table, chain("#v = :v", 300)))
        assert msg == f"Invalid FilterExpression: {TOO_MANY}599"
        assert "Count" in scan(dynamodb_client, table, chain("#v = :v", 150))()
        msg = _validation(scan(dynamodb_client, table, chain("#v = :v", 151)))
        assert msg == f"Invalid FilterExpression: {TOO_MANY}301"
        msg = _validation(scan(dynamodb_client, table, chain("#v BETWEEN :v AND :v", 160)))
        assert msg == f"Invalid FilterExpression: {TOO_MANY}319"

    def test_query_filter_count(self, dynamodb_client, table):
        # [b06]
        msg = _validation(
            query(
                dynamodb_client,
                table,
                "pk = :p",
                FilterExpression=chain("#v = :v", 300),
                ExpressionAttributeValues={":v": {"S": "x"}, ":p": {"S": "p1"}},
            )
        )
        assert msg == f"Invalid FilterExpression: {TOO_MANY}599"

    def test_key_condition_count(self, dynamodb_client, table):
        # [b04] 601; [a58] BETWEEN; [a59] begins_with; [a61] 151 equalities.
        rows = [
            ("b04", "pk = :v AND " + chain("#v = :v", 300, " AND "), 601),
            ("a58", "pk = :v AND " + chain("#v BETWEEN :v AND :v", 150, " AND "), 301),
            ("a59", "pk = :v AND " + chain("begins_with(#v, :v)", 170, " AND "), 341),
            ("a61", chain("#v = :v", 151, " AND "), 301),
        ]
        for case, kce, count in rows:
            msg = _validation(query(dynamodb_client, table, kce))
            assert msg == f"Invalid KeyConditionExpression: {TOO_MANY}{count}", case

    def test_key_condition_count_precedes_the_key_grammar(self, dynamodb_client, table):
        # [a62] `pk = :v OR` + 150 terms answers the count, not the operator
        # refusal. The service also runs the count ahead of its
        # one-condition-per-key check (b05, a60); ExtendDB has no such check.
        msg = _validation(query(dynamodb_client, table, "pk = :v OR " + chain("#v = :v", 150, " AND ")))
        assert msg == f"Invalid KeyConditionExpression: {TOO_MANY}301"


class TestOperatorCountOrder:
    """Where the count falls against the other request checks."""

    def test_syntax_error_beats_the_count(self, dynamodb_client, table):
        # [e04], [e05]
        msg = _validation(put(dynamodb_client, table, chain("#v = :v", 300) + " = ="))
        assert _after_prefix(msg) == syntax("ConditionExpression", "=", ":v = =")
        msg = _validation(put(dynamodb_client, table, "= = " + chain("#v = :v", 300)))
        assert _after_prefix(msg) == syntax("ConditionExpression", "=", "= =")

    def test_redundant_parentheses_and_reserved_word_beat_the_count(self, dynamodb_client, table):
        # [p14], [p12]
        msg = _validation(put(dynamodb_client, table, "((#v = :v)) OR " + chain("#v = :v", 299)))
        assert _after_prefix(msg) == (
            "Invalid ConditionExpression: The expression has redundant parentheses;"
        )
        msg = _validation(put(dynamodb_client, table, chain("#v = :v", 299) + " OR status = :v"))
        assert _after_prefix(msg) == (
            "Invalid ConditionExpression: Attribute name is a reserved keyword; "
            "reserved keyword: status"
        )

    def test_ordering_operand_type_beats_the_count(self, dynamodb_client, table):
        # [p17]
        msg = _validation(
            put(
                dynamodb_client,
                table,
                chain("#v = :v", 299) + " OR #v < :b",
                ExpressionAttributeValues={":v": {"S": "x"}, ":b": {"BOOL": True}},
            )
        )
        assert _after_prefix(msg) == (
            "Invalid ConditionExpression: Incorrect operand type for operator or "
            "function; operator or function: <, operand type: BOOL"
        )

    def test_count_beats_unused_value_missing_table_and_key_mismatch(
        self, dynamodb_client, table
    ):
        # [e02], [p05], [p06]
        expected = f"Invalid ConditionExpression: {TOO_MANY}599"
        msg = _validation(
            put(
                dynamodb_client,
                table,
                chain("#v = :v", 300),
                ExpressionAttributeValues={":v": {"S": "x"}, ":u": {"S": "y"}},
            )
        )
        assert _after_prefix(msg) == expected
        msg = _validation(
            put(dynamodb_client, "extenddb-missing-b501", chain("#v = :v", 300))
        )
        assert _after_prefix(msg) == expected
        msg = _validation(
            put(dynamodb_client, table, chain("#v = :v", 300), Item={"pk": {"N": "1"}, "sk": {"S": "a"}})
        )
        assert _after_prefix(msg) == expected

    def test_scan_count_beats_unused_missing_table_and_unknown_index(
        self, dynamodb_client, table
    ):
        # [s02], [s07], [s08]
        expected = f"Invalid FilterExpression: {TOO_MANY}599"
        msg = _validation(
            scan(
                dynamodb_client,
                table,
                chain("#v = :v", 300),
                ExpressionAttributeValues={":v": {"S": "x"}, ":u": {"S": "y"}},
            )
        )
        assert msg == expected
        assert _validation(scan(dynamodb_client, "extenddb-missing-b501", chain("#v = :v", 300))) == expected
        assert _validation(scan(dynamodb_client, table, chain("#v = :v", 300), IndexName="nope")) == expected

    def test_update_item_condition_count_beats_update_expression_faults(
        self, dynamodb_client, table
    ):
        # [u04] update syntax error, [u07] update undefined name: the condition's
        # count is reported first.
        for update in ("SET #w = = :v", "SET #w = :v"):
            msg = _validation(
                lambda update=update: dynamodb_client.update_item(
                    TableName=table,
                    Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                    UpdateExpression=update,
                    ConditionExpression=chain("#v = :v", 300),
                    ExpressionAttributeNames=NAMES,
                    ExpressionAttributeValues=VALUES,
                )
            )
            assert _after_prefix(msg) == f"Invalid ConditionExpression: {TOO_MANY}599", update


UNDEFINED_VALUE = (
    "An expression attribute value used in expression is not defined; attribute value: :zz"
)
UNDEFINED_NAME = (
    "An expression attribute name used in the document path is not defined; attribute name: #x"
)
BOOL_OPERAND = (
    "Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"
)
N_PREFIX = (
    "Incorrect operand type for operator or function; operator or function: begins_with, "
    "operand type: N"
)
B = {":b": {"BOOL": True}}
N = {":n": {"N": "1"}}
U = {":u": {"S": "y"}}
P = {":p": {"S": "p1"}}

# Faults the service reports ahead of the count, measured 2026-11-19 with the
# same answers on Scan and on Query. Each row names its Scan case, then its
# Query case.
AHEAD_OF_THE_COUNT = [
    ("s01", "q01", chain("#v = :v", 150) + " OR #v < :b", B, BOOL_OPERAND),
    ("s02", "q03", chain("#v = :v", 151) + " OR #v = :zz", {}, UNDEFINED_VALUE),
    ("s03", "q04", chain("#v = :v", 151) + " OR begins_with(#v, :n)", N, N_PREFIX),
    ("s04", "q02", chain("#v = :v", 151) + " OR #x = :v", {}, UNDEFINED_NAME),
]


class TestChecksAheadOfTheCount:
    """Undefined placeholders and operand types come before the count."""

    @pytest.mark.parametrize("case, query_case, expr, values, want", AHEAD_OF_THE_COUNT)
    def test_scan_filter(self, dynamodb_client, table, case, query_case, expr, values, want):
        msg = _validation(
            scan(dynamodb_client, table, expr, ExpressionAttributeValues={**VALUES, **values})
        )
        assert msg == f"Invalid FilterExpression: {want}", case

    @pytest.mark.parametrize("scan_case, case, expr, values, want", AHEAD_OF_THE_COUNT)
    def test_query_filter(self, dynamodb_client, table, scan_case, case, expr, values, want):
        msg = _validation(
            query(
                dynamodb_client,
                table,
                "pk = :p",
                FilterExpression=expr,
                ExpressionAttributeValues={**VALUES, **P, **values},
            )
        )
        assert msg == f"Invalid FilterExpression: {want}", case

    def test_query_count_beats_an_unused_value(self, dynamodb_client, table):
        # [q05]
        msg = _validation(
            query(
                dynamodb_client,
                table,
                "pk = :p",
                FilterExpression=chain("#v = :v", 151),
                ExpressionAttributeValues={**VALUES, **P, **U},
            )
        )
        assert msg == f"Invalid FilterExpression: {TOO_MANY}301"

    @pytest.mark.parametrize(
        "case, kce, values, want",
        [
            ("k20", "pk = :zz AND " + chain("#v = :v", 150, " AND "), {}, UNDEFINED_VALUE),
            ("k21", "#x = :p AND " + chain("#v = :v", 150, " AND "), P, UNDEFINED_NAME),
            (
                "k22",
                "pk = :p AND begins_with(sk, :n) AND " + chain("#v = :v", 149, " AND "),
                {**P, **N},
                N_PREFIX,
            ),
            (
                "k26",
                "pk = :p AND " + chain("#v = :v", 150, " AND ") + " AND #y = :v",
                P,
                UNDEFINED_NAME.replace("#x", "#y"),
            ),
            (
                "k27",
                "pk = :p AND sk < :b AND " + chain("#v = :v", 149, " AND "),
                {**P, **B},
                BOOL_OPERAND,
            ),
        ],
    )
    def test_key_condition(self, dynamodb_client, table, case, kce, values, want):
        # Measured 2026-11-20: a key condition over the cap answers its
        # undefined placeholder or operand type fault, not the count.
        msg = _validation(
            query(dynamodb_client, table, kce, ExpressionAttributeValues={**VALUES, **values})
        )
        assert msg == f"Invalid KeyConditionExpression: {want}", case

    def test_write_conditions(self, dynamodb_client, table):
        # [p01], [p02], [p03] on PutItem; [u01] on UpdateItem; [d01] on DeleteItem.
        key = {"pk": {"S": "absent"}, "sk": {"S": "absent"}}
        undefined = chain("#v = :v", 151) + " OR #v = :zz"
        rows = [
            ("p01", put(dynamodb_client, table, undefined), UNDEFINED_VALUE),
            (
                "p02",
                put(dynamodb_client, table, chain("#v = :v", 151) + " OR #x = :v"),
                UNDEFINED_NAME,
            ),
            (
                "p03",
                put(
                    dynamodb_client,
                    table,
                    chain("#v = :v", 151) + " OR begins_with(#v, :n)",
                    ExpressionAttributeValues={**VALUES, **N},
                ),
                N_PREFIX,
            ),
            (
                "u01",
                lambda: dynamodb_client.update_item(
                    TableName=table,
                    Key=key,
                    UpdateExpression="SET #w = :v",
                    ConditionExpression=undefined,
                    ExpressionAttributeNames={"#v": "v", "#w": "w"},
                    ExpressionAttributeValues=VALUES,
                ),
                UNDEFINED_VALUE,
            ),
            (
                "d01",
                lambda: dynamodb_client.delete_item(
                    TableName=table,
                    Key=key,
                    ConditionExpression=undefined,
                    ExpressionAttributeNames=NAMES,
                    ExpressionAttributeValues=VALUES,
                ),
                UNDEFINED_VALUE,
            ),
        ]
        for case, request, want in rows:
            assert _after_prefix(_validation(request)) == f"Invalid ConditionExpression: {want}", case

    def test_the_checks_read_no_item(self, dynamodb_client, table):
        # Measured 2026-11-19: the undefined value is found where evaluation
        # would skip it or never run, and ahead of the other request faults.
        rows = [
            ("p04", put(dynamodb_client, table, "attribute_not_exists(pk) OR #v = :zz"), "Condition", UNDEFINED_VALUE),
            (
                "q06",
                query(
                    dynamodb_client,
                    table,
                    "pk = :p",
                    FilterExpression="#v = :zz",
                    ExpressionAttributeValues={**VALUES, ":p": {"S": "nothing"}},
                ),
                "Filter",
                UNDEFINED_VALUE,
            ),
            (
                "s07",
                scan(dynamodb_client, table, "#v = :zz", ExpressionAttributeValues={**VALUES, **U}),
                "Filter",
                UNDEFINED_VALUE,
            ),
            ("s08", scan(dynamodb_client, "extenddb-missing-b501", "#v = :zz"), "Filter", UNDEFINED_VALUE),
            (
                "s10",
                scan(
                    dynamodb_client,
                    table,
                    "#v = :zz OR #v < :b",
                    ExpressionAttributeValues={**VALUES, **B},
                ),
                "Filter",
                UNDEFINED_VALUE,
            ),
            (
                "s13",
                scan(
                    dynamodb_client,
                    table,
                    "#v < :b",
                    ExpressionAttributeValues={**VALUES, **B},
                    ProjectionExpression="#v, , #v",
                ),
                "Filter",
                BOOL_OPERAND,
            ),
        ]
        for case, request, kind, want in rows:
            assert _after_prefix(_validation(request)) == f"Invalid {kind}Expression: {want}", case


# The range texts: the service's, then ExtendDB's. ExtendDB keeps its own
# wording for the three Scan ranges.
RANGE_TEXTS = {
    "Limit": (
        "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
        "Member must have value greater than or equal to 1",
        "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: "
        "Member must have value greater than or equal to 1",
    ),
    "TotalSegments": (
        "1 validation error detected: Value at 'TotalSegments' failed to satisfy constraint: "
        "Member must have value greater than or equal to 1",
        "The parameter TotalSegments should be greater than or equal to 1",
    ),
}


class TestScanRangesAheadOfTheCount:
    """The Scan Limit, Segment and TotalSegments ranges come first."""

    @pytest.mark.parametrize(
        "case, expr, values, extra, field",
        [
            ("s05", chain("#v = :v", 151), {}, {"Limit": 0}, "Limit"),
            ("s16", "#v < :b", B, {"Limit": 0}, "Limit"),
            ("s17", "#v = :zz", {}, {"Limit": 0}, "Limit"),
            ("s22", "#v = = :v", {}, {"Limit": 0}, "Limit"),
            ("s06", chain("#v = :v", 151), {}, {"Segment": 0, "TotalSegments": 0}, "TotalSegments"),
            ("s41", chain("#v = :v", 151), {}, {"TotalSegments": 0}, "TotalSegments"),
        ],
    )
    def test_range_beats_the_filter_checks(
        self, dynamodb_client_no_validation, table, case, expr, values, extra, field
    ):
        # Measured 2026-11-19, and s41 on 2026-11-23: a TotalSegments out of
        # range without a Segment answers the range, not the missing partner.
        msg = _validation(
            scan(
                dynamodb_client_no_validation,
                table,
                expr,
                ExpressionAttributeValues={**VALUES, **values},
                **extra,
            )
        )
        assert msg == RANGE_TEXTS[field][0 if _is_real_dynamodb() else 1], (case, msg)

    def test_segment_pairing_comes_after_the_count(self, dynamodb_client_no_validation, table):
        # [s27] a Segment without TotalSegments beside 151 terms.
        msg = _validation(
            scan(dynamodb_client_no_validation, table, chain("#v = :v", 151), Segment=0)
        )
        assert msg == f"Invalid FilterExpression: {TOO_MANY}301"


class TestPartiqlStatementCount:
    """PartiQL counts a statement, not the text it writes for the statement."""

    def test_not_over_and_joined_terms_runs(self, dynamodb_client, table):
        # [e01]-[e04] measured 2026-11-19: the service runs each statement.
        key = {"pk": {"S": "p2"}, "sk": {"S": "s2"}}
        dynamodb_client.put_item(TableName=table, Item={**key, "v": {"S": "x"}})
        try:
            def not_and(n):
                return "NOT (" + " AND ".join(["v = 'y'"] * n) + ")"

            items = dynamodb_client.execute_statement(
                Statement=f'SELECT * FROM "{table}" WHERE {not_and(150)}'
            )["Items"]
            assert {"S": "p2"} in [item["pk"] for item in items]
            items = dynamodb_client.execute_statement(
                Statement=f"SELECT * FROM \"{table}\" WHERE pk = 'p2' AND {not_and(101)}"
            )["Items"]
            assert [item["pk"] for item in items] == [{"S": "p2"}]
            where = f"pk = 'p2' AND sk = 's2' AND {not_and(101)}"
            dynamodb_client.execute_statement(Statement=f'UPDATE "{table}" SET w = 1 WHERE {where}')
            item = dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
            assert item["w"] == {"N": "1"}
            dynamodb_client.execute_statement(Statement=f'DELETE FROM "{table}" WHERE {where}')
            assert "Item" not in dynamodb_client.get_item(
                TableName=table, Key=key, ConsistentRead=True
            )
        finally:
            dynamodb_client.delete_item(TableName=table, Key=key)
