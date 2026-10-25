# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Expression operator cap, placeholder key length and syntax error wording.

Three request shapes Amazon DynamoDB refuses with texts ExtendDB did not match:

  - an expression with more than 300 operators:
    ``Invalid <Kind>: The expression contains too many operators; operator
    count: <n>``, on ConditionExpression (PutItem, UpdateItem, DeleteItem),
    FilterExpression (Scan, Query) and KeyConditionExpression (Query). Every
    comparison operator, AND, OR, NOT, BETWEEN, IN and function call counts
    one; parentheses, paths, placeholders and list elements count zero.
  - an ExpressionAttributeNames or ExpressionAttributeValues key over 255
    bytes on PutItem: a ValidationException carrying ``size of key: <n>``,
    not a SerializationException.
  - a repeated or misplaced operator or keyword: ``Syntax error; token: "<t>",
    near: "<window>"`` where the window is the source text from the token
    before the offending one to the token after it, and ``<EOF>`` with the
    last token as the window when the input ends early.

Every expected text was measured against Amazon DynamoDB on 2026-10-25
(us-east-1); the probe case id is noted on each assertion. The write paths
carry a ``1 validation error detected: `` prefix on the service that ExtendDB
does not emit; those assertions compare the text after the prefix.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

PREFIX = "1 validation error detected: "

TOO_MANY = "The expression contains too many operators; operator count: "

def _error(func) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


def _validation(func) -> str:
    code, msg = _error(func)
    assert code == "ValidationException", (code, msg)
    return msg


def _after_prefix(msg: str) -> str:
    return msg[len(PREFIX) :] if msg.startswith(PREFIX) else msg


def chain(term: str, n: int, joiner: str = " OR ") -> str:
    return joiner.join([term] * n)


def syntax(kind: str, token: str, near: str) -> str:
    return f'Invalid {kind}: Syntax error; token: "{token}", near: "{near}"'


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """Composite-key table with one item for the Query and Scan rows."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    ) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "v": {"S": "x"}},
        )
        yield name


NAMES = {"#v": "v"}
VALUES = {":v": {"S": "x"}}


def put(client, table, condition, **kwargs):
    args = {
        "TableName": table,
        "Item": {"pk": {"S": "absent"}, "sk": {"S": "absent"}},
        "ConditionExpression": condition,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.put_item(**args)


def scan(client, table, filter_expr, **kwargs):
    args = {
        "TableName": table,
        "FilterExpression": filter_expr,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.scan(**args)


def query(client, table, kce, **kwargs):
    args = {
        "TableName": table,
        "KeyConditionExpression": kce,
        "ExpressionAttributeNames": NAMES,
        "ExpressionAttributeValues": VALUES,
    }
    args.update(kwargs)
    return lambda: client.query(**args)


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
            ("a46", chain("attribute_type(#v, :v)", 150)),  # 299
        ],
    )
    def test_put_item_condition_at_or_under_the_cap_is_evaluated(
        self, dynamodb_client, table, case, expr
    ):
        # The condition is false for the absent key, so nothing is written.
        code, _ = _error(put(dynamodb_client, table, expr))
        assert code == "ConditionalCheckFailedException", case

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


class TestPlaceholderKeyLength:
    """A placeholder key over 255 bytes is a ValidationException with its size."""

    @staticmethod
    def _names_msg(field: str, size: int) -> str:
        return (
            f"{field} contains invalid key: The expression attribute map contains "
            f"a key that is too long; size of key: {size}"
        )

    def test_put_item_names_key(self, dynamodb_client_no_validation, table):
        # [c01] 255 accepted (condition evaluated), 256 and 257 refused.
        ok = "#" + "a" * 254
        code, _ = _error(
            put(dynamodb_client_no_validation, table, f"{ok} = :v", ExpressionAttributeNames={ok: "v"})
        )
        assert code == "ConditionalCheckFailedException"
        for size in (256, 257):
            key = "#" + "a" * (size - 1)
            msg = _validation(
                put(dynamodb_client_no_validation, table, f"{key} = :v", ExpressionAttributeNames={key: "v"})
            )
            assert _after_prefix(msg) == self._names_msg("ExpressionAttributeNames", size)

    def test_put_item_values_key(self, dynamodb_client_no_validation, table):
        # [c02] 255 accepted, 256 and 257 refused with the size suffix.
        ok = ":" + "a" * 254
        code, _ = _error(
            put(dynamodb_client_no_validation, table, f"#v = {ok}", ExpressionAttributeValues={ok: {"S": "x"}})
        )
        assert code == "ConditionalCheckFailedException"
        for size in (256, 257):
            key = ":" + "a" * (size - 1)
            msg = _validation(
                put(
                    dynamodb_client_no_validation,
                    table,
                    f"#v = {key}",
                    ExpressionAttributeValues={key: {"S": "x"}},
                )
            )
            assert _after_prefix(msg) == self._names_msg("ExpressionAttributeValues", size)

    def test_put_item_key_length_is_bytes_and_checked_before_syntax(
        self, dynamodb_client_no_validation, table
    ):
        # [c03] 127 two-byte characters: 255 bytes, passes the length check and
        # fails key syntax; [c04] 128: 257 bytes; [c05] 258 bytes with an
        # invalid character reports the length.
        key = "#" + "\u00e9" * 127
        msg = _validation(
            put(dynamodb_client_no_validation, table, "#v = :v", ExpressionAttributeNames={"#v": "v", key: "w"})
        )
        assert _after_prefix(msg) == (
            f'ExpressionAttributeNames contains invalid key: Syntax error; key: "{key}"'
        )
        key = "#" + "\u00e9" * 128
        msg = _validation(
            put(dynamodb_client_no_validation, table, "#v = :v", ExpressionAttributeNames={"#v": "v", key: "w"})
        )
        assert _after_prefix(msg) == self._names_msg("ExpressionAttributeNames", 257)
        key = "#" + "a" * 255 + "-x"
        msg = _validation(
            put(dynamodb_client_no_validation, table, "#v = :v", ExpressionAttributeNames={"#v": "v", key: "w"})
        )
        assert _after_prefix(msg) == self._names_msg("ExpressionAttributeNames", 258)

    def test_put_item_too_long_key_beats_the_expression_checks(
        self, dynamodb_client_no_validation, table
    ):
        # [c06] unreferenced, [c07] with a 300-term chain, [c08] with a syntax
        # error, [c09] with an undefined name, [c10] with an unused value.
        key = "#" + "w" * 255
        names = {"#v": "v", key: "w"}
        expected = self._names_msg("ExpressionAttributeNames", 256)
        for condition, extra in [
            ("#v = :v", {}),
            (chain("#v = :v", 300), {}),
            ("#v = = :v", {}),
            ("#x = :v", {}),
            ("#v = :v", {"ExpressionAttributeValues": {":v": {"S": "x"}, ":u": {"S": "y"}}}),
        ]:
            msg = _validation(
                put(dynamodb_client_no_validation, table, condition, ExpressionAttributeNames=names, **extra)
            )
            assert _after_prefix(msg) == expected, condition

    def test_scan_query_update_keys(self, dynamodb_client_no_validation, table):
        # [c12] Scan names, [s10] Scan values, [s11] Query values, [s12] Query
        # names: no prefix. [c13] UpdateItem names: prefix on the service.
        nkey = "#" + "w" * 255
        vkey = ":" + "w" * 255
        assert _validation(
            scan(dynamodb_client_no_validation, table, f"{nkey} = :v", ExpressionAttributeNames={nkey: "w"})
        ) == self._names_msg("ExpressionAttributeNames", 256)
        assert _validation(
            scan(dynamodb_client_no_validation, table, f"#v = {vkey}", ExpressionAttributeValues={vkey: {"S": "x"}})
        ) == self._names_msg("ExpressionAttributeValues", 256)
        assert _validation(
            query(
                dynamodb_client_no_validation,
                table,
                "pk = :p",
                FilterExpression=f"#v = {vkey}",
                ExpressionAttributeValues={":p": {"S": "p1"}, vkey: {"S": "x"}},
            )
        ) == self._names_msg("ExpressionAttributeValues", 256)
        assert _validation(
            query(
                dynamodb_client_no_validation,
                table,
                "pk = :v",
                ExpressionAttributeNames={nkey: "w"},
            )
        ) == self._names_msg("ExpressionAttributeNames", 256)
        msg = _validation(
            lambda: dynamodb_client_no_validation.update_item(
                TableName=table,
                Key={"pk": {"S": "absent"}, "sk": {"S": "absent"}},
                UpdateExpression=f"SET {nkey} = :v",
                ExpressionAttributeNames={nkey: "w"},
                ExpressionAttributeValues=VALUES,
            )
        )
        assert _after_prefix(msg) == self._names_msg("ExpressionAttributeNames", 256)


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
