# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Placeholder key length in the expression attribute maps.

An ExpressionAttributeNames or ExpressionAttributeValues key over 255 bytes
is a ValidationException carrying ``size of key: <n>``, not a
SerializationException, on PutItem as on Scan, Query and UpdateItem. The
bound is bytes of UTF-8, and it is checked before the expression checks.

Every expected text was measured against Amazon DynamoDB on 2026-10-25,
2026-11-19, 2026-11-20 or 2026-11-23 (us-east-1); the probe case id is noted
on each assertion. The write paths carry a ``1 validation error detected: ``
prefix on the service that ExtendDB does not emit; those assertions compare
the text after the prefix.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

from expression_fault_helpers import (
    VALUES, _after_prefix, _error, _validation, chain, put, query, scan, table,
)


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
