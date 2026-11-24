# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Idempotency tokens and the per-table concurrency guard."""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import tempfile
import time
import uuid

import pytest
import requests

from bulk_put.bulk_helpers import *  # noqa: F401,F403

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="bulk operations tests require extenddb (EXTENDDB_TEST_ENDPOINT)",
)


class TestIdempotency:
    """ClientToken replay and mismatch."""

    def test_a_replayed_token_returns_the_same_operation_and_creates_no_second(
        self, bulk_table, bulk_bucket
    ):
        write_rows(bulk_bucket, "in/rows.json", [row("i", "1")])
        token = f"token-{uuid.uuid4().hex}"
        body = start_body(bulk_table, bulk_bucket, ClientToken=token)

        before = {s["BulkOperationArn"] for s in list_all_operations()}
        first = extenddb_request("StartBulkPutItems", body)
        first_arn = first["BulkOperationDescription"]["BulkOperationArn"]
        second = extenddb_request("StartBulkPutItems", body)
        second_arn = second["BulkOperationDescription"]["BulkOperationArn"]

        assert first_arn == second_arn
        # Compare ARN sets rather than counts so another test starting an
        # operation on this account at the same time cannot mask a second
        # operation created by the replay: any new operation other than the
        # first must belong to some other table.
        after = {s["BulkOperationArn"] for s in list_all_operations()}
        new_arns = after - before
        assert first_arn in new_arns
        table_id = first["BulkOperationDescription"]["TableDetails"][0]["TableId"]
        for arn in new_arns - {first_arn}:
            other = extenddb_request(
                "DescribeBulkOperation", {"BulkOperationArn": arn}
            )["BulkOperationDescription"]
            assert (
                other["TableDetails"][0]["TableId"] != table_id
            ), "the replay created a second operation"
        wait_until_terminal(first_arn)

    def test_a_reused_token_with_different_parameters_is_rejected(
        self, bulk_table, bulk_bucket
    ):
        write_rows(bulk_bucket, "in/rows.json", [row("j", "1")])
        token = f"token-{uuid.uuid4().hex}"
        first = extenddb_request(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, ClientToken=token,
                       MaxWriteRequestUnits=1024),
        )
        expect_error(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, ClientToken=token,
                       MaxWriteRequestUnits=2048),
            "IdempotentParameterMismatchException",
            IDEMPOTENT_MISMATCH_MESSAGE,
        )
        wait_until_terminal(first["BulkOperationDescription"]["BulkOperationArn"])


# ---------------------------------------------------------------------------
# Per-table guard
# ---------------------------------------------------------------------------


class TestPerTableGuard:
    """At most one non-terminal operation per table."""

    # Enough rows that the operation is still running when the second start
    # arrives a few milliseconds later, on every attempt.
    LONG_RUN_ROWS = 20000

    def test_a_second_start_on_a_busy_table_is_rejected_on_every_attempt(
        self, bulk_table, bulk_bucket
    ):
        rows = [row("k", str(i), f={"S": "x" * 40}) for i in range(self.LONG_RUN_ROWS)]
        write_rows(bulk_bucket, "in/rows.json", rows)

        acceptances = 0
        for attempt in range(20):
            # Each Start carries its own token, so the second one is not a
            # replay of the first.
            first = extenddb_request(
                "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
            )
            arn = first["BulkOperationDescription"]["BulkOperationArn"]

            status_code, result = _signed_post(
                "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
            )
            if status_code < 400:
                acceptances += 1
            else:
                got_type = result.get("__type", "").split("#")[-1]
                got_message = result.get("message", result.get("Message", ""))
                assert got_type == "ResourceInUseException", (
                    f"attempt {attempt}: {got_type}: {got_message}"
                )
                assert got_message == TABLE_BUSY_MESSAGE

            # End the operation so the next attempt starts fresh.
            extenddb_request("CancelBulkOperation", {"BulkOperationArn": arn})
            wait_until_terminal(arn)

        assert acceptances == 0, f"{acceptances} of 20 second starts were accepted"

    def test_a_start_after_the_previous_operation_is_terminal_is_accepted(
        self, bulk_table, bulk_bucket
    ):
        write_rows(bulk_bucket, "in/rows.json", [row("m", "1")])
        first = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        wait_until_terminal(first["BulkOperationDescription"]["BulkOperationArn"])

        second = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        desc = second["BulkOperationDescription"]
        assert desc["BulkOperationStatus"] == "IN_PROGRESS"
        wait_until_terminal(desc["BulkOperationArn"])


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _length_refusal(token: str) -> str:
    return (
        f"Invalid Request: 1 validation error detected: Value '{token}' at "
        "'clientToken' failed to satisfy constraint: Member must have length "
        "less than or equal to 1024"
    )


def _token_of(shape: str) -> str:
    """A token of one measured shape, with 32 characters taken from a fresh
    UUID: ASCII hex digits, or one two-byte character per digit where the
    shape fixes the UTF-8 length."""
    unique = uuid.uuid4().hex
    wide = "".join(chr(0xC0 + int(digit, 16)) for digit in unique)
    return {
        "1024-ascii": unique + "a" * 992,
        "1025-ascii": unique + "a" * 993,
        "513-chars-1025-bytes": wide + "\u00e9" * 480 + "a",
        "emoji-1024-chars": "\U0001F600" + unique + "a" * 991,
        "emoji-1025-chars": "\U0001F600" + unique + "a" * 992,
    }[shape]


class TestClientToken:
    """The ClientToken bounds, measured on the service on 2026-11-19."""

    def test_a_start_without_a_token_is_refused(self, bulk_table, bulk_bucket):
        body = start_body(bulk_table, bulk_bucket)
        del body["ClientToken"]
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            "Invalid Request: 1 validation error detected: Value null at "
            "'clientToken' failed to satisfy constraint: Member must not be null",
        )

    # The length is counted in characters (Unicode code points), not bytes.
    # Each case gets a token of its own, so a second run within the
    # eight-hour window does not replay another table's operation.
    @pytest.mark.parametrize("shape,chars,utf8_bytes,accepted", [
        ("1024-ascii", 1024, 1024, True),
        ("1025-ascii", 1025, 1025, False),
        ("513-chars-1025-bytes", 513, 1025, True),
        ("emoji-1024-chars", 1024, 1027, True),
        ("emoji-1025-chars", 1025, 1028, False),
    ])
    def test_the_token_length_is_counted_in_characters(
        self, bulk_table, bulk_bucket, shape, chars, utf8_bytes, accepted
    ):
        token = _token_of(shape)
        assert (len(token), len(token.encode("utf-8"))) == (chars, utf8_bytes)
        body = start_body(bulk_table, bulk_bucket, ClientToken=token)
        if not accepted:
            expect_error(
                "StartBulkPutItems", body, "ValidationException",
                _length_refusal(token),
            )
            return
        desc = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
        assert desc["ClientToken"] == token
        wait_until_terminal(desc["BulkOperationArn"])

    def test_a_token_holding_nul_replays_its_operation(self, bulk_table, bulk_bucket):
        token = f"nul\u0000{uuid.uuid4()}"
        body = start_body(bulk_table, bulk_bucket, ClientToken=token)
        first = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
        assert first["ClientToken"] == token
        replay = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
        assert replay["BulkOperationArn"] == first["BulkOperationArn"]
        expect_error(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, out_prefix="other", ClientToken=token),
            "IdempotentParameterMismatchException",
            IDEMPOTENT_MISMATCH_MESSAGE,
        )
        final = wait_until_terminal(first["BulkOperationArn"])
        assert final["ClientToken"] == token
