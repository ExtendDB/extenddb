# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle of a bulk operation: rows, reports, compression."""

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


class TestLifecycle:
    """A full run: start, poll to terminal, read items, read the report."""

    def test_the_description_carries_table_details_and_resolved_members(
        self, bulk_table, bulk_bucket
    ):
        # Measured on the service: the Start response and
        # every Describe carry TableDetails as a one-entry list of the
        # bare table name and the table id; an absent S3BucketOwner is
        # echoed as the caller's account id on both configurations and an
        # absent InputCompressionType as NONE.
        body = start_body(bulk_table, bulk_bucket)
        resp = extenddb_request("StartBulkPutItems", body)
        desc = resp["BulkOperationDescription"]
        details = desc["TableDetails"]
        assert len(details) == 1
        assert details[0]["TableName"] == bulk_table
        assert details[0]["TableId"]
        assert desc["InputS3Configuration"]["S3BucketOwner"] == ACCOUNT_ID
        assert desc["OutputS3Configuration"]["S3BucketOwner"] == ACCOUNT_ID
        assert desc["InputS3Configuration"]["InputCompressionType"] == "NONE"

        described = extenddb_request(
            "DescribeBulkOperation",
            {"BulkOperationArn": desc["BulkOperationArn"]},
        )["BulkOperationDescription"]
        assert described["TableDetails"] == details
        assert described["InputS3Configuration"]["S3BucketOwner"] == ACCOUNT_ID

    def test_a_run_with_one_bad_row_completes_with_errors_and_a_report(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        rows = [
            row("a", "1", n={"N": "1"}),
            row("b", "1", n={"N": "2"}),
            row("c", None, n={"N": "3"}),  # index 2: missing the sort key
            row("d", "1", n={"N": "4"}),
            row("e", "1", n={"N": "5"}),
        ]
        input_size = write_rows(bulk_bucket, "in/rows.json", rows)

        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        desc = resp["BulkOperationDescription"]
        assert desc["BulkOperationStatus"] == "IN_PROGRESS"
        assert desc["TotalInputBytes"] == input_size
        arn = desc["BulkOperationArn"]
        match = OPERATION_ARN_PATTERN.match(arn)
        assert match, f"unexpected operation ARN shape: {arn}"
        assert match.group(1) == ACCOUNT_ID

        terminal = wait_until_terminal(arn)
        assert terminal["BulkOperationStatus"] == "COMPLETED_WITH_ERRORS"
        assert terminal["ProcessedItemCount"] == 5
        assert terminal["FailedItemCount"] == 1
        assert terminal["ProcessedInputBytes"] == terminal["TotalInputBytes"]
        assert "EndTime" in terminal
        usage = terminal["BulkOperationUsage"]
        assert len(usage) == 1
        assert "BulkPutBytes" in usage[0]

        for pk in ("a", "b", "d", "e"):
            item = dynamodb_client.get_item(
                TableName=bulk_table, Key={"pk": {"S": pk}, "sk": {"S": "1"}}
            )
            assert "Item" in item, f"item {pk} was not written"

        records = read_report(bulk_bucket, "reports", arn, "errors")
        assert records is not None, report_key("reports", arn, "errors")
        assert len(records) == 1
        record = records[0]
        assert record["itemLocation"]["inputSourceArn"] == (
            f"arn:aws:s3:::{bulk_bucket}/in/rows.json"
        )
        assert record["itemLocation"]["itemIndex"] == 2
        assert record["errorMessages"][0] == MISSING_KEY_MESSAGE
        assert record["newImage"] == {"Item": rows[2]["Item"]}

    def test_gzip_input_is_read_and_counted_in_compressed_bytes(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        rows = [row("g", str(i)) for i in range(5)]
        input_size = write_rows(bulk_bucket, "in/rows.json.gz", rows, compress=True)

        body = start_body(bulk_table, bulk_bucket)
        body["InputS3Configuration"]["InputCompressionType"] = "GZIP"
        resp = extenddb_request("StartBulkPutItems", body)
        desc = resp["BulkOperationDescription"]
        assert desc["TotalInputBytes"] == input_size

        terminal = wait_until_terminal(desc["BulkOperationArn"])
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["ProcessedItemCount"] == 5
        assert terminal["FailedItemCount"] == 0
        for i in range(5):
            item = dynamodb_client.get_item(
                TableName=bulk_table, Key={"pk": {"S": "g"}, "sk": {"S": str(i)}}
            )
            assert "Item" in item

    def test_a_false_condition_is_recorded_but_not_counted_as_failed(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        stored = {"pk": {"S": "held"}, "sk": {"S": "1"}, "v": {"S": "before"}}
        dynamodb_client.put_item(TableName=bulk_table, Item=stored)

        true_row = {
            "Item": {"pk": {"S": "fresh"}, "sk": {"S": "1"}},
            "ConditionExpression": "attribute_not_exists(pk)",
        }
        false_row = {
            "Item": {"pk": {"S": "held"}, "sk": {"S": "1"}, "v": {"S": "after"}},
            "ConditionExpression": "attribute_not_exists(pk)",
        }
        write_rows(bulk_bucket, "in/rows.json", [true_row, false_row])

        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        arn = resp["BulkOperationDescription"]["BulkOperationArn"]
        terminal = wait_until_terminal(arn)
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["ProcessedItemCount"] == 2
        assert terminal["FailedItemCount"] == 0

        # The guarded item keeps its stored value.
        kept = dynamodb_client.get_item(
            TableName=bulk_table, Key={"pk": {"S": "held"}, "sk": {"S": "1"}}
        )["Item"]
        assert kept["v"]["S"] == "before"

        records = read_report(
            bulk_bucket, "reports", arn, "condition-check-failures"
        )
        assert records is not None, report_key(
            "reports", arn, "condition-check-failures"
        )
        assert len(records) == 1
        record = records[0]
        assert record["errorMessages"][0] == CONDITION_FAILED_MESSAGE
        assert record["newImage"] == {"Item": false_row["Item"]}
        assert record["oldImage"] == {"Item": stored}

    def test_an_input_matching_no_files_completes_with_zero_counters(
        self, bulk_table, bulk_bucket
    ):
        resp = extenddb_request(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, in_prefix="nothing-here/"),
        )
        desc = resp["BulkOperationDescription"]
        assert desc["TotalInputBytes"] == 0

        arn = desc["BulkOperationArn"]
        terminal = wait_until_terminal(arn)
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["ProcessedItemCount"] == 0
        assert terminal["FailedItemCount"] == 0
        assert terminal["ProcessedInputBytes"] == 0
        assert read_report(bulk_bucket, "reports", arn, "errors") is None

    def test_a_bucket_that_does_not_exist_fails_the_operation_at_start(
        self, bulk_table
    ):
        absent = f"absent-{uuid.uuid4().hex[:12]}"
        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, absent)
        )
        desc = resp["BulkOperationDescription"]
        assert desc["BulkOperationStatus"] == "FAILED"
        assert desc["FailureCode"] == "S3NoSuchBucket"
        # The message is the store's own: test_store_rules.py pins each.
        assert desc["ProcessedItemCount"] == 0
        # Born terminal before enumeration: the hosted service omits
        # TotalInputBytes here while keeping the counters at zero.
        assert "TotalInputBytes" not in desc
        assert "EndTime" in desc

        # Describe answers the same terminal row: the operation was
        # inserted already finished, with no worker involvement.
        arn = desc["BulkOperationArn"]
        described = extenddb_request(
            "DescribeBulkOperation", {"BulkOperationArn": arn}
        )["BulkOperationDescription"]
        assert described["BulkOperationStatus"] == "FAILED"
        assert described["FailureCode"] == "S3NoSuchBucket"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------
