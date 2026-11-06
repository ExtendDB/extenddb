# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Request validation, ARN parsing, and cancel outcomes."""

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


class TestValidation:
    """Field constraints, unsupported features, and the ARN layers."""

    def test_input_format_ion_is_refused(self, bulk_bucket):
        body = start_body("orders", bulk_bucket)
        body["InputS3Configuration"]["InputFormat"] = "ION"
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            "InputFormat ION is not supported by this server",
        )

    def test_zstd_input_is_read_and_counted_in_compressed_bytes(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        # One zstd frame of five rows {"Item": {"pk": {"S": "z"}, "sk":
        # {"S": "<i>"}}} for i in 0..5, written by the reference encoder
        # at level 19 without a checksum; embedded so the suite needs no
        # zstd module. Measured on the service: a
        # zstd input is read and TotalInputBytes is the compressed size.
        frame = bytes.fromhex(
            "28b52ffd0068cd010082420910a06d0cf43a257e7dd5819e62523f6b60a0eda6"
            "9001da2edc193a3ccf69b5639e997ae8180600035306e60d4c462a086b1aadab"
            "7c01"
        )
        write_object(bulk_bucket, "in/rows.json.zst", frame)

        body = start_body(bulk_table, bulk_bucket)
        body["InputS3Configuration"]["InputCompressionType"] = "ZSTD"
        resp = extenddb_request("StartBulkPutItems", body)
        desc = resp["BulkOperationDescription"]
        assert desc["InputS3Configuration"]["InputCompressionType"] == "ZSTD"
        assert desc["TotalInputBytes"] == len(frame)

        terminal = wait_until_terminal(desc["BulkOperationArn"])
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["ProcessedItemCount"] == 5
        assert terminal["FailedItemCount"] == 0
        assert terminal["ProcessedInputBytes"] == len(frame)
        for i in range(5):
            item = dynamodb_client.get_item(
                TableName=bulk_table, Key={"pk": {"S": "z"}, "sk": {"S": str(i)}}
            )
            assert "Item" in item

    def test_max_results_above_the_ceiling_is_refused(self):
        expect_error(
            "ListBulkOperations", {"MaxResults": 26},
            "ValidationException",
            "1 validation error detected: Value '26' at 'maxResults' failed to "
            "satisfy constraint: Member must have value less than or equal to 25",
        )

    def test_max_write_request_units_zero_is_refused(self, bulk_bucket):
        expect_error(
            "StartBulkPutItems",
            start_body("orders", bulk_bucket, MaxWriteRequestUnits=0),
            "ValidationException",
            "1 validation error detected: Value '0' at 'maxWriteRequestUnits' "
            "failed to satisfy constraint: Member must have value greater than "
            "or equal to 1024",
        )

    def test_an_empty_output_type_list_is_refused(self, bulk_bucket):
        expect_error(
            "StartBulkPutItems",
            start_body("orders", bulk_bucket, output_types=[]),
            "ValidationException",
            "Invalid Request: OutputS3Configuration.OutputType cannot be empty",
        )

    def test_an_output_configuration_without_a_bucket_is_refused(self, bulk_bucket):
        # Measured on the service: the output bucket member is
        # checked at Start, not at the first report record.
        body = start_body("orders", bulk_bucket)
        body["OutputS3Configuration"].pop("S3Bucket", None)
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            "Invalid Request: OutputS3Configuration.S3Bucket is invalid",
        )

    def test_a_dotted_ip_bucket_name_is_refused(self, bulk_bucket):
        # Measured on the service: a bucket name spelled as a dotted
        # IPv4 literal is refused even though it satisfies the name
        # pattern and both length bounds.
        body = start_body("orders", bulk_bucket)
        body["InputS3Configuration"]["S3Bucket"] = "10.20.30.40"
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            "Invalid Request: InputS3Configuration.S3Bucket is invalid",
        )

    @pytest.mark.parametrize("bucket", ["s3://loads/orders", "loads/orders"])
    def test_a_bucket_given_as_a_uri_or_with_a_key_path_is_refused(self, bucket):
        # Measured on the service: S3Bucket names a bucket only, and a
        # URI or a bucket followed by a key path is refused at once with
        # the model's pattern clause for the member.
        body = start_body("orders", "loads")
        body["InputS3Configuration"]["S3Bucket"] = bucket
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            f"1 validation error detected: Value '{bucket}' at "
            "'inputS3Configuration.s3Bucket' failed to satisfy constraint: "
            r"Member must satisfy regular expression pattern: "
            r"^[a-z0-9A-Z]+[\.\-\w]*[a-z0-9A-Z]+$",
        )

    @filesystem_store_only
    def test_a_prefix_with_a_parent_component_is_refused(self, bulk_bucket):
        expect_error(
            "StartBulkPutItems",
            start_body("orders", bulk_bucket, in_prefix="in/../out/"),
            "ValidationException",
            "S3KeyPrefix contains an invalid path component",
        )

    @s3_store_only
    def test_the_s3_store_accepts_a_prefix_the_path_rules_refuse(
        self, bulk_table, bulk_bucket
    ):
        # The path rules are the filesystem store's; an S3 key prefix may
        # hold a backslash.
        body = start_body(bulk_table, bulk_bucket, in_prefix="in\\x/")
        desc = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
        assert desc["InputS3Configuration"]["S3KeyPrefix"] == "in\\x/"
        final = wait_until_terminal(desc["BulkOperationArn"])
        assert final["BulkOperationStatus"] == "COMPLETED", final
        assert final["ProcessedItemCount"] == 0

    def test_a_foreign_account_table_arn_is_refused(self, bulk_bucket):
        foreign = f"arn:aws:dynamodb:{REGION}:999999999999:table/orders"
        expect_error(
            "StartBulkPutItems",
            start_body(foreign, bulk_bucket),
            "AccessDeniedException",
            "Access is denied",
        )

    def test_a_foreign_region_table_arn_is_refused(self, bulk_bucket):
        foreign = f"arn:aws:dynamodb:eu-west-1:{ACCOUNT_ID}:table/orders"
        expect_error(
            "StartBulkPutItems",
            start_body(foreign, bulk_bucket),
            "ValidationException",
            f"1 validation error detected: Invalid AWS region in '{foreign}'",
        )

    def test_a_string_that_is_not_an_operation_arn_is_refused(self):
        expect_error(
            "DescribeBulkOperation", {"BulkOperationArn": "orders"},
            "ValidationException", ARN_SHAPE_MESSAGE,
        )

    def test_an_operation_arn_with_a_malformed_id_is_refused(self):
        arn = (
            f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:bulk-operation/"
            f"{uuid.uuid4()}"
        )
        expect_error(
            "DescribeBulkOperation", {"BulkOperationArn": arn},
            "ValidationException", ARN_ID_MESSAGE,
        )

    def test_a_well_formed_arn_for_no_operation_answers_not_found(self):
        arn = (
            f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:bulk-operation/"
            "00000000000001-deadbeef"
        )
        expect_error(
            "DescribeBulkOperation", {"BulkOperationArn": arn},
            "ResourceNotFoundException",
            f"Bulk operation with arn: {arn} does not exist",
        )

    def test_a_cancel_on_a_terminal_operation_names_the_status(
        self, bulk_table, bulk_bucket
    ):
        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        arn = resp["BulkOperationDescription"]["BulkOperationArn"]
        terminal = wait_until_terminal(arn)
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        expect_error(
            "CancelBulkOperation", {"BulkOperationArn": arn},
            "ValidationException",
            "Invalid Request: Operation already in terminal state: COMPLETED",
        )

    def test_a_cancel_on_a_running_operation_ends_it_cancelled(
        self, bulk_table, bulk_bucket
    ):
        rows = [row("n", str(i), f={"S": "x" * 40}) for i in range(20000)]
        write_rows(bulk_bucket, "in/rows.json", rows)
        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        arn = resp["BulkOperationDescription"]["BulkOperationArn"]

        cancel = extenddb_request("CancelBulkOperation", {"BulkOperationArn": arn})
        assert cancel["BulkOperationStatus"] == "CANCELLING"
        assert cancel["BulkOperationArn"] == arn
        assert set(cancel.keys()) == {"BulkOperationArn", "BulkOperationStatus"}

        terminal = wait_until_terminal(arn)
        assert terminal["BulkOperationStatus"] == "CANCELLED"
        # Measured on the hosted service.
        assert terminal["FailureCode"] == "RequestCancelled"
        assert terminal["FailureMessage"] == "The request was cancelled"
        assert terminal["ProcessedInputBytes"] == 0
        # The cancel lands within milliseconds of the start and the run
        # takes seconds, so a worker that stopped at the next boundary has
        # processed far fewer than all rows. A server that finished the
        # whole file and relabelled the outcome would fail here.
        assert terminal["ProcessedItemCount"] < 20000


# ---------------------------------------------------------------------------
# Second accounts, for listing and isolation
# ---------------------------------------------------------------------------
