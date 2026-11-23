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


# A KMS key ARN that names no key. Start refuses the pairs below before any
# key is resolved.
UNUSED_KMS_KEY_ARN = (
    "arn:aws:kms:us-east-1:111111111111:key/00000000-0000-0000-0000-000000000000"
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

        terminal = wait_until_terminal(desc["BulkOperationArn"])
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["TotalInputBytes"] == len(frame)
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

    # The service's `Invalid Request:` checks run after the table lookup
    # (measured: a missing table answers ResourceNotFoundException before
    # any of them), so each test of one runs on a table that exists.
    def test_an_empty_output_type_list_is_refused(self, bulk_table, bulk_bucket):
        expect_error(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, output_types=[]),
            "ValidationException",
            "Invalid Request: OutputS3Configuration.OutputType cannot be empty",
        )

    # Measured on the service, twice each, with a key ARN of this shape: each
    # pair is refused at Start with its own message.
    @pytest.mark.parametrize("algorithm,key_id,message", [
        ("KMS", None,
         "Invalid Request: OutputS3Configuration.S3SseKmsKeyId cannot be empty "
         "for when OutputS3Configuration.S3SseAlgorithm is 'KMS'"),
        ("AES256", UNUSED_KMS_KEY_ARN,
         "Invalid Request: Cannot provide OutputS3Configuration.S3SseKmsKeyId "
         "when OutputS3Configuration.S3SseAlgorithm is 'AES256'"),
        (None, UNUSED_KMS_KEY_ARN,
         "Invalid Request: OutputS3Configuration.S3SseAlgorithm is required "
         "when OutputS3Configuration.S3SseKmsKeyId is provided"),
    ])
    def test_an_inconsistent_output_encryption_pair_is_refused(
        self, bulk_table, bulk_bucket, algorithm, key_id, message
    ):
        body = start_body(bulk_table, bulk_bucket)
        if algorithm is not None:
            body["OutputS3Configuration"]["S3SseAlgorithm"] = algorithm
        if key_id is not None:
            body["OutputS3Configuration"]["S3SseKmsKeyId"] = key_id
        expect_error("StartBulkPutItems", body, "ValidationException", message)

    def test_an_output_configuration_without_a_bucket_is_refused(
        self, bulk_table, bulk_bucket
    ):
        # Measured on the service: the output bucket member is
        # checked at Start, not at the first report record.
        body = start_body(bulk_table, bulk_bucket)
        body["OutputS3Configuration"].pop("S3Bucket", None)
        expect_error(
            "StartBulkPutItems", body,
            "ValidationException",
            "Invalid Request: OutputS3Configuration.S3Bucket is invalid",
        )

    def test_a_dotted_ip_bucket_name_is_refused(self, bulk_table, bulk_bucket):
        # Measured on the service: a bucket name spelled as a dotted
        # IPv4 literal is refused even though it satisfies the name
        # pattern and both length bounds.
        body = start_body(bulk_table, bulk_bucket)
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
    def test_a_prefix_with_a_parent_component_is_refused(
        self, bulk_table, bulk_bucket
    ):
        expect_error(
            "StartBulkPutItems",
            start_body(bulk_table, bulk_bucket, in_prefix="in/../out/"),
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
        # A cancel that lands before the input is listed leaves both byte
        # members absent; one that lands after carries ProcessedInputBytes 0
        # (measured on the service: 3.2 s or less after Start, and 3.9 s or
        # more).
        if "TotalInputBytes" in terminal:
            assert terminal["ProcessedInputBytes"] == 0
        else:
            assert "ProcessedInputBytes" not in terminal
        # The cancel lands within milliseconds of the start and the run
        # takes seconds, so a worker that stopped at the next boundary has
        # processed far fewer than all rows. A server that finished the
        # whole file and relabelled the outcome would fail here.
        assert terminal["ProcessedItemCount"] < 20000

    def test_the_account_limit_counts_each_performance_tier(self, mgmt):
        # Measured on the service (2026-11-19): five STANDARD operations and
        # one ACCELERATED operation run at once, and the sixth STANDARD Start
        # answers LimitExceededException with this text. MaxWriteRequestUnits
        # holds each run to about 20 s on this input, so all six are still
        # running when the seventh Start arrives. The limit counts every
        # operation of the account, so the test runs in an account of its
        # own: other bulk files start operations in the default account at
        # the same time when pytest runs them in parallel.
        account_id, creds = _provision_account(mgmt, "tiers")
        client = _boto_client(creds)
        bucket = f"bulk-tier-{uuid.uuid4().hex[:8]}"
        create_bucket(bucket, account_id)
        tables = [f"extenddb-bulk-tier-{uuid.uuid4().hex[:8]}" for _ in range(7)]
        arns = []
        try:
            write_rows(
                bucket, "in/rows.json", [row("n", str(i)) for i in range(20000)],
                account=account_id,
            )
            for name in tables:
                client.create_table(
                    TableName=name,
                    AttributeDefinitions=[
                        {"AttributeName": "pk", "AttributeType": "S"},
                        {"AttributeName": "sk", "AttributeType": "S"},
                    ],
                    KeySchema=[
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"},
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
            for name in tables:
                wait_for_active(client, name)
            tiers = ["STANDARD"] * 5 + ["ACCELERATED"]
            for name, tier in zip(tables, tiers):
                body = start_body(
                    name, bucket, PerformanceTier=tier, MaxWriteRequestUnits=1024
                )
                desc = extenddb_request("StartBulkPutItems", body, creds)[
                    "BulkOperationDescription"
                ]
                arns.append(desc["BulkOperationArn"])
            expect_error(
                "StartBulkPutItems",
                start_body(
                    tables[6], bucket, PerformanceTier="STANDARD",
                    MaxWriteRequestUnits=1024,
                ),
                "LimitExceededException",
                "Subscriber limit exceededService quota exceeded You have reached the "
                "maximum number of concurrent bulk operations for the selected "
                "PerformanceTier, which is 5. Wait until other jobs finish, and try "
                "again. You can also reach out to AWS Support to request a quota "
                "increase.",
                creds,
            )
        finally:
            for arn in arns:
                try:
                    extenddb_request(
                        "CancelBulkOperation", {"BulkOperationArn": arn}, creds
                    )
                except Exception:
                    pass
            for arn in arns:
                try:
                    wait_until_terminal(arn, timeout=60, creds=creds)
                except Exception:
                    pass
            for name in tables:
                try:
                    client.delete_table(TableName=name)
                    wait_for_deleted(client, name)
                except Exception:
                    pass
            discard_bucket(bucket, account_id)
            _release_account(mgmt, account_id, "tiers")


# ---------------------------------------------------------------------------
# Second accounts, for listing and isolation
# ---------------------------------------------------------------------------
