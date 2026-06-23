# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for ImportTable and ExportTableToPointInTime operations.

These tests are extenddb-specific (FileSource instead of S3BucketSource) and
only run against extenddb, not real DynamoDB. They use raw HTTP requests because
boto3 validates parameters against the DynamoDB API model, which does not
include extenddb-specific fields like FileSource and FilePath.

Requests are signed with SigV4 using credentials from environment variables.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from conftest import wait_for_active, wait_for_deleted
# EXTENDDB_TEST_ENDPOINT is required — devtools/run-tests validates this.
# Tests will use the default endpoint if the env var is missing.

ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "http://localhost:18443").strip()
def extenddb_request(operation: str, body: dict) -> dict:
    """Send a raw DynamoDB-format request to extenddb with SigV4 authentication."""
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }

    # Sign the request with SigV4 using env var credentials.
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if access_key and secret_key:
        creds = Credentials(access_key, secret_key)
        aws_req = AWSRequest(method="POST", url=ENDPOINT, data=body_bytes, headers=headers)
        SigV4Auth(creds, "dynamodb", region).add_auth(aws_req)
        headers = dict(aws_req.headers)

    resp = requests.post(
        ENDPOINT,
        data=body_bytes,
        headers=headers,
        verify=not ENDPOINT.startswith("https://"),
    )
    result = resp.json()
    if resp.status_code >= 400:
        error_type = result.get("__type", "Unknown")
        error_msg = result.get("message", result.get("Message", ""))
        raise RuntimeError(f"{error_type}: {error_msg} (HTTP {resp.status_code})")
    return result
@pytest.fixture()
def unique_table_name():
    """Generate a unique table name."""
    return f"extenddb-ie-test-{uuid.uuid4().hex[:8]}"
@pytest.fixture()
def cleanup_table(dynamodb_client):
    """Ensure table is deleted after test."""
    tables: list[str] = []

    def _register(name: str) -> None:
        tables.append(name)

    yield _register

    for name in tables:
        try:
            dynamodb_client.delete_table(TableName=name)
            wait_for_deleted(dynamodb_client, name)
        except Exception:
            pass
# ---------------------------------------------------------------------------
# ExportTableToPointInTime
# ---------------------------------------------------------------------------
class TestExportTable:
    """Tests for ExportTableToPointInTime."""

    @pytest.fixture()
    def populated_table(self, dynamodb_client, unique_table_name, cleanup_table):
        """Create and populate a table for export tests."""
        name = unique_table_name
        cleanup_table(name)
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)

        for i in range(5):
            dynamodb_client.put_item(
                TableName=name,
                Item={"pk": {"S": f"item-{i}"}, "data": {"S": f"value-{i}"}},
            )
        return name

    def test_export_dynamodb_json(self, dynamodb_client, populated_table):
        """Export table to DYNAMODB_JSON format."""
        table_name = populated_table
        desc = dynamodb_client.describe_table(TableName=table_name)
        table_arn = desc["Table"]["TableArn"]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            export_path = f.name

        try:
            resp = extenddb_request("ExportTableToPointInTime", {
                "TableArn": table_arn,
                "FilePath": export_path,
                "ExportFormat": "DYNAMODB_JSON",
            })
            export_desc = resp["ExportDescription"]
            assert export_desc["ExportStatus"] == "COMPLETED"
            assert export_desc["ItemCount"] == 5
            assert export_desc["ExportFormat"] == "DYNAMODB_JSON"

            # Verify file contents.
            with open(export_path) as f:
                lines = [line.strip() for line in f if line.strip()]
            assert len(lines) == 5

            for line in lines:
                obj = json.loads(line)
                assert "Item" in obj
                assert "pk" in obj["Item"]
        finally:
            os.unlink(export_path)

    def test_export_empty_table(self, dynamodb_client, unique_table_name, cleanup_table):
        """Export an empty table produces empty file."""
        name = unique_table_name
        cleanup_table(name)
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)

        desc = dynamodb_client.describe_table(TableName=name)
        table_arn = desc["Table"]["TableArn"]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            export_path = f.name

        try:
            resp = extenddb_request("ExportTableToPointInTime", {
                "TableArn": table_arn,
                "FilePath": export_path,
            })
            assert resp["ExportDescription"]["ItemCount"] == 0
        finally:
            os.unlink(export_path)
# ---------------------------------------------------------------------------
# ImportTable
# ---------------------------------------------------------------------------
class TestImportTable:
    """Tests for ImportTable."""

    def test_import_dynamodb_json(self, dynamodb_client, unique_table_name, cleanup_table):
        """Import items from DYNAMODB_JSON file."""
        name = unique_table_name
        cleanup_table(name)

        items = [
            {"Item": {"pk": {"S": f"imp-{i}"}, "val": {"N": str(i)}}}
            for i in range(3)
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            for item in items:
                f.write(json.dumps(item) + "\n")
            source_path = f.name

        try:
            resp = extenddb_request("ImportTable", {
                "FileSource": {"Path": source_path},
                "InputFormat": "DYNAMODB_JSON",
                "TableCreationParameters": {
                    "TableName": name,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            })
            desc = resp["ImportTableDescription"]
            assert desc["ImportStatus"] == "COMPLETED"
            assert desc["ImportedItemCount"] == 3
            assert desc["ErrorCount"] == 0

            for i in range(3):
                item = dynamodb_client.get_item(
                    TableName=name, Key={"pk": {"S": f"imp-{i}"}}
                )
                assert item["Item"]["val"]["N"] == str(i)
        finally:
            os.unlink(source_path)

    def test_import_csv(self, dynamodb_client, unique_table_name, cleanup_table):
        """Import items from CSV file."""
        name = unique_table_name
        cleanup_table(name)

        csv_content = "pk,name,age\ncsv-1,Alice,30\ncsv-2,Bob,25\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_content)
            source_path = f.name

        try:
            resp = extenddb_request("ImportTable", {
                "FileSource": {"Path": source_path},
                "InputFormat": "CSV",
                "TableCreationParameters": {
                    "TableName": name,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            })
            desc = resp["ImportTableDescription"]
            assert desc["ImportStatus"] == "COMPLETED"
            assert desc["ImportedItemCount"] == 2

            item = dynamodb_client.get_item(
                TableName=name, Key={"pk": {"S": "csv-1"}}
            )
            assert item["Item"]["name"]["S"] == "Alice"
            assert item["Item"]["age"]["S"] == "30"
        finally:
            os.unlink(source_path)

    def test_import_nonexistent_source(self, dynamodb_client, unique_table_name, cleanup_table):
        """Import from nonexistent path returns error."""
        name = unique_table_name
        cleanup_table(name)
        with pytest.raises(RuntimeError, match="ValidationException"):
            extenddb_request("ImportTable", {
                "FileSource": {"Path": "/nonexistent/path/data.json"},
                "InputFormat": "DYNAMODB_JSON",
                "TableCreationParameters": {
                    "TableName": name,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ],
                },
            })

    def test_export_then_import_roundtrip(
        self, dynamodb_client, unique_table_name, cleanup_table
    ):
        """Export a table, then import into a new table — data roundtrips."""
        src_name = unique_table_name
        cleanup_table(src_name)
        dynamodb_client.create_table(
            TableName=src_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, src_name)

        original_items = {}
        for i in range(10):
            pk = f"rt-{i}"
            dynamodb_client.put_item(
                TableName=src_name,
                Item={"pk": {"S": pk}, "n": {"N": str(i * 10)}, "s": {"S": f"data-{i}"}},
            )
            original_items[pk] = {"n": str(i * 10), "s": f"data-{i}"}

        desc = dynamodb_client.describe_table(TableName=src_name)
        table_arn = desc["Table"]["TableArn"]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            export_path = f.name

        try:
            extenddb_request("ExportTableToPointInTime", {
                "TableArn": table_arn,
                "FilePath": export_path,
                "ExportFormat": "DYNAMODB_JSON",
            })

            dst_name = f"extenddb-ie-dst-{uuid.uuid4().hex[:8]}"
            cleanup_table(dst_name)

            extenddb_request("ImportTable", {
                "FileSource": {"Path": export_path},
                "InputFormat": "DYNAMODB_JSON",
                "TableCreationParameters": {
                    "TableName": dst_name,
                    "AttributeDefinitions": [
                        {"AttributeName": "pk", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            })

            for pk, expected in original_items.items():
                item = dynamodb_client.get_item(
                    TableName=dst_name, Key={"pk": {"S": pk}}
                )
                assert item["Item"]["n"]["N"] == expected["n"]
                assert item["Item"]["s"]["S"] == expected["s"]
        finally:
            os.unlink(export_path)
