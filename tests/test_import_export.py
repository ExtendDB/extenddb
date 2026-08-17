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


# ---------------------------------------------------------------------------
# Per-account path helpers
#
# Import and export files are namespaced by account: a caller in account A may
# only read and write beneath <root>/A for each configured root. A path directly
# under the bare root is rejected, which is what stops tenants sharing an
# instance from reading or overwriting each other's files.
#
# devtools/run-tests points TMPDIR at the configured root, so
# tempfile.gettempdir() is that root.
# ---------------------------------------------------------------------------

ACCOUNT_ID = os.environ.get("EXTENDDB_TEST_ACCOUNT_ID", "123456789012")
FOREIGN_ACCOUNT = "999999999999"


def account_dir(account: str = ACCOUNT_ID) -> str:
    """Return (creating if needed) the per-account subtree of the root."""
    path = os.path.join(tempfile.gettempdir(), account)
    os.makedirs(path, exist_ok=True)
    return path


def export_target(suffix: str = ".json") -> str:
    """A path for an export that does not yet exist.

    Export refuses to overwrite, so the destination must not be pre-created.
    This is why export destinations cannot use NamedTemporaryFile.
    """
    return os.path.join(account_dir(), f"exp-{uuid.uuid4().hex}{suffix}")


def import_source(content: str, suffix: str = ".json", account: str = ACCOUNT_ID) -> str:
    """Write an import fixture inside `account`'s subtree and return its path."""
    path = os.path.join(account_dir(account), f"imp-{uuid.uuid4().hex}{suffix}")
    with open(path, "w") as f:
        f.write(content)
    return path


def discard(*paths: str) -> None:
    """Remove files that may or may not exist."""
    for path in paths:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


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

        export_path = export_target()

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
            discard(export_path)

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

        export_path = export_target()

        try:
            resp = extenddb_request("ExportTableToPointInTime", {
                "TableArn": table_arn,
                "FilePath": export_path,
            })
            assert resp["ExportDescription"]["ItemCount"] == 0
        finally:
            discard(export_path)
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
        source_path = import_source("".join(json.dumps(i) + "\n" for i in items))

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
            discard(source_path)

    def test_import_csv(self, dynamodb_client, unique_table_name, cleanup_table):
        """Import items from CSV file."""
        name = unique_table_name
        cleanup_table(name)

        csv_content = "pk,name,age\ncsv-1,Alice,30\ncsv-2,Bob,25\n"
        source_path = import_source(csv_content, suffix=".csv")

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
            discard(source_path)

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

        export_path = export_target()

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
            discard(export_path)


# ---------------------------------------------------------------------------
# Cross-tenant isolation
# ---------------------------------------------------------------------------
class TestCrossTenantIsolation:
    """Import/export files are confined to the calling account's subtree.

    A single instance may host several accounts, and the import/export roots are
    server-wide. Without per-account namespacing, containment in a shared root
    lets any tenant name another tenant's file: read it by importing it, or
    destroy it by exporting over it. These tests drive a foreign account's path
    directly rather than provisioning a second set of credentials, which is the
    same approach `backup_arn_scoping` takes for ARNs.
    """

    TCP = {
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
    }

    def _import(self, path: str, table: str) -> dict:
        return extenddb_request("ImportTable", {
            "FileSource": {"Path": path},
            "InputFormat": "DYNAMODB_JSON",
            "TableCreationParameters": {"TableName": table, **self.TCP},
        })

    def test_import_from_another_accounts_subtree_is_denied(
        self, unique_table_name, cleanup_table
    ):
        """The reported read primitive: importing a co-tenant's export file."""
        cleanup_table(unique_table_name)
        victim = import_source(
            json.dumps({"Item": {"pk": {"S": "secret"}}}) + "\n",
            account=FOREIGN_ACCOUNT,
        )
        try:
            with pytest.raises(RuntimeError, match="ValidationException"):
                self._import(victim, unique_table_name)
        finally:
            discard(victim)

    def test_export_into_another_accounts_subtree_is_denied(
        self, dynamodb_client, unique_table_name, cleanup_table
    ):
        """The reported destruction primitive: truncating a co-tenant's export.

        The victim file must still hold its original bytes afterwards. Before the
        fix it was truncated to zero length with no error to either party.
        """
        name = unique_table_name
        cleanup_table(name)
        dynamodb_client.create_table(TableName=name, **{
            "AttributeDefinitions": self.TCP["AttributeDefinitions"],
            "KeySchema": self.TCP["KeySchema"],
            "BillingMode": self.TCP["BillingMode"],
        })
        wait_for_active(dynamodb_client, name)
        table_arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]

        original = json.dumps({"Item": {"pk": {"S": "victim-data"}}}) + "\n"
        victim = import_source(original, account=FOREIGN_ACCOUNT)
        try:
            with pytest.raises(RuntimeError, match="ValidationException"):
                extenddb_request("ExportTableToPointInTime", {
                    "TableArn": table_arn,
                    "FilePath": victim,
                    "ExportFormat": "DYNAMODB_JSON",
                })
            with open(victim) as f:
                assert f.read() == original, "co-tenant's file was modified"
        finally:
            discard(victim)

    def test_import_from_the_bare_root_is_denied(self, unique_table_name, cleanup_table):
        """Containment in the shared root is not sufficient on its own."""
        cleanup_table(unique_table_name)
        stray = os.path.join(
            tempfile.gettempdir(), f"unscoped-{uuid.uuid4().hex}.json"
        )
        with open(stray, "w") as f:
            f.write(json.dumps({"Item": {"pk": {"S": "x"}}}) + "\n")
        try:
            with pytest.raises(RuntimeError, match="ValidationException"):
                self._import(stray, unique_table_name)
        finally:
            discard(stray)

    def test_export_refuses_to_overwrite_an_existing_file(
        self, dynamodb_client, unique_table_name, cleanup_table
    ):
        """Export will not truncate a file that is already there, even its own."""
        name = unique_table_name
        cleanup_table(name)
        dynamodb_client.create_table(TableName=name, **{
            "AttributeDefinitions": self.TCP["AttributeDefinitions"],
            "KeySchema": self.TCP["KeySchema"],
            "BillingMode": self.TCP["BillingMode"],
        })
        wait_for_active(dynamodb_client, name)
        table_arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]

        target = export_target()
        try:
            extenddb_request("ExportTableToPointInTime", {
                "TableArn": table_arn,
                "FilePath": target,
                "ExportFormat": "DYNAMODB_JSON",
            })
            assert os.path.exists(target)
            with pytest.raises(RuntimeError, match="already exists"):
                extenddb_request("ExportTableToPointInTime", {
                    "TableArn": table_arn,
                    "FilePath": target,
                    "ExportFormat": "DYNAMODB_JSON",
                })
        finally:
            discard(target)
