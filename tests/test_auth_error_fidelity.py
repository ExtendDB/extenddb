# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Auth error fidelity tests — validate extenddb error responses match real DynamoDB.

These tests send intentionally bad credentials to the DynamoDB endpoint and
verify the error code, HTTP status, and message structure. They run against
both real DynamoDB (when EXTENDDB_TEST_ENDPOINT is unset) and extenddb (when set),
using the standard conftest.py dual-target pattern.

No management API access is needed — any bogus AKIA* key or wrong secret
produces the error responses we want to validate.

REQ-TEST-001, REQ-AUTH-001
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config as BotoConfig
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

from conftest import scoped_table
def _make_client(endpoint_url: str | None, access_key: str, secret_key: str) -> Any:
    """Create a boto3 DynamoDB client with explicit credentials."""
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    kwargs: dict = {
        "service_name": "dynamodb",
        "region_name": region,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "config": BotoConfig(retries={"max_attempts": 0}),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
    return boto3.client(**kwargs)
class TestAuthErrorFidelity:
    """Validate auth error responses match real DynamoDB behavior.

    These tests exercise credential failure paths that work identically
    against real DynamoDB and extenddb — no management API required.

    When targeting extenddb, auth must be enabled (auth.provider = "builtin").
    In Mode 1 (auth.provider = "none"), extenddb accepts all requests regardless
    of credentials, so these tests are skipped.
    """

    @pytest.fixture(autouse=True)
    def setup(self, endpoint_url):
        # When targeting extenddb (endpoint_url is set), skip unless auth is enabled.
        # EXTENDDB_ADMIN_USER being set signals that extenddb is running with builtin auth.
        # When targeting real DynamoDB (endpoint_url is None), always run.
        if endpoint_url and not os.environ.get("EXTENDDB_ADMIN_USER", "").strip():
            pytest.fail(
                "MISCONFIGURED: Auth error fidelity tests require auth-enabled extenddb "
                "(set EXTENDDB_ADMIN_USER to signal builtin auth mode). "
                "These must be set by devtools/run-tests before test execution."
            )
        self.endpoint_url = endpoint_url

    def test_invalid_access_key_returns_unrecognized_client(self):
        """Completely bogus access key returns UnrecognizedClientException.

        Real DynamoDB returns HTTP 400 with __type ending in
        UnrecognizedClientException. extenddb must match.
        """
        client = _make_client(
            self.endpoint_url,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "UnrecognizedClientException"

    def test_invalid_access_key_message_structure(self):
        """Error message for invalid access key mentions the key ID.

        Real DynamoDB: "The security token included in the request is invalid."
        """
        client = _make_client(
            self.endpoint_url,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        msg = exc_info.value.response["Error"].get("Message", "")
        # Real DynamoDB says "The security token included in the request is invalid."
        assert "security token" in msg.lower() or "invalid" in msg.lower()

    def test_wrong_secret_key_returns_unrecognized_client(self):
        """Valid-format access key with wrong secret returns expected error.

        Both real DynamoDB and extenddb should return HTTP 400 with
        UnrecognizedClientException (the key doesn't exist in either case,
        so the error is the same as an invalid key).
        """
        client = _make_client(
            self.endpoint_url,
            "AKIA0000000000000000",
            "0000000000000000000000000000000000000000",
        )
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert err["Error"]["Code"] == "UnrecognizedClientException"

    def test_empty_access_key_rejected(self):
        """Empty access key is rejected with an auth error.

        boto3 may raise a different error for empty credentials, but if
        the request reaches the server, it must be rejected.
        """
        # boto3 with empty string credentials still sends a SigV4 header.
        client = _make_client(self.endpoint_url, "X", "X")
        with pytest.raises(ClientError) as exc_info:
            client.list_tables()

        err = exc_info.value.response
        # Either UnrecognizedClientException or InvalidSignatureException.
        assert err["Error"]["Code"] in (
            "UnrecognizedClientException",
            "InvalidSignatureException",
        )
        assert err["ResponseMetadata"]["HTTPStatusCode"] == 400


class TestSignedPayloadIntegrity:
    """The signature must cover the body the server receives.

    A SigV4 signature includes the SHA-256 of the request body. A server that
    takes that hash from the client's ``x-amz-content-sha256`` header instead
    of hashing the body it received lets anyone between client and server
    rewrite a signed request into a different request of the same operation.
    These tests sign one body, transmit another, and require the rejection.
    """

    @pytest.fixture(autouse=True)
    def setup(self, endpoint_url):
        if endpoint_url and not os.environ.get("EXTENDDB_ADMIN_USER", "").strip():
            pytest.fail(
                "MISCONFIGURED: signed payload tests require auth-enabled extenddb "
                "(set EXTENDDB_ADMIN_USER to signal builtin auth mode)."
            )
        self.endpoint_url = endpoint_url

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def _post_signed_then_swapped(
        self, signed_body: dict, sent_body: dict, add_hash_header: bool
    ) -> requests.Response:
        """Sign ``signed_body``, then send ``sent_body`` under that signature.

        With ``add_hash_header`` the request also carries
        ``x-amz-content-sha256`` set to the hash of the signed body and signed
        as a header, the shape a forwarding proxy would produce. Credentials
        come from the environment, as in the other raw-request tests.
        """
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if not (access_key and secret_key):
            pytest.skip("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required to sign")
        token = os.environ.get("AWS_SESSION_TOKEN") or None
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        url = self.endpoint_url or f"https://dynamodb.{region}.amazonaws.com/"

        signed_bytes = json.dumps(signed_body).encode("utf-8")
        headers = {
            "X-Amz-Target": "DynamoDB_20120810.PutItem",
            "Content-Type": "application/x-amz-json-1.0",
        }
        if add_hash_header:
            headers["X-Amz-Content-SHA256"] = hashlib.sha256(signed_bytes).hexdigest()
        aws_req = AWSRequest(method="POST", url=url, data=signed_bytes, headers=headers)
        SigV4Auth(Credentials(access_key, secret_key, token), "dynamodb", region).add_auth(
            aws_req
        )

        return requests.post(
            url,
            data=json.dumps(sent_body).encode("utf-8"),
            headers=dict(aws_req.headers),
            # extenddb serves a self-signed certificate; the service does not.
            verify=self.endpoint_url is None,
        )

    @pytest.mark.parametrize(
        "add_hash_header",
        [False, True],
        ids=["no-hash-header", "hash-header-of-signed-body"],
    )
    def test_body_changed_after_signing_is_rejected(
        self, dynamodb_client, table, add_hash_header
    ):
        signed = {"TableName": table, "Item": {"pk": {"S": "signed"}}}
        sent = {
            "TableName": table,
            "Item": {"pk": {"S": "tampered"}, "injected": {"S": "yes"}},
        }

        resp = self._post_signed_then_swapped(signed, sent, add_hash_header)

        assert resp.status_code == 400, f"expected 400, got {resp.status_code}: {resp.text}"
        assert resp.json()["__type"].endswith("InvalidSignatureException"), resp.text
        for pk in ("signed", "tampered"):
            got = dynamodb_client.get_item(
                TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True
            )
            assert "Item" not in got, f"item {pk!r} was stored from an unsigned body"

    def test_unchanged_body_is_accepted(self, dynamodb_client, table):
        body = {"TableName": table, "Item": {"pk": {"S": "intact"}}}

        resp = self._post_signed_then_swapped(body, body, add_hash_header=True)

        assert resp.status_code == 200, f"expected 200, got {resp.status_code}: {resp.text}"
        got = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": "intact"}}, ConsistentRead=True
        )
        assert got["Item"]["pk"]["S"] == "intact"
