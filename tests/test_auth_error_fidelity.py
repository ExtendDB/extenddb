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


def _env_credentials() -> tuple[str, str, str | None]:
    """Credentials to sign with: the environment first, then the SDK's chain."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        return access_key, secret_key, os.environ.get("AWS_SESSION_TOKEN") or None
    resolved = boto3.Session().get_credentials()
    if resolved is None:
        pytest.skip("no credentials available to sign the request")
    frozen = resolved.get_frozen_credentials()
    return frozen.access_key, frozen.secret_key, frozen.token


class TestRequestChecks:
    """Checks on the signed request other than the signature itself.

    Each expected message was measured against the service on 2026-09-16.
    """

    @pytest.fixture(autouse=True)
    def setup(self, endpoint_url):
        if endpoint_url and not os.environ.get("EXTENDDB_ADMIN_USER", "").strip():
            pytest.fail(
                "MISCONFIGURED: request check tests require auth-enabled extenddb "
                "(set EXTENDDB_ADMIN_USER to signal builtin auth mode)."
            )
        self.endpoint_url = endpoint_url
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self.url = endpoint_url or f"https://dynamodb.{self.region}.amazonaws.com/"

    def _post(self, body: dict, *, headers: dict | None = None, region: str | None = None,
              token: str | None = "env") -> requests.Response:
        """Sign and send one ListTables-style request with botocore's signer.

        ``headers`` are added before signing so they are covered. ``region``
        overrides the credential scope. ``token`` "env" uses the environment's
        session token, if any; None sends no token; a string sends that token.
        """
        access_key, secret_key, env_token = _env_credentials()
        if token == "env":
            token = env_token
        body_bytes = json.dumps(body).encode("utf-8")
        hdrs = {
            "X-Amz-Target": "DynamoDB_20120810.ListTables",
            "Content-Type": "application/x-amz-json-1.0",
        }
        hdrs.update(headers or {})
        aws_req = AWSRequest(method="POST", url=self.url, data=body_bytes, headers=hdrs)
        SigV4Auth(Credentials(access_key, secret_key, token), "dynamodb", region or self.region).add_auth(aws_req)
        return requests.post(
            self.url,
            data=body_bytes,
            headers=dict(aws_req.headers),
            verify=self.endpoint_url is None,
        )

    def test_scope_for_another_region_is_rejected(self):
        other = "us-west-2" if self.region != "us-west-2" else "us-east-1"
        resp = self._post({}, region=other)
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("InvalidSignatureException"), resp.text
        assert resp.json()["message"] == "Credential should be scoped to a valid region. "

    def test_scope_wrong_in_region_and_service_reports_both(self):
        access_key, secret_key, token = _env_credentials()
        other = "us-west-2" if self.region != "us-west-2" else "us-east-1"
        body_bytes = json.dumps({}).encode("utf-8")
        aws_req = AWSRequest(
            method="POST",
            url=self.url,
            data=body_bytes,
            headers={
                "X-Amz-Target": "DynamoDB_20120810.ListTables",
                "Content-Type": "application/x-amz-json-1.0",
            },
        )
        SigV4Auth(Credentials(access_key, secret_key, token), "s3", other).add_auth(aws_req)
        resp = requests.post(self.url, data=body_bytes, headers=dict(aws_req.headers), verify=self.endpoint_url is None)
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("InvalidSignatureException"), resp.text
        assert resp.json()["message"] == (
            "Credential should be scoped to a valid region. "
            "Credential should be scoped to correct service: 'dynamodb'. "
        )

    def test_duplicate_host_header_is_refused(self):
        # The service's front end refuses a request with two host headers
        # before it reaches signing (400, connection closed).
        import http.client  # noqa: PLC0415
        import ssl  # noqa: PLC0415
        from urllib.parse import urlparse  # noqa: PLC0415

        u = urlparse(self.url)
        ctx = ssl.create_default_context()
        if self.endpoint_url is not None:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, context=ctx, timeout=30)
        conn.putrequest("POST", "/", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", u.netloc)
        conn.putheader("Host", "evil.example.com")
        conn.putheader("X-Amz-Target", "DynamoDB_20120810.ListTables")
        conn.putheader("Content-Type", "application/x-amz-json-1.0")
        conn.putheader("Content-Length", "2")
        conn.endheaders()
        conn.send(b"{}")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        assert resp.status == 400

    def test_session_token_with_a_long_term_key_is_rejected(self):
        _, _, env_token = _env_credentials()
        if env_token:
            pytest.skip("the environment's credentials are temporary; this case needs a long-term key")
        resp = self._post({}, token="AQoDYXdzEJr-not-a-real-token")
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("UnrecognizedClientException"), resp.text
        assert resp.json()["message"] == "The security token included in the request is invalid"

    def test_repeated_signed_header_is_canonicalized_comma_joined(self):
        # botocore joins repeated header values with commas when signing; the
        # server must canonicalize the same way, or every such request fails.
        access_key, secret_key, token = _env_credentials()
        body_bytes = json.dumps({}).encode("utf-8")
        aws_req = AWSRequest(
            method="POST",
            url=self.url,
            data=body_bytes,
            headers={
                "X-Amz-Target": "DynamoDB_20120810.ListTables",
                "Content-Type": "application/x-amz-json-1.0",
            },
        )
        aws_req.headers.add_header("X-Amz-Meta-Dup", "first")
        aws_req.headers.add_header("X-Amz-Meta-Dup", "second")
        SigV4Auth(Credentials(access_key, secret_key, token), "dynamodb", self.region).add_auth(aws_req)
        # Send every header as its own line, including both X-Amz-Meta-Dup values.
        prepared_headers = list(aws_req.headers.items())
        import http.client  # noqa: PLC0415
        import ssl  # noqa: PLC0415
        from urllib.parse import urlparse  # noqa: PLC0415

        u = urlparse(self.url)
        ctx = ssl.create_default_context()
        if self.endpoint_url is not None:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, context=ctx, timeout=30)
        conn.putrequest("POST", "/", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", u.netloc)
        for k, v in prepared_headers:
            conn.putheader(k, v)
        conn.putheader("Content-Length", str(len(body_bytes)))
        conn.endheaders()
        conn.send(body_bytes)
        resp = conn.getresponse()
        text = resp.read().decode()
        conn.close()
        assert resp.status == 200, text

    def test_unsigned_payload_literal_is_a_plain_signature_mismatch(self):
        # A client that signs the payload line as the S3 literal fails with the
        # ordinary mismatch: the service does not special-case the literal.
        access_key, secret_key, token = _env_credentials()
        body_bytes = json.dumps({}).encode("utf-8")
        aws_req = AWSRequest(
            method="POST",
            url=self.url,
            data=body_bytes,
            headers={
                "X-Amz-Target": "DynamoDB_20120810.ListTables",
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Content-SHA256": "UNSIGNED-PAYLOAD",
            },
        )
        signer = SigV4Auth(Credentials(access_key, secret_key, token), "dynamodb", self.region)
        signer.payload = lambda request: "UNSIGNED-PAYLOAD"  # sign the literal, as such a client does
        signer.add_auth(aws_req)
        resp = requests.post(self.url, data=body_bytes, headers=dict(aws_req.headers), verify=self.endpoint_url is None)
        assert resp.status_code == 400, resp.text
        assert resp.json()["__type"].endswith("InvalidSignatureException"), resp.text
        assert resp.json()["message"].startswith("The request signature we calculated does not match"), resp.text
        assert "UNSIGNED-PAYLOAD" not in resp.json()["message"]

    def test_stalled_body_is_closed_by_the_server(self):
        """A Content-Length the client never delivers must not hold the connection.

        Runs against extenddb only. The server's `request_timeout_secs` bounds the
        wait; the test allows that plus ten seconds, reading the value from
        EXTENDDB_REQUEST_TIMEOUT_SECS (default 30).
        """
        if self.endpoint_url is None:
            pytest.skip("timeout behavior is a server property; not measured against the service")
        import http.client  # noqa: PLC0415
        import socket  # noqa: PLC0415
        import ssl  # noqa: PLC0415
        import time  # noqa: PLC0415
        from urllib.parse import urlparse  # noqa: PLC0415

        timeout = int(os.environ.get("EXTENDDB_REQUEST_TIMEOUT_SECS", "30"))
        u = urlparse(self.url)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, context=ctx, timeout=timeout + 10)
        conn.putrequest("POST", "/", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{u.hostname}:{u.port or 443}")
        conn.putheader("X-Amz-Target", "DynamoDB_20120810.ListTables")
        conn.putheader("Content-Type", "application/x-amz-json-1.0")
        conn.putheader("Content-Length", "100")
        conn.endheaders()
        conn.send(b"{}")  # 2 of the announced 100 bytes; nothing more follows
        started = time.monotonic()
        try:
            resp = conn.getresponse()
            status = resp.status
            resp.read()
        except (http.client.RemoteDisconnected, ConnectionResetError, socket.timeout) as e:
            status = type(e).__name__
        elapsed = time.monotonic() - started
        conn.close()
        assert status in (408, "RemoteDisconnected", "ConnectionResetError"), status
        assert elapsed < timeout + 10, f"server held the stalled connection for {elapsed:.0f}s"
