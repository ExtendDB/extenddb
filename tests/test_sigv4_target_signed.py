# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""X-Amz-Target must be one of the signed headers.

The server picks the operation from X-Amz-Target. If the signature does not
cover that header, anyone who holds one signed request can resend its body
and signature under another operation, for example a GetItem as a DeleteItem.
These tests sign with botocore's signer, leave X-Amz-Target out of
SignedHeaders, and require the refusal. They run against the service and
against extenddb with builtin auth, using the conftest dual-target pattern.

The error type asserted here is the one extenddb returns for the other
SignedHeaders refusal (Host). The service's error type and message for this
case have not been measured, so the message is not asserted.

REQ-TEST-001, REQ-AUTH-001
"""

from __future__ import annotations

import json
import os

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from conftest import scoped_table

DYNAMODB_PREFIX = "DynamoDB_20120810"
STREAMS_PREFIX = "DynamoDBStreams_20120810"


class _SignerWithoutTarget(SigV4Auth):
    """botocore's SigV4 signer with X-Amz-Target left out of SignedHeaders."""

    def headers_to_sign(self, request):
        headers = super().headers_to_sign(request)
        del headers["x-amz-target"]
        return headers


def _credentials() -> Credentials:
    """Credentials to sign with: the environment first, then the SDK's chain."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    if access_key and secret_key:
        return Credentials(access_key, secret_key, os.environ.get("AWS_SESSION_TOKEN") or None)
    resolved = boto3.Session().get_credentials()
    if resolved is None:
        pytest.skip("no credentials available to sign the request")
    frozen = resolved.get_frozen_credentials()
    return Credentials(frozen.access_key, frozen.secret_key, frozen.token)


class TestTargetMustBeSigned:
    @pytest.fixture(autouse=True)
    def setup(self, endpoint_url):
        if endpoint_url and not os.environ.get("EXTENDDB_ADMIN_USER", "").strip():
            pytest.fail(
                "MISCONFIGURED: signed target tests require auth-enabled extenddb "
                "(set EXTENDDB_ADMIN_USER to signal builtin auth mode)."
            )
        self.endpoint_url = endpoint_url
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    @pytest.fixture(scope="class")
    def table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            yield name

    def _url(self, streams: bool = False) -> str:
        if self.endpoint_url:
            return self.endpoint_url
        if streams:
            return f"https://streams.dynamodb.{self.region}.amazonaws.com/"
        return f"https://dynamodb.{self.region}.amazonaws.com/"

    def _send(
        self,
        signed_target: str,
        body: dict,
        *,
        sign_target: bool,
        sent_target: str | None = None,
        streams: bool = False,
    ) -> requests.Response:
        """Sign a request for ``signed_target`` and send it as ``sent_target``.

        With ``sign_target`` False, X-Amz-Target is sent but not named in
        SignedHeaders. ``sent_target`` replaces the header after signing.
        """
        url = self._url(streams)
        payload = json.dumps(body).encode("utf-8")
        request = AWSRequest(
            method="POST",
            url=url,
            data=payload,
            headers={
                "X-Amz-Target": signed_target,
                "Content-Type": "application/x-amz-json-1.0",
            },
        )
        signer = SigV4Auth if sign_target else _SignerWithoutTarget
        signer(_credentials(), "dynamodb", self.region).add_auth(request)
        signed_headers = request.headers["Authorization"].split("SignedHeaders=")[1]
        signed_headers = signed_headers.split(",")[0].split(";")
        assert ("x-amz-target" in signed_headers) == sign_target, signed_headers
        headers = dict(request.headers)
        if sent_target is not None:
            headers["X-Amz-Target"] = sent_target
        return requests.post(
            url,
            data=payload,
            headers=headers,
            timeout=30,
            # extenddb serves a self-signed certificate; the service does not.
            verify=self.endpoint_url is None,
        )

    @staticmethod
    def _assert_refused(resp: requests.Response) -> None:
        assert resp.status_code == 400, f"expected 400, got {resp.status_code}: {resp.text}"
        assert resp.json()["__type"].endswith("InvalidSignatureException"), resp.text

    def test_get_item_resent_as_delete_item_is_refused(self, dynamodb_client, table):
        key = {"pk": {"S": "kept"}}
        dynamodb_client.put_item(TableName=table, Item=key)

        resp = self._send(
            f"{DYNAMODB_PREFIX}.GetItem",
            {"TableName": table, "Key": key},
            sign_target=False,
            sent_target=f"{DYNAMODB_PREFIX}.DeleteItem",
        )

        self._assert_refused(resp)
        got = dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)
        assert got.get("Item") == key, "the item was deleted under a GetItem signature"

    def test_unsigned_target_is_refused_when_unchanged(self, table):
        resp = self._send(
            f"{DYNAMODB_PREFIX}.GetItem",
            {"TableName": table, "Key": {"pk": {"S": "kept"}}},
            sign_target=False,
        )
        self._assert_refused(resp)

    def test_unsigned_streams_target_is_refused(self):
        resp = self._send(
            f"{STREAMS_PREFIX}.ListStreams",
            {},
            sign_target=False,
            streams=True,
        )
        self._assert_refused(resp)

    def test_signed_target_is_accepted(self, table):
        """The control: the same GetItem with X-Amz-Target signed succeeds."""
        resp = self._send(
            f"{DYNAMODB_PREFIX}.GetItem",
            {"TableName": table, "Key": {"pk": {"S": "kept"}}},
            sign_target=True,
        )
        assert resp.status_code == 200, f"expected 200, got {resp.status_code}: {resp.text}"

    def test_signed_streams_target_is_accepted(self):
        """The control for the Streams prefix."""
        resp = self._send(f"{STREAMS_PREFIX}.ListStreams", {}, sign_target=True, streams=True)
        assert resp.status_code == 200, f"expected 200, got {resp.status_code}: {resp.text}"
