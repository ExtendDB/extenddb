# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Send a SigV4-signed DynamoDB request whose body no client library builds.

boto3 validates and reshapes the parameters before it signs, so it cannot send
a tag written as ``["k", "v"]``. ``signed_post`` signs the bytes it is given with
botocore's signer and posts them unchanged, with the credentials of the caller
rather than the ones in the environment. ``client_post`` signs and sends them
with a boto3 client's own signer and connection.
"""

from __future__ import annotations

import json
from typing import Any

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


def client_post(client, operation: str, body: Any) -> tuple[int, dict]:
    """POST ``body`` to ``operation`` as written, signed with ``client``'s
    credentials, and return the status and the decoded answer."""
    req = AWSRequest(method="POST", url=client.meta.endpoint_url, data=json.dumps(body),
                     headers={"Content-Type": "application/x-amz-json-1.0",
                              "X-Amz-Target": f"DynamoDB_20120810.{operation}"})
    client._request_signer.sign(operation, req)
    resp = client._endpoint.http_session.send(req.prepare())
    return resp.status_code, json.loads(resp.content.decode("utf-8", "replace") or "{}")


def signed_post(
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    operation: str,
    body: Any,
    session_token: str | None = None,
) -> requests.Response:
    """POST ``body`` as JSON to ``operation`` under the DynamoDB JSON-1.0 protocol."""
    payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    request = AWSRequest(method="POST", url=endpoint, data=payload, headers=headers)
    credentials = Credentials(access_key, secret_key, session_token)
    SigV4Auth(credentials, "dynamodb", region).add_auth(request)
    # D4: the self-signed certificate from `extenddb init` cannot be verified.
    return requests.post(
        endpoint,
        data=payload,
        headers=dict(request.headers),
        verify=not endpoint.startswith("https://"),
        timeout=30,
    )
