# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Send a SigV4-signed DynamoDB request whose body no client library builds.

boto3 validates and reshapes the parameters before it signs, so it cannot send
a tag written as ``["k", "v"]``. ``signed_post`` signs the bytes it is given with
botocore's signer and posts them unchanged, with the credentials of the caller
rather than the ones in the environment. ``answer`` reads the status, the error
type and the message of the response.
"""

from __future__ import annotations

import json
from typing import Any

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


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


def answer(resp: requests.Response) -> tuple[int, str, str]:
    """The HTTP status, the error type after the ``#`` and the message of a
    JSON answer, or the status, an empty type and the text of an answer that
    is not JSON."""
    try:
        doc = resp.json()
    except ValueError:
        return resp.status_code, "", resp.text
    return (
        resp.status_code,
        doc.get("__type", "").rsplit("#", 1)[-1],
        doc.get("message", doc.get("Message", "")),
    )
