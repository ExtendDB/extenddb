# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Send a SigV4-signed DynamoDB request whose body no client library builds.

boto3 validates and reshapes the parameters before it signs, so it cannot send
a tag written as ``["k", "v"]``. ``signed_post`` signs the bytes it is given with
botocore's signer and posts them unchanged, with the credentials of the caller
rather than the ones in the environment.
"""

from __future__ import annotations

import http.client
import json
import ssl
from typing import Any
from urllib.parse import urlsplit

import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


def raw_request(
    url: str,
    method: str,
    target: str,
    headers: list[tuple[str, str]],
    body: bytes = b"",
    *,
    verify_tls: bool = True,
    chunked: bool = False,
) -> tuple[int, dict[str, str], bytes]:
    """Send ``method target`` to the host of ``url``, each header on its own line.

    The headers go out in the order given, a repeated name twice, and Host only
    when the caller passes one. With ``chunked`` the body goes out as one chunk
    under ``Transfer-Encoding: chunked``, with no Content-Length. Returns the
    status, the headers with lower-case names, and the body.
    """
    parts = urlsplit(url)
    if parts.scheme == "https":
        context = ssl.create_default_context()
        if not verify_tls:
            # D4: the self-signed certificate from `extenddb init` cannot be verified.
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        conn = http.client.HTTPSConnection(
            parts.hostname, parts.port or 443, context=context, timeout=60
        )
    else:
        conn = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=60)
    conn.putrequest(method, target, skip_host=True, skip_accept_encoding=True)
    for name, value in headers:
        conn.putheader(name, value)
    if chunked:
        conn.putheader("Transfer-Encoding", "chunked")
        body = b"%x\r\n%s\r\n0\r\n\r\n" % (len(body), body)
    elif body or method in ("POST", "PUT", "PATCH"):
        conn.putheader("Content-Length", str(len(body)))
    conn.endheaders()
    try:
        conn.send(body)
    except (BrokenPipeError, ConnectionResetError):
        pass  # the server answered before it read the whole body
    resp = conn.getresponse()
    answer = (resp.status, {k.lower(): v for k, v in resp.getheaders()}, resp.read())
    conn.close()
    return answer


def signed_headers(
    url: str,
    access_key: str,
    secret_key: str,
    region: str,
    headers: dict[str, str],
    payload: bytes,
    session_token: str | None = None,
) -> dict[str, str]:
    """``headers`` with the SigV4 headers for a POST of ``payload`` to ``url``.

    The signature covers the path and query string of ``url``; a caller may
    send the request elsewhere to check that.
    """
    request = AWSRequest(method="POST", url=url, data=payload, headers=headers)
    credentials = Credentials(access_key, secret_key, session_token)
    SigV4Auth(credentials, "dynamodb", region).add_auth(request)
    return dict(request.headers)


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
    signed = signed_headers(
        endpoint, access_key, secret_key, region, headers, payload, session_token
    )
    # D4: the self-signed certificate from `extenddb init` cannot be verified.
    return requests.post(
        endpoint,
        data=payload,
        headers=signed,
        verify=not endpoint.startswith("https://"),
        timeout=30,
    )
