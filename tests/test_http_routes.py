# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""How the service answers a request by its media type, its method and its path.

Measured against dynamodb.us-east-1 on 2026-11-14 and 2026-11-17. Each request
is a ListTables with the body {"Limit": 1}, changed in one part and signed after
the change, or a GET; each went out twice and got the same answer both times.

A POST at any path is served when its Content-Type, without its parameters and
in any letter case, is application/x-amz-json-1.0 or application/json, and the
answer is in that media type. A POST in any other media type, or with none,
gets a 404 with an HTML page and no Content-Type, ahead of every other check
but the 16 MiB body limit: over it, a POST gets a 413 with no body in every
media type. The signature covers the path and the query string. A GET of / with
no query string, or of /ping, gets a health text. Every other request gets the
404 page, and HEAD gets it without the body. Every answer but the 413 carries
an x-amzn-RequestId and the X-Amz-Crc32 of its body (for HEAD, of the page).

The tests run against the service when EXTENDDB_TEST_ENDPOINT is unset and
against extenddb when it is set (conftest.py). The checks of extenddb's own
routes, and of the layers in front of every route, run against extenddb only.
"""

from __future__ import annotations

import json
import os
import zlib
from dataclasses import dataclass
from urllib.parse import urlsplit

import boto3
import pytest

from raw_signed_request import raw_request, signed_headers

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
LIST_TABLES = "DynamoDB_20120810.ListTables"
NO_SUCH_OPERATION = "DynamoDB_20120810.NoSuchOperation"
AMZ_JSON = "application/x-amz-json-1.0"
JSON = "application/json"
BODY = b'{"Limit": 1}'
PAGE = (
    b'<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
    b'"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">\n'
    b'<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">\n'
    b"<head>\n  <title>Page Not Found</title>\n</head>\n<body>Page Not Found</body>\n</html>"
)
HEALTH = f"healthy: dynamodb.{REGION}.amazonaws.com ".encode()
MISMATCH = (
    "The request signature we calculated does not match the signature you provided. "
    "Check your AWS Secret Access Key and signing method. "
    "Consult the service documentation for details."
)


@dataclass
class Answer:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def content_type(self) -> str | None:
        return self.headers.get("content-type")


class Target:
    """The endpoint under test and the credentials its requests are signed with."""

    def __init__(self, url: str, local: bool):
        self.url = url.rstrip("/")
        self.local = local
        parts = urlsplit(self.url)
        self.netloc = parts.netloc
        self.https = parts.scheme == "https"
        resolved = boto3.Session().get_credentials()
        if resolved is None:
            pytest.skip("no credentials to sign the requests with")
        self.creds = resolved.get_frozen_credentials()

    def send(self, method, target, headers=(), body=b"", *, absolute=False, chunked=False) -> Answer:
        """Send one request, with Host first and each header on its own line."""
        request_target = self.url + target if absolute else target
        status, resp_headers, data = raw_request(
            self.url,
            method,
            request_target,
            [("Host", self.netloc), *headers],
            body,
            verify_tls=not self.local,
            chunked=chunked,
        )
        return Answer(status, resp_headers, data)

    def post(
        self,
        target="/",
        *,
        content_type=AMZ_JSON,
        operation=LIST_TABLES,
        body=BODY,
        sign=True,
        signed_for=None,
        absolute=False,
        extra=(),
        chunked=False,
    ) -> Answer:
        """POST a ListTables request, signed for `signed_for` (or `target`)."""
        headers = {}
        if operation is not None:
            headers["X-Amz-Target"] = operation
        if content_type is not None:
            headers["Content-Type"] = content_type
        if sign:
            c = self.creds
            url = self.url + (signed_for or target)
            headers = signed_headers(url, c.access_key, c.secret_key, REGION, headers, body, c.token)
        return self.send("POST", target, [*headers.items(), *extra], body, absolute=absolute, chunked=chunked)


@pytest.fixture(scope="module")
def target(endpoint_url) -> Target:
    return Target(endpoint_url or f"https://dynamodb.{REGION}.amazonaws.com", endpoint_url is not None)


@pytest.fixture()
def local(target) -> Target:
    if not target.local:
        pytest.skip("extenddb's own routes and the layers in front of them")
    return target


def assert_wire(answer: Answer, body: bytes | None = None) -> None:
    """The request id and the CRC32 of `body`, by default the answer's own."""
    assert answer.headers.get("x-amzn-requestid"), answer
    expected = answer.body if body is None else body
    assert answer.headers.get("x-amz-crc32") == str(zlib.crc32(expected)), answer


def assert_page(answer: Answer) -> None:
    assert (answer.status, answer.content_type, answer.body) == (404, None, PAGE), answer
    assert_wire(answer)


def assert_served(answer: Answer, media_type: str) -> None:
    assert (answer.status, answer.content_type) == (200, media_type), answer
    assert "TableNames" in json.loads(answer.body), answer
    assert_wire(answer)


# A signed POST at / in each Content-Type of the measured rows: the media type
# of the answer, or None for the 404 page.
MEDIA_TYPES = [
    ("application/x-amz-json-1.0", AMZ_JSON),
    ("application/x-amz-json-1.0; charset=utf-8", AMZ_JSON),
    ("APPLICATION/X-AMZ-JSON-1.0", AMZ_JSON),
    ("application/x-amz-json-1.0 ; charset=utf-8", AMZ_JSON),
    ("application/json", JSON),
    ("application/json; charset=utf-8", JSON),
    ("Application/JSON", JSON),
    ("application/x-amz-json-1.1", None),
    ("text/plain", None),
    ("application/xml", None),
    ("application/octet-stream", None),
    ("text/html; charset=utf-8", None),
    ("*/*", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize("content_type,answered_in", MEDIA_TYPES)
def test_a_post_is_served_in_two_media_types(target, content_type, answered_in):
    answer = target.post(content_type=content_type)
    if answered_in is None:
        assert_page(answer)
    else:
        assert_served(answer, answered_in)


# A signed POST sent to a target, signed for it or for another one, in the
# origin form or the absolute form: served, or InvalidSignatureException.
TARGETS = [
    ("/ping", "/ping", False, True),
    ("/ping", "/ping", True, True),
    ("/other/path", "/other/path", False, True),
    ("/?a=b", "/?a=b", False, True),
    ("/?a=b", "/", False, False),
    ("/ping", "/", False, False),
    ("/", "/ping", False, False),
]


@pytest.mark.parametrize("sent_to,signed_for,absolute,served", TARGETS)
def test_the_signature_covers_the_path_and_the_query_string(
    target, sent_to, signed_for, absolute, served
):
    answer = target.post(sent_to, signed_for=signed_for, absolute=absolute)
    if served:
        assert_served(answer, AMZ_JSON)
        return
    assert (answer.status, answer.content_type) == (400, AMZ_JSON), answer
    assert json.loads(answer.body) == {
        "__type": "com.amazon.coral.service#InvalidSignatureException",
        "message": MISMATCH,
    }
    assert_wire(answer)


# GETs and other methods, unsigned: the health text, the 404 page, or for HEAD
# the 404 without the body.
OTHER_REQUESTS = [
    ("GET", "/", (), False, "health"),
    ("GET", "/", (), True, "health"),
    ("GET", "/ping", (), False, "health"),
    ("GET", "/ping", (), True, "health"),
    ("GET", "/ping?x=1", (), False, "health"),
    ("GET", "/", (("Authorization", "not-a-signature"),), False, "health"),
    ("GET", "/?probe=1", (), False, "page"),
    ("PUT", "/", (), False, "page"),
    ("DELETE", "/", (), False, "page"),
    ("PATCH", "/", (), False, "page"),
    ("OPTIONS", "/", (), False, "page"),
    ("GET", "/other/path", (), False, "page"),
    ("HEAD", "/", (), False, "head"),
]


@pytest.mark.parametrize("method,path,headers,absolute,expected", OTHER_REQUESTS)
def test_other_requests_get_the_health_text_or_the_page(
    target, method, path, headers, absolute, expected
):
    answer = target.send(method, path, headers, absolute=absolute)
    if expected == "health":
        assert (answer.status, answer.content_type, answer.body) == (200, "text/plain", HEALTH)
        assert_wire(answer)
    elif expected == "page":
        assert_page(answer)
    else:
        assert (answer.status, answer.content_type, answer.body) == (404, None, b""), answer
        assert_wire(answer, PAGE)


# The 404 page comes ahead of authentication, the operation check, the JSON
# parse and the X-Amz-Target check; an error to a request in application/json
# is in application/json (2026-11-17).
CHECK_ORDER = [
    ("text/plain", False, LIST_TABLES, BODY, None),
    ("text/plain", True, NO_SUCH_OPERATION, BODY, None),
    ("text/plain", True, LIST_TABLES, b"not json", None),
    ("text/plain", True, None, BODY, None),
    (JSON, False, LIST_TABLES, BODY, "MissingAuthenticationToken"),
    (JSON, True, LIST_TABLES, b"not json", "SerializationException"),
    (JSON, True, NO_SUCH_OPERATION, BODY, "UnknownOperationException"),
]


@pytest.mark.parametrize("content_type,sign,operation,body,error", CHECK_ORDER)
def test_the_page_comes_ahead_of_every_other_check(
    target, content_type, sign, operation, body, error
):
    answer = target.post(content_type=content_type, operation=operation, body=body, sign=sign)
    if error is None:
        assert_page(answer)
        return
    assert (answer.status, answer.content_type) == (400, JSON), answer
    error_type = json.loads(answer.body)["__type"]
    assert error_type.startswith("com.amazon.coral.service#" + error), answer
    assert_wire(answer)


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/management"),
        ("POST", "/management/"),
        ("POST", "/management/no-such-route"),
        ("POST", "/console/no-such-page"),
        ("GET", "/console/no-such-page"),
        ("HEAD", "/management/no-such-route"),
    ],
)
def test_a_path_under_an_extenddb_router_keeps_its_empty_404(local, method, path):
    # A POST is a signed ListTables, which a path outside them serves.
    answer = local.post(path) if method == "POST" else local.send(method, path)
    assert (answer.status, answer.content_type, answer.body) == (404, None, b""), answer
    assert "x-amz-crc32" not in answer.headers, answer


def test_extenddb_routes_keep_their_answers(local):
    health = local.send("GET", "/health")
    assert health.status == 200, health
    assert json.loads(health.body)["status"] in ("healthy", "degraded"), health
    assert local.send("GET", "/metrics").status == 401
    assert local.send("POST", "/health").status == 405


# One answer of each kind: the health text, the 404 page, and the API at a
# path other than /.
LAYERED = [("GET", "/ping", HEALTH), ("GET", "/other/path", PAGE), ("POST", "/ping", None)]


@pytest.mark.parametrize("method,path,body", LAYERED)
def test_the_answers_carry_the_security_headers(local, method, path, body):
    answer = local.post(path) if method == "POST" else local.send(method, path)
    if body is None:
        assert_served(answer, AMZ_JSON)
    else:
        assert answer.body == body, answer
    assert answer.headers.get("x-content-type-options") == "nosniff"
    assert answer.headers.get("x-frame-options") == "DENY"
    assert answer.headers.get("referrer-policy") == "strict-origin-when-cross-origin"
    hsts = "max-age=63072000; includeSubDomains" if local.https else None
    assert answer.headers.get("strict-transport-security") == hsts


@pytest.mark.parametrize("path", ["/", "/ping"])
def test_a_post_at_any_path_has_the_16_mib_body_limit(target, path):
    # Past the 1024 bytes of extenddb's own routes and past the framework's
    # 2 MB default, then past 16 MiB, where the service answers 413 with no
    # body in every media type, text/plain included, with a Content-Length or
    # chunked (2026-11-17).
    for size in (4096, 3 * 1024 * 1024):
        assert_served(target.post(path, body=b'{"Limit": 1' + b" " * size + b"}"), AMZ_JSON)
    big = b'{"Limit": 1' + b" " * (16 * 1024 * 1024) + b"}"
    for content_type, chunked in ((AMZ_JSON, False), ("text/plain", False), ("text/plain", True)):
        answer = target.post(path, content_type=content_type, body=big, chunked=chunked)
        assert answer.status == 413, (content_type, chunked, answer)
