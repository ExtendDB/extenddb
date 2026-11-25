# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""`conftest.call_with_handshake_retry` retries only an SSLError raised in the
TLS handshake, before the request was sent. Local sockets stand in for the server, so these
tests need no ExtendDB server.
"""

from __future__ import annotations

import contextlib
import shutil
import socket
import ssl
import subprocess
import threading

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ConnectionClosedError, SSLError

from conftest import call_with_handshake_retry


def _self_signed_cert(tmp_path) -> tuple[str, str]:
    """A throwaway self-signed cert and key, written with openssl; their paths.
    The client sets verify=False, so the cert only has to let the handshake
    complete."""
    if not shutil.which("openssl"):
        pytest.fail("MISCONFIGURED: openssl is required for the TLS listener test")
    cert = str(tmp_path / "cert.pem")
    key = str(tmp_path / "key.pem")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key, "-out", cert, "-days", "1",
            "-subj", "/CN=127.0.0.1",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return cert, key


@contextlib.contextmanager
def _tls_listener(cert: str, key: str, handler):
    """A TLS listener that completes the handshake, then calls `handler(tls)`
    with the accepted socket; yields its port. On exit it shuts the listening
    socket down, which ends the thread: on Linux, close alone does not wake a
    blocked accept."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(16)

    def serve():
        while True:
            try:
                raw, _ = sock.accept()
            except OSError:
                return
            try:
                with context.wrap_socket(raw, server_side=True) as tls:
                    handler(tls)
            except (ssl.SSLError, OSError):
                pass

    threading.Thread(target=serve, daemon=True).start()
    try:
        yield sock.getsockname()[1]
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


def _read_request(tls) -> bytes:
    """The whole HTTP request: the headers and `Content-Length` bytes of body.
    botocore sends the two in separate TLS records. A body left unread when the
    socket closes makes the kernel send a reset in place of a FIN."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = tls.recv(65536)
        if not chunk:
            return data
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(body) < length:
        chunk = tls.recv(65536)
        if not chunk:
            break
        body += chunk
    return head + b"\r\n\r\n" + body


def _write_raw_bytes(tls) -> None:
    """Write bytes that are not a TLS record. The base-class sendall writes past
    the TLS layer, so the client reads a malformed record in place of an
    answer."""
    socket.socket.sendall(tls, b"not a tls record")


def _requests(got: list[bytes]) -> int:
    return sum(1 for r in got if r.startswith(b"POST "))


def _listener(reply: bytes) -> int:
    """A listener that sends `reply` and closes each connection; its port."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(16)

    def serve():
        while True:
            conn, _ = sock.accept()
            try:
                conn.recv(65536)
                conn.sendall(reply)
            finally:
                conn.close()

    threading.Thread(target=serve, daemon=True).start()
    return sock.getsockname()[1]


def _client(url: str):
    return boto3.client(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url=url,
        verify=False,
        aws_access_key_id="x",
        aws_secret_access_key="y",
        config=Config(retries={"total_max_attempts": 1}),
    )


def _counting(fn):
    calls = []

    def wrapped(*args, **kwargs):
        calls.append(1)
        return fn(*args, **kwargs)

    return wrapped, calls


def test_a_handshake_failure_is_retried_and_printed(capsys):
    port = _listener(b"HTTP/1.1 400 Bad Request\r\n\r\n")
    call, calls = _counting(_client(f"https://127.0.0.1:{port}").list_tables)
    with pytest.raises(SSLError):
        call_with_handshake_retry(call, attempts=3)
    assert len(calls) == 3
    assert capsys.readouterr().out.count("retrying after a TLS handshake failure") == 2


def test_a_connection_closed_without_an_answer_is_not_retried():
    port = _listener(b"")
    call, calls = _counting(_client(f"http://127.0.0.1:{port}").list_tables)
    with pytest.raises(ConnectionClosedError):
        call_with_handshake_retry(call, attempts=3)
    assert len(calls) == 1


def test_a_failure_after_the_handshake_is_not_retried(tmp_path):
    """A failure after the handshake must not be retried: the request was
    already sent, so a retry would resend it. The listener completes the
    handshake, reads the request, then writes raw bytes that are not a TLS
    record in place of the answer. The client reads them while decoding the
    answer and raises an SSLError whose traceback holds no handshake frame, so
    the helper must raise it without a retry. The request reaches the listener
    once."""
    cert, key = _self_signed_cert(tmp_path)
    got = []

    def handler(tls):
        got.append(_read_request(tls))
        _write_raw_bytes(tls)

    with _tls_listener(cert, key, handler) as port:
        call, calls = _counting(_client(f"https://127.0.0.1:{port}").list_tables)
        with pytest.raises(SSLError):
            call_with_handshake_retry(call, attempts=3)
    assert len(calls) == 1
    assert _requests(got) == 1


def test_a_post_send_ssl_error_is_not_retried_despite_a_handshake_context(tmp_path):
    """A post-send SSLError raised while a handshake failure is handled must not
    be read as a handshake failure: its request was already sent, so a retry
    would resend it. botocore stores the error and raises it again later, inside
    the caller's except block, so Python sets its __context__ to the handshake
    failure. The post-send call is a real request to a TLS listener that writes
    raw bytes in place of the answer."""
    port = _listener(b"HTTP/1.1 400 Bad Request\r\n\r\n")
    with pytest.raises(SSLError) as handshake:
        _client(f"https://127.0.0.1:{port}").list_tables()

    cert, key = _self_signed_cert(tmp_path)
    got = []

    def handler(tls):
        got.append(_read_request(tls))
        _write_raw_bytes(tls)

    with _tls_listener(cert, key, handler) as tls_port:
        client = _client(f"https://127.0.0.1:{tls_port}")

        def post_send():
            try:
                raise handshake.value
            except SSLError:
                return client.list_tables()

        call, calls = _counting(post_send)
        with pytest.raises(SSLError):
            call_with_handshake_retry(call, attempts=3)
    assert len(calls) == 1
    assert _requests(got) == 1
