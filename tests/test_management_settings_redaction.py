# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The management settings API prints `••••••••` in place of a secret value.

`GET /management/settings`, `GET /management/settings/{key}` and the
`PUT /management/settings/{key}` response print the mask for a key that
`extenddb_config::should_redact` matches. The tests call the server at
EXTENDDB_TEST_ENDPOINT as the admin in EXTENDDB_ADMIN_USER and
EXTENDDB_ADMIN_PASSWORD, and skip when no endpoint is set. `init` writes
`encryption_key` into the settings store on every backend, so the `GET` tests
run on every backend.

`PUT` writes only the keys `ops_settings::set_setting` accepts, and none of
`KNOWN_KEYS` matches a redaction pattern. A server built with
`mongodb-test-hooks` also accepts the table-scoped test gate keys, which carry a
table name, so the `PUT` test writes the resting value of a gate whose table
name contains `secret`. It skips on a server that refuses that key as unknown.
"""

from __future__ import annotations

import os
import uuid

import pytest
import requests

MASK = "••••••••"
UNKNOWN_COMMIT_TEST_GATE = "unknown_commit_test_gate"


@pytest.fixture(scope="module")
def settings_api() -> tuple[str, tuple[str, str]]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not password:
        pytest.fail("EXTENDDB_ADMIN_PASSWORD is required for the management API")
    user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip() or "admin"
    return f"{endpoint.rstrip('/')}/management/settings", (user, password)


def _call(method: str, url: str, auth: tuple[str, str], **kwargs) -> requests.Response:
    return requests.request(method, url, auth=auth, timeout=30, verify=False, **kwargs)


def test_list_masks_encryption_key(settings_api):
    url, auth = settings_api
    resp = _call("GET", url, auth)
    assert resp.status_code == 200, resp.text
    rows = {row["key"]: row["value"] for row in resp.json()}
    assert rows.get("encryption_key") == MASK, rows.get("encryption_key")
    # A key that matches no pattern still prints its stored value.
    assert rows.get("catalog_version") not in (None, "", MASK), rows


def test_get_masks_encryption_key(settings_api):
    url, auth = settings_api
    resp = _call("GET", f"{url}/encryption_key", auth)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"key": "encryption_key", "value": MASK}

    resp = _call("GET", f"{url}/catalog_version", auth)
    assert resp.status_code == 200, resp.text
    assert resp.json()["value"] not in ("", MASK), resp.text


def test_put_response_masks_a_secret_key(settings_api):
    url, auth = settings_api
    key = f"{UNKNOWN_COMMIT_TEST_GATE}:extenddb-test-secret-{uuid.uuid4().hex[:12]}"
    # `idle` is the gate's resting value, and no table carries this name.
    resp = _call("PUT", f"{url}/{key}", auth, json={"value": "idle"})
    if resp.status_code == 400 and "Unknown setting" in resp.text:
        pytest.skip("this build accepts no writable key that matches a redaction pattern")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"key": key, "value": MASK}

    resp = _call("GET", f"{url}/{key}", auth)
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"key": key, "value": MASK}
