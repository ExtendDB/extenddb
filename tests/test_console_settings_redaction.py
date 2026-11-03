# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The console settings page prints `••••••••` in place of a secret value.

`GET /console/settings` lists the runtime settings and prints the mask for a
key that `extenddb_config::should_redact` matches. The test signs in to the
console at EXTENDDB_TEST_ENDPOINT as the admin in EXTENDDB_ADMIN_USER and
EXTENDDB_ADMIN_PASSWORD, and skips when no endpoint is set. `init` writes
`encryption_key` and `catalog_version` on every backend, and the page always
lists `data_database_connection_string`, so the test runs on every backend.
"""

from __future__ import annotations

import os
import re

import pytest
import requests

MASK = "••••••••"


def _cell(page: str, key: str) -> str | None:
    m = re.search(rf"<code>{re.escape(key)}</code></td><td><code>([^<]*)</code>", page)
    return m.group(1) if m else None


def test_settings_page_masks_secret_values():
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not password:
        pytest.fail("EXTENDDB_ADMIN_PASSWORD is required for the console")
    user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip() or "admin"
    base = endpoint.rstrip("/")
    session = requests.Session()
    resp = session.post(
        f"{base}/console/login",
        data={"username": user, "password": password},
        allow_redirects=False,
        timeout=30,
        verify=False,
    )
    assert resp.status_code == 303, resp.status_code
    resp = session.get(f"{base}/console/settings", allow_redirects=False, timeout=30, verify=False)
    assert resp.status_code == 200, resp.status_code
    page = resp.text
    assert _cell(page, "encryption_key") == MASK
    assert _cell(page, "data_database_connection_string") == MASK
    # A key that matches no pattern still prints its stored value.
    assert _cell(page, "catalog_version") not in (None, "", MASK)
