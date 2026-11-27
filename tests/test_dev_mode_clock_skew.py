# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""`EXTENDDB_DEV_IGNORE_CLOCK_SKEW` lets a dev-mode server accept stale dates.

Test suites that freeze or travel time sign requests with an `X-Amz-Date`
outside the 15 minute window (#359). With the variable set, a dev-mode server
skips that check and still verifies the signature.

These require a dev-mode server started with EXTENDDB_DEV_IGNORE_CLOCK_SKEW=1,
so they are gated on EXTENDDB_TEST_DEV_MODE and
EXTENDDB_TEST_DEV_IGNORE_CLOCK_SKEW. The default refusal is covered by
`tests/test_sigv4_scope_date.py`.
"""

from __future__ import annotations

import os

import pytest

from test_sigv4_scope_date import _send

pytestmark = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_DEV_MODE", "").strip() != "1"
    or os.environ.get("EXTENDDB_TEST_DEV_IGNORE_CLOCK_SKEW", "").strip() != "1",
    reason=(
        "requires a dev-mode server started with EXTENDDB_DEV_IGNORE_CLOCK_SKEW=1 "
        "(set EXTENDDB_TEST_DEV_MODE=1 and EXTENDDB_TEST_DEV_IGNORE_CLOCK_SKEW=1)"
    ),
)

# The seeded zero-config dev credential.
EXAMPLE_KEY = "AKIAIOSFODNN7EXAMPLE"
EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


def test_a_request_dated_years_ago_is_served(endpoint_url):
    status, error_type, message, _ = _send(
        endpoint_url, EXAMPLE_KEY, EXAMPLE_SECRET, amzdate="20210101T000000Z"
    )
    assert status == 200, (error_type, message)


def test_a_stale_request_with_a_bad_signature_is_still_refused(endpoint_url):
    status, error_type, _, _ = _send(
        endpoint_url, EXAMPLE_KEY, "not-the-secret", amzdate="20210101T000000Z"
    )
    assert status == 400
    assert error_type.endswith("InvalidSignatureException"), error_type
