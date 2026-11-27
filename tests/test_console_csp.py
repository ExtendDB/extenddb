# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The console's Content-Security-Policy, on a running server.

Each page in the table carries the policy and loads the console's script
file, and none holds script that the policy blocks: a script element without
`src`, an event handler attribute, or a `javascript:` URL. The delete forms
carry their question in `data-confirm`, which the script reads.

Prerequisites and run: as for `tests/test_console_integration.py`, whose
client and fixtures this file uses.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser

import pytest
from test_console_integration import (  # noqa: F401 (the fixtures are used by name)
    ConsoleClient,
    auth_env,
    console,
    test_account_id,
)

# Attributes that hold a URL a browser may navigate to or load.
_URL_ATTRIBUTES = {"href", "src", "action", "formaction", "xlink:href"}


class _BlockedScript(HTMLParser):
    """Collect the script that the policy does not run."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.found: list[str] = []

    def handle_starttag(self, tag, attrs) -> None:
        if tag == "script" and "src" not in {name for name, _ in attrs}:
            self.found.append("<script> without src")
        for name, value in attrs:
            # The parser has decoded entities. Before it reads the scheme, a
            # browser removes the ASCII tabs and newlines of a URL, and the
            # C0 controls and spaces at its ends.
            url = re.sub(r"^[\x00-\x20]+|[\x00-\x20]+$|[\t\n\r]", "", value or "").lower()
            if name.startswith("on"):
                self.found.append(f"<{tag} {name}=>")
            elif name in _URL_ATTRIBUTES and url.startswith("javascript:"):
                self.found.append(f"<{tag} {name}={value!r}>")

    handle_startendtag = handle_starttag


def _blocked(html: str) -> list[str]:
    parser = _BlockedScript()
    parser.feed(html)
    return parser.found


def test_the_blocked_script_check_finds_each_shape():
    for html in [
        "<script>x()</script>",
        '<script data-src="a.js">x()</script>',
        '<form ONSUBMIT="x()">',
        '<a href="#"onclick="x()">',
        "<svg/onload=x()>",
        '<a href="java&#x09;script:x()">',
        '<a href=" JavaScript:x()">',
        '<a href="\x01javascript:x()">',
    ]:
        assert _blocked(html) != [], html
    assert _blocked('<script src="/a.js"></script><p>javascript: is text</p>') == []


class TestConsoleContentSecurityPolicy:
    """The pages hold the delete forms of an account, a user, an access key, a
    group, a role and their inline policies."""

    @pytest.fixture(autouse=True)
    def setup(self, console, test_account_id):
        self.console = console
        a = self.account_id = test_account_id

        def document(statement):
            return json.dumps({"Version": "2012-10-17", "Statement": [statement]})

        allow = {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}
        assume = {"Effect": "Allow", "Action": "sts:AssumeRole",
                  "Principal": {"AWS": f"arn:aws:iam::{a}:root"}}
        policy = {"policy_name": "p1", "policy_document": document(allow)}
        role = {"role_name": "r1", "trust_policy": document(assume)}
        user = {"user_name": "u1", "password": "TestPass123!"}
        # A create redirects (303); the access key page shows the secret (200).
        steps = [
            ("/accounts/new", {"account_id": a, "account_name": f"csp-{a}"}, 303),
            (f"/accounts/{a}/users/new", user, 303),
            (f"/accounts/{a}/users/u1/access-keys/new", {}, 200),
            (f"/accounts/{a}/users/u1/policies/new", policy, 303),
            (f"/accounts/{a}/groups/new", {"group_name": "g1"}, 303),
            (f"/accounts/{a}/groups/g1/policies/new", policy, 303),
            (f"/accounts/{a}/roles/new", role, 303),
            (f"/accounts/{a}/roles/r1/policies/new", policy, 303),
        ]
        try:
            for path, data, status in steps:
                resp = self.console.post(path, data)
                assert resp.status_code == status, f"{path}: {resp.status_code}"
            yield
        finally:
            self.console.post(f"/accounts/{a}/delete")

    @pytest.mark.parametrize("page", [
        "/login",
        "",
        "/accounts",
        "/accounts/{a}",
        "/accounts/{a}/users/u1",
        "/accounts/{a}/groups/g1",
        "/accounts/{a}/roles/r1",
        "/metrics",
        "/cache",
        "/settings",
        "/docs",
    ])
    def test_page_carries_the_policy_and_no_blocked_script(self, page):
        resp = self.console.get(page.format(a=self.account_id))
        assert resp.status_code == 200, page
        policy = resp.headers.get("Content-Security-Policy", "")
        assert "script-src 'self';" in policy, page
        assert '<script src="/console/assets/console.js"></script>' in resp.text, page
        assert _blocked(resp.text) == [], page

    # Every new user also gets the SelfServicePolicy inline policy. A backend
    # lists the policies in the order of its own collation, so the check
    # sorts the questions.
    @pytest.mark.parametrize(("page", "questions"), [
        ("/accounts/{a}", ["Delete account {a}?"]),
        ("/accounts/{a}/users/u1", [
            "Delete user u1?",
            "Delete key <key>?",
            "Delete policy SelfServicePolicy?",
            "Delete policy p1?",
        ]),
        ("/accounts/{a}/groups/g1", ["Delete group g1?", "Delete policy p1?"]),
        ("/accounts/{a}/roles/r1", ["Delete role r1?", "Delete policy p1?"]),
    ])
    def test_delete_forms_carry_the_question_in_data_confirm(self, page, questions):
        resp = self.console.get(page.format(a=self.account_id))
        found = [
            re.sub(r"AKIA[A-Z0-9]+", "<key>", q)
            for q in re.findall(r'data-confirm="([^"]*)"', resp.text)
        ]
        expected = [q.format(a=self.account_id) for q in questions]
        assert sorted(found) == sorted(expected), page

    @pytest.mark.parametrize("name", ["console.js", "metrics.js"])
    def test_scripts_are_served_as_javascript_without_a_session(self, auth_env, name):
        resp = ConsoleClient(auth_env[0]).get(f"/assets/{name}")
        assert resp.status_code == 200
        assert resp.headers.get("Content-Type") == "text/javascript; charset=utf-8"
        assert "Content-Security-Policy" not in resp.headers
