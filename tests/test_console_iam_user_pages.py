# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Console pages of an IAM user's own account serve the user its own user
page and its own access keys only.

The management API gives an IAM user its own access keys (list, create and
delete) and its own password, and answers every other read and write with
HTTP 403. The console gives an IAM-user session the same: its own user page,
which lists its access keys, and the forms that create and delete them.
Every other page and form under its account redirects it to /console, and no
page it can open names another user, group or role, or links to a page that
redirects it.

The account holds a user `owner` with an inline policy, an access key and a
tag; a second user `peer`; a group with `owner` as its member and an inline
policy; and a role with an inline policy. The IAM user `viewer` is created
with a console password, so it holds only the self-service policy that every
such user gets. It is the member of a second group and has a tag, which its
own page leaves out. Each name carries a random suffix, so that no page
shows it by chance.

Prerequisites match tests/test_console_account_scope.py:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Callable

import pytest
import requests

from management_helpers import ManagementClient

_CSRF_RE = re.compile(r'<meta\s+name="csrf-token"\s+content="([^"]+)"')
_HREF_RE = re.compile(r'href="([^"]+)"')
_KEY_RE = re.compile(r'<div class="secret-box">(AKIA[^<]+)</div>')

# The inline policy every user created with a console password holds.
_SELF_SERVICE_POLICY = "SelfServicePolicy"

_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}],
}

_FULL_ACCESS = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
}


def _trust_policy(account_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: console IAM user page tests require "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER, and "
            "EXTENDDB_ADMIN_PASSWORD. These must be set by devtools/run-tests "
            "before test execution."
        )
    return endpoint, admin_user, admin_pass


class Console:
    """One browser: a cookie jar talking to the server's console."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = requests.Session()
        # Self-signed certs from ``extenddb init``. The harness sets
        # REQUESTS_CA_BUNDLE, which overrides a session-level ``verify``, so
        # the environment is ignored and ``verify`` is passed per request.
        self.http.trust_env = False
        self.verify = not self.base_url.startswith("https://")
        self.csrf = ""

    def login(self, username: str, password: str) -> requests.Response:
        """Log in and return the home page the login redirects to."""
        resp = self.http.post(
            f"{self.base_url}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )
        assert resp.status_code == 303, (
            f"login as {username}: {resp.status_code} {resp.text[:200]}"
        )
        assert resp.headers.get("location") == "/console"
        home = self.get("")
        assert home.status_code == 200, home.status_code
        match = _CSRF_RE.search(home.text)
        assert match, "the console home page carries no CSRF token"
        self.csrf = match.group(1)
        return home

    def get(self, path: str) -> requests.Response:
        return self.http.get(
            f"{self.base_url}/console{path}",
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )

    def open(self, url: str) -> requests.Response:
        """GET a link target as it appears in a page (`/console/...`)."""
        assert url.startswith("/console"), url
        return self.get(url[len("/console"):])

    def post(self, path: str, form: dict[str, str]) -> requests.Response:
        return self.http.post(
            f"{self.base_url}/console{path}",
            data={"_csrf": self.csrf, **form},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )


@dataclass
class World:
    """The account, the names in it, and the sessions that read it."""

    endpoint: str
    mgmt: ManagementClient
    admin: Console
    viewer: Console
    account_id: str
    account_name: str
    owner: str
    peer: str
    group: str
    role: str
    user_policy: str
    group_policy: str
    role_policy: str
    owner_key: str
    tag_key: str
    tag_value: str
    viewer_name: str
    viewer_password: str
    viewer_key: str
    viewer_group: str  # the group the viewer is a member of
    viewer_tag_key: str
    viewer_tag_value: str
    missing_user: str  # a name that no user has

    def others(self) -> list[str]:
        """Every name of the account, its other users, its groups and its
        role, which no page the viewer opens may show."""
        return [
            self.account_name, self.owner, self.peer, self.group, self.role,
            self.user_policy, self.group_policy, self.role_policy,
            self.owner_key, self.tag_key, self.tag_value, self.viewer_group,
        ]

    def viewer_hidden(self) -> list[str]:
        """The viewer's group and tag, which the management API does not
        give the viewer and its own page leaves out."""
        return [self.viewer_group, self.viewer_tag_key, self.viewer_tag_value]

    def viewer_auth(self) -> tuple[str, str]:
        return (f"{self.account_id}/{self.viewer_name}", self.viewer_password)


@pytest.fixture(scope="module")
def world():
    endpoint, admin_user, admin_pass = _require_auth_env()
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)
    suffix = uuid.uuid4().hex[:10]
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    w = World(
        endpoint=endpoint,
        mgmt=mgmt,
        admin=Console(endpoint),
        viewer=Console(endpoint),
        account_id=account_id,
        account_name=f"own-pages-{suffix}",
        owner=f"owner-{suffix}",
        peer=f"peer-{suffix}",
        group=f"group-{suffix}",
        role=f"role-{suffix}",
        user_policy=f"user-policy-{suffix}",
        group_policy=f"group-policy-{suffix}",
        role_policy=f"role-policy-{suffix}",
        owner_key="",
        tag_key=f"tag-key-{suffix}",
        tag_value=f"tag-value-{suffix}",
        viewer_name=f"viewer-{suffix}",
        viewer_password=uuid.uuid4().hex,
        viewer_key="",
        viewer_group=f"viewer-group-{suffix}",
        viewer_tag_key=f"viewer-tag-key-{suffix}",
        viewer_tag_value=f"viewer-tag-value-{suffix}",
        missing_user=f"nobody-{suffix}",
    )
    a = w.account_id
    try:
        steps: list[Callable[[], requests.Response]] = [
            lambda: mgmt.create_account(a, w.account_name),
            lambda: mgmt.create_user(a, w.owner),
            lambda: mgmt.create_user(a, w.peer),
            lambda: mgmt.create_user(a, w.viewer_name, w.viewer_password),
            lambda: mgmt.create_group(a, w.group),
            lambda: mgmt.add_group_member(a, w.group, w.owner),
            lambda: mgmt.create_role(a, w.role, _trust_policy(a)),
            lambda: mgmt.put_user_policy(a, w.owner, w.user_policy, _POLICY),
            lambda: mgmt.put_group_policy(a, w.group, w.group_policy, _POLICY),
            lambda: mgmt.put_role_policy(a, w.role, w.role_policy, _POLICY),
            lambda: mgmt.tag_user(a, w.owner, {w.tag_key: w.tag_value}),
            lambda: mgmt.create_group(a, w.viewer_group),
            lambda: mgmt.add_group_member(a, w.viewer_group, w.viewer_name),
            lambda: mgmt.tag_user(a, w.viewer_name, {w.viewer_tag_key: w.viewer_tag_value}),
        ]
        for step in steps:
            resp = step()
            assert resp.status_code in (200, 201, 204), f"{resp.url}: {resp.text}"
        resp = mgmt.create_access_key(a, w.owner)
        assert resp.status_code == 201, resp.text
        w.owner_key = resp.json()["access_key_id"]
        resp = mgmt.create_access_key(a, w.viewer_name)
        assert resp.status_code == 201, resp.text
        w.viewer_key = resp.json()["access_key_id"]

        w.admin.login(admin_user, admin_pass)
        w.viewer.login(*w.viewer_auth())
        yield w
    finally:
        mgmt.delete_user_policy(a, w.owner, w.user_policy)
        mgmt.delete_group_policy(a, w.group, w.group_policy)
        mgmt.delete_role_policy(a, w.role, w.role_policy)
        mgmt.remove_group_member(a, w.group, w.owner)
        mgmt.delete_group(a, w.group)
        mgmt.remove_group_member(a, w.viewer_group, w.viewer_name)
        mgmt.delete_group(a, w.viewer_group)
        mgmt.delete_role(a, w.role)
        for user in (w.owner, w.peer, w.viewer_name):
            keys = mgmt.list_access_keys(a, user)
            for key in keys.json() if keys.status_code == 200 else []:
                mgmt.delete_access_key(a, user, key["access_key_id"])
            mgmt.delete_user(a, user)
        mgmt.delete_account(a)


# Console paths that answer GET and redirect an IAM-user session, each named
# by what it shows an admin.
PAGES: dict[str, Callable[[World], str]] = {
    "accounts": lambda w: "/accounts",
    "new-account-form": lambda w: "/accounts/new",
    "account": lambda w: f"/accounts/{w.account_id}",
    "owner": lambda w: f"/accounts/{w.account_id}/users/{w.owner}",
    "peer": lambda w: f"/accounts/{w.account_id}/users/{w.peer}",
    "missing-user": lambda w: f"/accounts/{w.account_id}/users/{w.missing_user}",
    "group": lambda w: f"/accounts/{w.account_id}/groups/{w.group}",
    "role": lambda w: f"/accounts/{w.account_id}/roles/{w.role}",
    "new-user-form": lambda w: f"/accounts/{w.account_id}/users/new",
    "new-group-form": lambda w: f"/accounts/{w.account_id}/groups/new",
    "new-role-form": lambda w: f"/accounts/{w.account_id}/roles/new",
    "own-policy-form": lambda w: (
        f"/accounts/{w.account_id}/users/{w.viewer_name}/policies/new"),
    "owner-policy-form": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}/policies/new"),
    "group-policy-form": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}/policies/new"),
    "role-policy-form": lambda w: (
        f"/accounts/{w.account_id}/roles/{w.role}/policies/new"),
}

# Console forms that redirect an IAM-user session, with a complete form for
# each so that the handler runs. Each one changes the catalog if it is let
# through. Several name the viewer itself: its own user, its own policies and
# its own group membership are for admins to change.
FORMS: dict[str, Callable[[World], tuple[str, dict[str, str]]]] = {
    "create-account": lambda w: (
        "/accounts/new", {"account_id": "", "account_name": f"new-{w.account_name}"}),
    "delete-account": lambda w: (f"/accounts/{w.account_id}/delete", {}),
    "create-user": lambda w: (
        f"/accounts/{w.account_id}/users/new", {"user_name": f"new-{w.peer}", "password": ""}),
    "delete-peer": lambda w: (f"/accounts/{w.account_id}/users/{w.peer}/delete", {}),
    "delete-self": lambda w: (
        f"/accounts/{w.account_id}/users/{w.viewer_name}/delete", {}),
    "create-owner-key": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}/access-keys/new", {}),
    "delete-owner-key": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}/access-keys/{w.owner_key}/delete", {}),
    "create-group": lambda w: (
        f"/accounts/{w.account_id}/groups/new", {"group_name": f"new-{w.group}"}),
    "delete-group": lambda w: (f"/accounts/{w.account_id}/groups/{w.group}/delete", {}),
    "join-group": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}/members/add",
        {"user_name": w.viewer_name}),
    "remove-member": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}/members/{w.owner}/remove", {}),
    "create-role": lambda w: (
        f"/accounts/{w.account_id}/roles/new",
        {"role_name": f"new-{w.role}", "trust_policy": json.dumps(_trust_policy(w.account_id))}),
    "delete-role": lambda w: (f"/accounts/{w.account_id}/roles/{w.role}/delete", {}),
    "put-own-policy": lambda w: (
        f"/accounts/{w.account_id}/users/{w.viewer_name}/policies/new",
        {"policy_name": f"self-{w.user_policy}", "policy_document": json.dumps(_FULL_ACCESS)}),
    "delete-own-policy": lambda w: (
        f"/accounts/{w.account_id}/users/{w.viewer_name}/policies/{_SELF_SERVICE_POLICY}/delete",
        {}),
    "put-owner-policy": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}/policies/new",
        {"policy_name": f"new-{w.user_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-owner-policy": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}/policies/{w.user_policy}/delete", {}),
    "put-group-policy": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}/policies/new",
        {"policy_name": f"new-{w.group_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-group-policy": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}/policies/{w.group_policy}/delete", {}),
    "put-role-policy": lambda w: (
        f"/accounts/{w.account_id}/roles/{w.role}/policies/new",
        {"policy_name": f"new-{w.role_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-role-policy": lambda w: (
        f"/accounts/{w.account_id}/roles/{w.role}/policies/{w.role_policy}/delete", {}),
}


def _assert_redirected_to_console(resp: requests.Response, what: str) -> None:
    assert resp.status_code == 303, f"{what}: {resp.status_code} {resp.text[:300]}"
    assert resp.headers.get("location") == "/console", what
    assert resp.content == b"", f"{what}: {resp.text[:300]}"


def _state(w: World) -> dict[str, object]:
    """What a console form could change: the admin's pages of the account,
    each user, the group and the role, and whether the account list holds
    this account and the account the create-account form names. The admin's
    console session reads them without a password check per request, which
    a management API read would need. Other test files create and delete
    accounts while this one runs, so the rest of the account list is not
    compared."""
    a = w.account_id
    paths = [f"/accounts/{a}", f"/accounts/{a}/groups/{w.group}",
             f"/accounts/{a}/roles/{w.role}"]
    paths += [f"/accounts/{a}/users/{u}" for u in (w.owner, w.peer, w.viewer_name)]
    state: dict[str, object] = {}
    for path in paths:
        page = w.admin.get(path)
        assert page.status_code == 200, f"{path}: {page.status_code}"
        state[path] = page.text
    accounts = w.admin.get("/accounts")
    assert accounts.status_code == 200, accounts.status_code
    state["/accounts"] = (a in accounts.text, f"new-{w.account_name}" in accounts.text)
    return state


@pytest.mark.parametrize("page", sorted(PAGES))
def test_refused_page_redirects_to_console(world: World, page: str) -> None:
    resp = world.viewer.get(PAGES[page](world))
    _assert_redirected_to_console(resp, page)
    for name in world.others():
        assert name not in str(resp.headers), f"{page} headers name {name}"


def test_another_user_and_a_missing_user_get_the_same_answer(world: World) -> None:
    other = world.viewer.get(PAGES["owner"](world))
    missing = world.viewer.get(PAGES["missing-user"](world))
    assert (missing.status_code, missing.headers.get("location"), missing.content) == (
        other.status_code, other.headers.get("location"), other.content)


@pytest.mark.parametrize("action", sorted(FORMS))
def test_refused_form_redirects_to_console_and_changes_nothing(
    world: World, action: str
) -> None:
    path, form = FORMS[action](world)
    before = _state(world)

    resp = world.viewer.post(path, form)

    after = _state(world)
    _assert_redirected_to_console(resp, action)
    changed = [page for page in before if after[page] != before[page]]
    assert not changed, f"{action} changed {changed}"


def test_viewer_reads_its_own_user_page(world: World) -> None:
    a, user = world.account_id, world.viewer_name
    resp = world.viewer.get(f"/accounts/{a}/users/{user}")

    assert resp.status_code == 200, resp.status_code
    assert f"<h1>User: {user}</h1>" in resp.text
    assert f"<code>{world.viewer_key}</code>" in resp.text
    assert f'action="/console/accounts/{a}/users/{user}/access-keys/new"' in resp.text
    assert (f'action="/console/accounts/{a}/users/{user}/access-keys/'
            f'{world.viewer_key}/delete"') in resp.text
    # The management API refuses the viewer its own policies and tags, so
    # the page leaves out the policies, groups and tags sections.
    for absent in ("<h2>Policies", "<h2>Groups", "<h2>Tags", _SELF_SERVICE_POLICY):
        assert absent not in resp.text, absent
    for name in world.viewer_hidden():
        assert name not in resp.text, f"the viewer's own page shows {name}"


def test_management_api_gives_the_viewer_the_same_reads(world: World) -> None:
    """The reads the console gives the viewer, and the ones it refuses, are
    the ones the management API gives and refuses the same user."""
    base = f"{world.endpoint.rstrip('/')}/management/accounts/{world.account_id}"
    verify = not world.endpoint.startswith("https://")

    def read(path: str) -> requests.Response:
        return requests.get(f"{base}{path}", auth=world.viewer_auth(), timeout=30,
                            verify=verify)

    own_keys = read(f"/users/{world.viewer_name}/access-keys")
    assert own_keys.status_code == 200, own_keys.text
    assert world.viewer_key in own_keys.text
    for path in (
        "/users",
        "/groups",
        "/roles",
        f"/users/{world.owner}/access-keys",
        f"/users/{world.owner}/policies",
        f"/users/{world.owner}/tags",
        f"/users/{world.viewer_name}/policies",
        f"/users/{world.viewer_name}/tags",
        f"/groups/{world.group}/policies",
        f"/roles/{world.role}/policies",
    ):
        resp = read(path)
        assert resp.status_code == 403, f"{path}: {resp.status_code} {resp.text[:200]}"


def test_viewer_manages_its_own_access_keys(world: World) -> None:
    a, user = world.account_id, world.viewer_name
    created = world.viewer.post(f"/accounts/{a}/users/{user}/access-keys/new", {})
    assert created.status_code == 200, created.status_code
    match = _KEY_RE.search(created.text)
    assert match, created.text[:500]
    key_id = match.group(1)
    assert f'href="/console/accounts/{a}/users/{user}"' in created.text
    assert key_id in world.mgmt.list_access_keys(a, user).text

    page = world.viewer.get(f"/accounts/{a}/users/{user}")
    assert f"<code>{key_id}</code>" in page.text

    deleted = world.viewer.post(f"/accounts/{a}/users/{user}/access-keys/{key_id}/delete", {})
    assert deleted.status_code == 303, deleted.status_code
    assert deleted.headers.get("location") == f"/console/accounts/{a}/users/{user}"
    assert key_id not in world.mgmt.list_access_keys(a, user).text
    assert world.viewer_key in world.mgmt.list_access_keys(a, user).text


def _viewer_key_ids(w: World) -> set[str]:
    """The viewer's access key IDs, as the management API lists them."""
    resp = w.mgmt.list_access_keys(w.account_id, w.viewer_name)
    assert resp.status_code == 200, resp.text
    return {key["access_key_id"] for key in resp.json()}


# A `_csrf` value other than the session's token: an empty one, and one of
# the same length that differs from the token in its last character.
WRONG_CSRF: dict[str, Callable[[str], str]] = {
    "empty": lambda token: "",
    "altered": lambda token: token[:-1] + ("1" if token.endswith("0") else "0"),
}


@pytest.mark.parametrize("csrf", sorted(WRONG_CSRF))
@pytest.mark.parametrize("form", ["create-key", "delete-key"])
def test_key_form_with_a_wrong_csrf_token_gets_403_and_changes_no_key(
    world: World, form: str, csrf: str
) -> None:
    """The two forms an IAM-user session can submit check the session's
    CSRF token before they change a key."""
    a, user = world.account_id, world.viewer_name
    # A key for the delete form to name, so that the viewer's own key stays
    # in place if the form deletes it.
    extra = world.mgmt.create_access_key(a, user)
    assert extra.status_code == 201, extra.text
    path = {
        "create-key": f"/accounts/{a}/users/{user}/access-keys/new",
        "delete-key": (f"/accounts/{a}/users/{user}/access-keys/"
                       f"{extra.json()['access_key_id']}/delete"),
    }[form]
    before = _viewer_key_ids(world)
    try:
        resp = world.viewer.post(path, {"_csrf": WRONG_CSRF[csrf](world.viewer.csrf)})
        after = _viewer_key_ids(world)
    finally:
        for key_id in _viewer_key_ids(world) - {world.viewer_key}:
            world.mgmt.delete_access_key(a, user, key_id)

    assert resp.status_code == 403, f"{form}: {resp.status_code} {resp.text[:300]}"
    assert after == before, f"{form} changed the keys from {before} to {after}"


def _viewer_pages(world: World) -> dict[str, str]:
    """Every page the viewer opens: the home page, its user page, and the
    page that shows a key it creates. The key is deleted again."""
    a, user = world.account_id, world.viewer_name
    pages = {}
    for name, path in (("home", ""), ("user", f"/accounts/{a}/users/{user}")):
        resp = world.viewer.get(path)
        assert resp.status_code == 200, f"{name}: {resp.status_code}"
        pages[name] = resp.text
    created = world.viewer.post(f"/accounts/{a}/users/{user}/access-keys/new", {})
    assert created.status_code == 200, created.status_code
    pages["new-key"] = created.text
    match = _KEY_RE.search(created.text)
    assert match, created.text[:500]
    deleted = world.viewer.post(
        f"/accounts/{a}/users/{user}/access-keys/{match.group(1)}/delete", {})
    assert deleted.status_code == 303, deleted.status_code
    return pages


def test_no_page_the_viewer_opens_names_another_principal(world: World) -> None:
    for page, text in _viewer_pages(world).items():
        for name in world.others():
            assert name not in text, f"the {page} page shows {name}"


def test_viewer_pages_link_to_no_page_that_redirects_it(world: World) -> None:
    pages = _viewer_pages(world)
    own_page = f"/console/accounts/{world.account_id}/users/{world.viewer_name}"
    for page in ("home", "new-key"):
        assert f'href="{own_page}"' in pages[page], f"the {page} page has no link to {own_page}"
    # The Metrics, Settings and Cache links are outside the account pages,
    # and the documentation pages are public, so the check covers the home
    # page and the pages under /console/accounts.
    for page, text in pages.items():
        for link in sorted(set(_HREF_RE.findall(text))):
            if link != "/console" and not link.startswith("/console/accounts"):
                continue
            resp = world.viewer.open(link)
            assert resp.status_code == 200, (
                f"the {page} page links to {link}, which answers {resp.status_code} "
                f"{resp.headers.get('location')}")


def test_viewer_logs_in_manages_keys_and_logs_out(world: World) -> None:
    a, user = world.account_id, world.viewer_name
    browser = Console(world.endpoint)
    home = browser.login(*world.viewer_auth())
    own_page = f"/console/accounts/{a}/users/{user}"
    assert f'<a href="{own_page}">' in home.text

    page = browser.open(own_page)
    assert page.status_code == 200, page.status_code
    assert f"<code>{world.viewer_key}</code>" in page.text

    resp = browser.post("/logout", {})
    assert resp.status_code == 303, resp.status_code
    assert resp.headers.get("location") == "/console/login"
    resp = browser.open(own_page)
    assert resp.status_code == 303, resp.status_code
    assert resp.headers.get("location") == "/console/login"


# The names each page shows an admin.
SHOWN: dict[str, Callable[[World], tuple[str, list[str]]]] = {
    "accounts": lambda w: ("/accounts", [w.account_id, w.account_name]),
    "account": lambda w: (
        f"/accounts/{w.account_id}",
        [w.account_name, w.owner, w.peer, w.viewer_name, w.group, w.role]),
    "owner": lambda w: (
        f"/accounts/{w.account_id}/users/{w.owner}",
        [w.owner_key, w.user_policy, w.group, w.tag_key, w.tag_value]),
    "viewer": lambda w: (
        f"/accounts/{w.account_id}/users/{w.viewer_name}",
        [w.viewer_key, _SELF_SERVICE_POLICY, *w.viewer_hidden()]),
    "group": lambda w: (
        f"/accounts/{w.account_id}/groups/{w.group}", [w.owner, w.group_policy]),
    "role": lambda w: (f"/accounts/{w.account_id}/roles/{w.role}", [w.role_policy]),
}


@pytest.mark.parametrize("page", sorted(SHOWN))
def test_admin_reads_every_page_of_the_account(world: World, page: str) -> None:
    path, names = SHOWN[page](world)
    resp = world.admin.get(path)
    assert resp.status_code == 200, f"{page}: {resp.status_code}"
    for name in names:
        assert name in resp.text, f"{page} lacks {name}"
