#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Keep the extenddb repositories on Docker Hub in a fixed listing order.

Docker Hub has no setting for the order in which it lists a namespace's
repositories. It lists them by last update, newest first, and each image push
sets that date, so every publish moves its image to the top. This script puts
the list back in ORDER by pushing the manifest that each repository's `latest`
tag already points to, byte for byte. The digest stays the same, so the image,
its signature and every pinned pull stay the same.

It pushes only what is out of order: it finds the lowest repository that is
not newer than the one below it, then pushes that one and each one above it,
from the bottom up. Without --apply it needs no credentials. It prints the
plan and checks each manifest it would push.

Usage:
    ci/dockerhub-order.py                        # dry run
    ci/dockerhub-order.py --apply                # push
    ci/dockerhub-order.py --apply --warn-only    # CI: a failure only warns

The publish workflows run it with --apply --warn-only after their Docker Hub
pushes. By hand, set DOCKERHUB_TOKEN or answer the prompt, which does not
echo. DOCKERHUB_USERNAME defaults to extenddb.
"""

import argparse
import base64
import getpass
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

NAMESPACE = "extenddb"
# The listing order, top first.
ORDER = ["extenddb-postgres", "extenddb-dev", "extenddb-cassandra", "extenddb-mongodb"]
TAG = "latest"
HUB = "https://hub.docker.com/v2"
AUTH = "https://auth.docker.io/token"
REGISTRY = "https://registry-1.docker.io/v2"
MANIFEST_TYPES = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


class Stop(Exception):
    """A reason to stop before the next push."""


def request(method, url, headers=None, data=None):
    headers = dict(headers or {}, **{"User-Agent": "extenddb-dockerhub-order"})
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.headers, err.read()


def get_json(url, headers=None, missing_ok=False):
    status, _, body = request("GET", url, headers)
    if status == 404 and missing_ok:
        return None
    if status != 200:
        raise Stop(f"GET {url.split('?')[0]} returned {status}: {body[:200]!r}")
    return json.loads(body)


def parse_time(text):
    # Docker Hub writes from 0 to 6 fractional digits.
    whole, _, frac = text.rstrip("Z").partition(".")
    stamp = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S")
    return stamp.replace(microsecond=int((frac + "000000")[:6]), tzinfo=timezone.utc)


def last_updated(repo):
    info = get_json(f"{HUB}/namespaces/{NAMESPACE}/repositories/{repo}")
    return parse_time(info["last_updated"])


def tag_digest(repo):
    tag = get_json(f"{HUB}/namespaces/{NAMESPACE}/repositories/{repo}/tags/{TAG}", missing_ok=True)
    return tag["digest"] if tag else None


def listed_order():
    page = get_json(f"{HUB}/namespaces/{NAMESPACE}/repositories?page_size=100&ordering=last_updated")
    return [r["name"] for r in page["results"]]


def out_of_order(stamps):
    """Return the repositories to push, bottom first, so the list matches ORDER."""
    for i in range(len(ORDER) - 2, -1, -1):
        if stamps[ORDER[i]] <= stamps[ORDER[i + 1]]:
            return ORDER[i::-1]
    return []


def credentials():
    user = os.environ.get("DOCKERHUB_USERNAME") or NAMESPACE
    secret = os.environ.get("DOCKERHUB_TOKEN")
    if not secret:
        if not sys.stdin.isatty():
            raise Stop("DOCKERHUB_TOKEN is not set")
        secret = getpass.getpass(f"Docker Hub token for {user}: ")
    return user, secret


def registry_token(repo, creds=None):
    scope = "pull,push" if creds else "pull"
    query = urllib.parse.urlencode({"service": "registry.docker.io",
                                    "scope": f"repository:{NAMESPACE}/{repo}:{scope}"})
    headers = {}
    if creds:
        pair = ":".join(creds).encode()
        headers["Authorization"] = "Basic " + base64.b64encode(pair).decode()
    reply = get_json(f"{AUTH}?{query}", headers)
    return reply.get("token") or reply["access_token"]


def manifest_url(repo):
    return f"{REGISTRY}/{NAMESPACE}/{repo}/manifests/{TAG}"


def fetch_manifest(repo, token, expected):
    status, headers, body = request("GET", manifest_url(repo), {
        "Authorization": f"Bearer {token}", "Accept": MANIFEST_TYPES})
    if status != 200:
        raise Stop(f"{repo}: GET manifest returned {status}: {body[:200]!r}")
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    if not (digest == headers.get("Docker-Content-Digest") == expected):
        raise Stop(f"{repo}: the bytes hash to {digest}, the registry says "
                   f"{headers.get('Docker-Content-Digest')}, Docker Hub says {expected}")
    return headers["Content-Type"], body


def push_again(repo, creds, expected):
    token = registry_token(repo, creds)
    content_type, body = fetch_manifest(repo, token, expected)
    status, _, reply = request("PUT", manifest_url(repo), {
        "Authorization": f"Bearer {token}", "Content-Type": content_type}, body)
    if status not in (200, 201):
        raise Stop(f"{repo}: PUT manifest returned {status}: {reply[:200]!r}")
    _, headers, _ = request("HEAD", manifest_url(repo), {
        "Authorization": f"Bearer {token}", "Accept": MANIFEST_TYPES})
    if headers.get("Docker-Content-Digest") != expected:
        raise Stop(f"{repo}: {TAG} now resolves to {headers.get('Docker-Content-Digest')}, "
                   f"expected {expected}")


def wait_for_move(repo, before, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(3)
        after = last_updated(repo)
        if after > before:
            return after
    raise Stop(f"{repo}: last_updated did not move within {timeout} s of the push")


def short(repo):
    return repo.replace(f"{NAMESPACE}-", "")


def show(label, names):
    print(f"{label:<13}" + ", ".join(short(n) for n in names))


def run(apply):
    stamps = {repo: last_updated(repo) for repo in ORDER}
    show("Listed now:", listed_order())
    show("Wanted:", ORDER)
    for repo in ORDER:
        print(f"  {short(repo):<10} last updated {stamps[repo]:%Y-%m-%d %H:%M:%S} UTC")

    plan = out_of_order(stamps)
    if not plan:
        print("Already in order. Nothing to push.")
        return
    digests = {repo: tag_digest(repo) for repo in plan}
    for repo in plan:
        if digests[repo] is None:
            raise Stop(f"{repo} has no {TAG} tag yet, so it cannot move up "
                       "until its first image is published")
    show("Push order:", plan)

    if not apply:
        for repo in plan:
            fetch_manifest(repo, registry_token(repo), digests[repo])
            print(f"  {short(repo):<10} {TAG} is {digests[repo]}, and the bytes match it")
        print("Dry run. Run again with --apply to push.")
        return

    creds = credentials()
    for repo in plan:
        before = last_updated(repo)
        push_again(repo, creds, digests[repo])
        after = wait_for_move(repo, before)
        print(f"  {short(repo):<10} pushed {TAG} again, same digest, "
              f"last updated {after:%Y-%m-%d %H:%M:%S} UTC")

    deadline = time.monotonic() + 60
    while True:
        listed = listed_order()
        if [n for n in listed if n in ORDER] == ORDER or time.monotonic() > deadline:
            break
        time.sleep(5)
    show("Listed now:", listed)
    if [n for n in listed if n in ORDER] != ORDER:
        raise Stop("the listing does not show the wanted order yet")


def report(message, warn_only):
    if warn_only:
        # Workflow command data must escape %, CR and LF.
        data = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::warning title=Docker Hub listing order::{data}")
        return 0
    print(f"Stopped: {message}", file=sys.stderr)
    return 1


def main(argv):
    parser = argparse.ArgumentParser(
        description="Keep the extenddb repositories on Docker Hub in a fixed listing order.")
    parser.add_argument("--apply", action="store_true",
                        help="push; without it, print the plan and check the manifests")
    parser.add_argument("--warn-only", action="store_true",
                        help="report any failure as a GitHub Actions warning and exit 0")
    args = parser.parse_args(argv)
    try:
        run(args.apply)
    except Stop as err:
        return report(str(err), args.warn_only)
    except Exception as err:
        if not args.warn_only:
            raise
        return report(f"{type(err).__name__}: {err}", True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
