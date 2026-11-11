# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/publish-crates.

The tests replace the script's calls to `cargo metadata`, to the crates.io
sparse index and API, to `cargo publish`, to the clock and to sleep with
fakes. So they use no network and publish nothing. CargoMetadataTest runs
`cargo metadata` on workspaces in a temporary directory, and RunTest starts
Python child processes.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import contextlib
import email.utils
import hashlib
import http.client
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from datetime import UTC, datetime, timedelta
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parent / "publish-crates"
START = datetime(2026, 11, 10, 14, 0, 0, tzinfo=UTC)
TOKEN = "cio-test-token-that-must-not-appear"
INDEX = "https://index.crates.io/"
API = "https://crates.io/api/v1/crates/"


def load_script():
    """Import the script, which has no .py suffix, as a module."""
    loader = SourceFileLoader("publish_crates", str(SCRIPT))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def member(name: str, *dependencies, version: str = "1.0.0", publish=None) -> dict:
    """A workspace member as `cargo metadata --no-deps` lists it. Each
    dependency is a name (a normal dependency) or a (name, kind) pair."""
    listed = []
    for dependency in dependencies:
        target, kind = dependency if isinstance(dependency, tuple) else (dependency, None)
        listed.append({"name": target, "kind": kind})
    return {"name": name, "version": version, "publish": publish, "dependencies": listed}


def package_bytes(name: str, version: str) -> bytes:
    """The bytes the fake `cargo publish` writes for a package."""
    return f"package {name} {version}\n".encode()


def too_many_requests(at: datetime) -> tuple[int, str]:
    """cargo's exit code and output for crates.io's 429 answer."""
    return 101, (
        "error: failed to publish to registry at https://crates.io\n\nCaused by:\n"
        "  the remote server responded with an error (status 429 Too Many Requests): You have "
        "published too many new crates in a short period of time. Please try again after "
        f"{email.utils.format_datetime(at, usegmt=True)} and see https://crates.io/docs/rate-limits "
        "for more details.\n"
    )


def code_answer(status: int, *headers: str) -> tuple[int, str]:
    """cargo's exit code and output for an answer without crates.io's JSON
    errors."""
    lines = "".join(f"\t{header}\n" for header in headers)
    return 101, (
        "error: failed to publish to registry at https://crates.io\n\nCaused by:\n"
        f"  failed to get a 200 OK response, got {status}\nheaders:\n{lines}body:\nslow down\n"
    )


# crates.io's answer for its limit on the versions of one crate
# (src/controllers/krate/publish.rs), as cargo prints it.
DAILY_LIMIT_ANSWER = (101, (
    "error: failed to publish to registry at https://crates.io\n\nCaused by:\n"
    "  the remote server responded with an error (status 429 Too Many Requests): You have "
    "published too many versions of this crate in the last 24 hours\n"
))


def missing_crate(name: str) -> tuple[int, str]:
    """cargo's answer when the index does not know a dependency."""
    return 101, (
        "error: failed to prepare local package for uploading\n\nCaused by:\n"
        f"  no matching package named `{name}` found\n  location searched: crates.io index\n"
        "  required by package `b v1.0.0 (/work/crates/b)`\n"
    )


def missing_version(name: str, version: str) -> tuple[int, str]:
    """cargo's answer when the index does not have a dependency's version."""
    return 101, (
        "error: failed to prepare local package for uploading\n\nCaused by:\n"
        f"  failed to select a version for the requirement `{name} = \"={version}\"`\n"
        "  candidate versions found which didn't match: 0.9.0\n  location searched: crates.io index\n"
    )


class FakeTestCase(unittest.TestCase):
    """Runs the script's main with fakes for cargo, the index, the API, the
    clock and sleep. Tests fill `members`, `index`, `cksums`, `trustpub`
    and `results`. A publish that succeeds adds its version and the cksum
    of its bytes to the index, and writes its package where cargo writes
    it, as crates.io and cargo do."""

    def setUp(self) -> None:
        self.module = load_script()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.target = Path(self.tmp.name) / "target"
        self.build = Path(self.tmp.name) / "build"
        self.metadata_extra: dict = {"build_directory": str(self.build)}
        self.members: list[dict] = []
        # Crate name to the versions crates.io has. A missing name is a
        # crate crates.io does not have.
        self.index: dict[str, list[str]] = {}
        # (crate name, version) to the cksum that the index lists, when it
        # is not the SHA-256 of package_bytes(). A fake publish records the
        # SHA-256 of the bytes it uploads.
        self.cksums: dict[tuple[str, str], str] = {}
        # Crate name to its trustpub_only flag. A crate in `index` at the
        # start requires trusted publishing unless a test says otherwise. A
        # crate that a fake publish creates does not.
        self.trustpub: dict[str, bool] = {}
        # Crate name to the results of its successive publishes: (exit code,
        # output) pairs, or functions that return one. A publish with no
        # result left succeeds.
        self.results: dict[str, list] = {}
        # Crate name to the bytes a successful publish writes, when they are
        # not package_bytes(). A publish of a crate in no_file writes no file.
        self.uploaded: dict[str, bytes] = {}
        self.no_file: set[str] = set()
        self.commands: list[list[str]] = []
        self.fetched: list[str] = []
        self.api_reads: list[str] = []
        self.slept: list[int] = []
        self.pauses = 0
        self.clock = START
        for name, fake in (("cargo_metadata", self.fake_metadata), ("fetch", self.fake_fetch),
                           ("run", self.fake_run), ("now", lambda: self.clock), ("sleep", self.fake_sleep),
                           ("api_pause", self.fake_pause)):
            patcher = mock.patch.object(self.module, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)
        environment = mock.patch.dict(os.environ, {"CARGO_REGISTRY_TOKEN": TOKEN})
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop("CARGO", None)

    def fake_metadata(self, root: Path) -> dict:
        return {"packages": self.members, "target_directory": str(self.target), **self.metadata_extra}

    def fake_fetch(self, url: str) -> bytes | None:
        if url.startswith(API):
            name = url[len(API):]
            self.api_reads.append(name)
            if name not in self.index:
                return None
            return json.dumps({"crate": {"name": name, "trustpub_only": self.trustpub.get(name, True)}}).encode()
        self.fetched.append(url)
        name = url.rsplit("/", 1)[1]
        if name not in self.index:
            return None
        return "".join(json.dumps({"name": name, "vers": v, "cksum": self.cksum_of(name, v)}) + "\n"
                       for v in self.index[name]).encode()

    def cksum_of(self, name: str, version: str) -> str:
        return self.cksums.get((name, version), hashlib.sha256(package_bytes(name, version)).hexdigest())

    def version_of(self, name: str) -> str:
        return next(package["version"] for package in self.members if package["name"] == name)

    def fake_run(self, command: list[str], root: Path) -> tuple[int, str]:
        self.commands.append(command)
        name = command[command.index("-p") + 1]
        results = self.results.get(name)
        result = results.pop(0) if results else (0, f"Uploaded {name}\n")
        if callable(result):
            result = result()
        if result[0] == 0:
            version = self.version_of(name)
            if name not in self.index:
                self.index[name] = []
                self.trustpub[name] = False
            self.index[name].append(version)
            uploaded = self.uploaded.get(name, package_bytes(name, version))
            self.cksums[(name, version)] = hashlib.sha256(uploaded).hexdigest()
            if name not in self.no_file:
                # cargo publish writes the package it uploads here, and only
                # cargo package copies it into the target directory.
                crate = self.build / "package" / "tmp-crate" / f"{name}-{version}.crate"
                crate.parent.mkdir(parents=True, exist_ok=True)
                crate.write_bytes(uploaded)
                stale = self.target / "package" / crate.name
                stale.parent.mkdir(parents=True, exist_ok=True)
                stale.write_bytes(package_bytes(name, version))
        return result

    def fake_sleep(self, seconds: int) -> None:
        self.slept.append(seconds)
        self.clock += timedelta(seconds=seconds)

    def fake_pause(self) -> None:
        self.pauses += 1

    def main(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = self.module.main(list(argv))
        output, errors = out.getvalue(), err.getvalue()
        self.assertNotIn(TOKEN, output + errors)
        return code, output, errors

    def published(self) -> list[str]:
        return [command[command.index("-p") + 1] for command in self.commands]

    def later(self, seconds: float):
        """A result: crates.io's 429 answer that asks for a wait of `seconds`
        from the time of the publish."""
        return lambda: too_many_requests(self.clock + timedelta(seconds=seconds))

    def sums_file(self, *wrong: str, leave_out: tuple[str, ...] = ()) -> Path:
        """A SHA-256 list of the members' packages. The crates in `wrong` get
        another hash, and the crates in `leave_out` get no line."""
        lines = []
        for package in self.members:
            name, version = package["name"], package["version"]
            if name in leave_out:
                continue
            digest = "0" * 64 if name in wrong else hashlib.sha256(package_bytes(name, version)).hexdigest()
            lines.append(f"{digest}  {name}-{version}.crate\n")
        path = Path(self.tmp.name) / "SHA256SUMS"
        path.write_text("".join(lines))
        return path

    def three_crates(self) -> None:
        """a, then b, which depends on a, then c, which depends on b. crates.io
        has each crate at 0.9.0."""
        self.members = [member("c", "b"), member("b", "a"), member("a")]
        self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}

    def new_crate_between(self) -> None:
        """a, b, then the new crate n, then c, which depends on n. crates.io
        has a, b and c at 0.9.0."""
        self.members = [member("c", "n"), member("n", "b"), member("b", "a"), member("a")]
        self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}


class OrderTest(FakeTestCase):
    def order(self) -> list[str]:
        return [package["name"] for package in self.module.publish_order({"packages": self.members})]

    def test_a_crate_comes_after_each_crate_it_depends_on(self) -> None:
        self.members = [member("alpha", ("mid", "build")), member("mid", "zeta"), member("zeta")]
        self.assertEqual(self.order(), ["zeta", "mid", "alpha"])

    def test_crates_that_are_ready_together_go_in_name_order(self) -> None:
        self.members = [member("c"), member("b", "a"), member("a")]
        self.assertEqual(self.order(), ["a", "b", "c"])

    def test_a_dev_dependency_does_not_order_the_crates(self) -> None:
        self.members = [member("a", ("b", "dev")), member("b", "a")]
        self.assertEqual(self.order(), ["a", "b"])

    def test_a_dependency_outside_the_workspace_is_ignored(self) -> None:
        self.members = [member("b", "serde", "a"), member("a", ("tokio", "build"))]
        self.assertEqual(self.order(), ["a", "b"])

    def test_a_member_with_publish_false_is_left_out(self) -> None:
        self.members = [member("x", "a", publish=[]), member("a")]
        self.assertEqual(self.order(), ["a"])

    def test_a_member_published_only_to_another_registry_is_left_out(self) -> None:
        self.members = [member("x", "a", publish=["my-registry"]), member("a")]
        self.assertEqual(self.order(), ["a"])

    def test_a_member_whose_publish_list_names_crates_io_is_kept(self) -> None:
        self.members = [member("x", "a", publish=["my-registry", "crates-io"]), member("a", publish=["crates-io"])]
        self.assertEqual(self.order(), ["a", "x"])

    def test_a_dependency_on_a_member_that_crates_io_does_not_receive_fails(self) -> None:
        for publish in ([], ["my-registry"]):
            with self.subTest(publish=publish):
                self.members = [member("a", "x"), member("x", publish=publish)]
                with self.assertRaisesRegex(self.module.Failure, "a depends on x, which crates.io does not receive"):
                    self.order()

    def test_a_cycle_fails_and_names_the_crates_in_it(self) -> None:
        self.members = [member("a", "b"), member("b", "c"), member("c", "a"), member("z")]
        with self.assertRaisesRegex(self.module.Failure, "in a cycle: a -> b -> c -> a$"):
            self.order()

    def test_the_cycle_named_leaves_out_a_crate_that_only_waits_for_it(self) -> None:
        self.members = [member("a", "c"), member("c", "d"), member("d", "c")]
        with self.assertRaisesRegex(self.module.Failure, "in a cycle: c -> d -> c$"):
            self.order()

    def test_the_cycle_named_follows_the_smallest_name_when_a_crate_waits_for_two(self) -> None:
        self.members = [member("a", "c", "b"), member("b", "a"), member("c", "a")]
        with self.assertRaisesRegex(self.module.Failure, "in a cycle: a -> b -> a$"):
            self.order()

    def test_the_cycle_named_starts_at_the_smallest_name_and_not_the_first_member(self) -> None:
        self.members = [member("y", "x"), member("x", "y")]
        with self.assertRaisesRegex(self.module.Failure, "in a cycle: x -> y -> x$"):
            self.order()

    def test_a_cycle_stops_the_run_before_the_index_is_read(self) -> None:
        self.members = [member("a", "b"), member("b", "a")]
        code, _, errors = self.main("--dry-run")
        self.assertEqual(code, 1)
        self.assertIn("error: the workspace crates depend on each other in a cycle: a -> b -> a", errors)
        self.assertEqual((self.fetched, self.api_reads, self.commands), ([], [], []))

    def test_the_plan_lists_the_crates_in_publish_order(self) -> None:
        self.three_crates()
        code, output, _ = self.main("--dry-run")
        self.assertEqual(code, 0, output)
        self.assertIn("Publish order, 3 crates, with trusted publishing:\n"
                      "  1. a 1.0.0: new version, publish\n  2. b 1.0.0: new version, publish\n"
                      "  3. c 1.0.0: new version, publish\nThis run publishes 3 crates.\n", output)


class CargoMetadataTest(unittest.TestCase):
    """The script against `cargo metadata` of workspaces in a temporary
    directory. Needs cargo, and no network."""

    def setUp(self) -> None:
        self.module = load_script()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.write("Cargo.toml", '[workspace]\nresolver = "2"\nmembers = ["crates/*"]\n')

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def crate(self, name: str, extra: str = "") -> None:
        self.write(f"crates/{name}/Cargo.toml",
                   f'[package]\nname = "ex-{name}"\nversion = "0.1.0"\nedition = "2021"\n{extra}')
        self.write(f"crates/{name}/src/lib.rs", "")

    def order(self) -> list[str]:
        return [package["name"] for package in self.module.publish_order(self.module.cargo_metadata(self.root))]

    def run_main(self, *argv: str, env: dict | None = None) -> tuple[int, str]:
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, env or {}), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = self.module.main(list(argv))
        return code, out.getvalue() + err.getvalue()

    def test_the_order_of_a_workspace_that_cargo_reads(self) -> None:
        self.crate("zeta", '\n[dev-dependencies]\nex-alpha = { path = "../alpha" }\n')
        self.crate("mid", '\n[dependencies]\nex-zeta = { path = "../zeta", version = "=0.1.0" }\n')
        self.crate("alpha", '\n[build-dependencies]\nex-mid = { path = "../mid", version = "=0.1.0" }\n')
        self.crate("private", 'publish = false\n\n[dependencies]\nex-zeta = { path = "../zeta" }\n')
        self.assertEqual(self.order(), ["ex-zeta", "ex-mid", "ex-alpha"])

    def test_target_specific_optional_and_renamed_dependencies_order_the_crates(self) -> None:
        # Without its dependencies, each crate would go in name order.
        self.crate("zeta")
        self.crate("yak", "\n[target.'cfg(unix)'.dependencies]\n"
                          'ex-zeta = { path = "../zeta", version = "=0.1.0" }\n'
                          "\n[target.'cfg(windows)'.dependencies]\n"
                          'ex-zeta = { path = "../zeta", version = "=0.1.0" }\n')
        self.crate("xen", '\n[dependencies]\nex-yak = { path = "../yak", version = "=0.1.0", optional = true }\n')
        self.crate("wolf", '\n[dependencies]\nalias = { package = "ex-xen", path = "../xen", version = "=0.1.0" }\n')
        self.assertEqual(self.order(), ["ex-zeta", "ex-yak", "ex-xen", "ex-wolf"])

    def test_publish_lists_of_a_workspace_that_cargo_reads(self) -> None:
        self.crate("open")
        self.crate("named", 'publish = ["crates-io"]\n')
        self.crate("other", 'publish = ["my-registry"]\n')
        self.assertEqual(self.order(), ["ex-named", "ex-open"])

    def test_root_may_be_a_member_directory(self) -> None:
        self.crate("zeta")
        self.crate("mid", '\n[dependencies]\nex-zeta = { path = "../zeta", version = "=0.1.0" }\n')
        code, output = self.run_main("--list-packages", "--root", str(self.root / "crates" / "mid"))
        self.assertEqual(code, 0, output)
        self.assertEqual(output, "ex-zeta-0.1.0.crate\nex-mid-0.1.0.crate\n")

    def test_a_failing_cargo_metadata_is_reported(self) -> None:
        self.write("Cargo.toml", "[workspace\n")
        code, output = self.run_main("--dry-run", "--root", str(self.root))
        self.assertEqual(code, 1)
        self.assertIn("error: `cargo metadata` failed in", output)

    def test_a_cargo_that_cannot_start_is_reported(self) -> None:
        code, output = self.run_main("--dry-run", "--root", str(self.root), env={"CARGO": str(self.root / "no-cargo")})
        self.assertEqual(code, 1)
        self.assertIn(f"error: cannot run `{self.root / 'no-cargo'} metadata`", output)
        self.assertNotIn("Traceback", output)


class IndexTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_script()

    def test_the_index_path_follows_the_cargo_book(self) -> None:
        cases = {"a": "1/a", "ab": "2/ab", "abc": "3/a/abc", "abcd": "ab/cd/abcd",
                 "Extenddb-Core": "ex/te/extenddb-core"}
        for name, path in cases.items():
            with self.subTest(name=name):
                self.assertEqual(self.module.index_path(name), path)

    def test_index_versions_reads_each_line_of_the_crate_file(self) -> None:
        body = (b'{"name":"example-a","vers":"0.1.0","cksum":"1"}\n'
                b'{"name":"example-a","vers":"0.2.0","cksum":"2","yanked":true}\n\n')
        with mock.patch.object(self.module, "fetch", return_value=body) as fetch:
            self.assertEqual(self.module.index_versions("example-a"), {"0.1.0": "1", "0.2.0": "2"})
        fetch.assert_called_once_with("https://index.crates.io/ex/am/example-a")

    def test_index_versions_is_none_for_a_crate_crates_io_does_not_have(self) -> None:
        with mock.patch.object(self.module, "fetch", return_value=None):
            self.assertIsNone(self.module.index_versions("example-a"))

    def test_an_index_file_that_is_not_an_index_fails_with_its_url(self) -> None:
        bodies = {"not UTF-8": b"\xff\xfe\n", "not JSON": b"<html>\n", "no vers": b'{"name":"example-a"}\n',
                  "no cksum": b'{"name":"example-a","vers":"0.1.0"}\n', "a JSON list": b'["0.1.0"]\n'}
        for label, body in bodies.items():
            with self.subTest(body=label), mock.patch.object(self.module, "fetch", return_value=body):
                with self.assertRaisesRegex(self.module.Failure,
                                            "^https://index.crates.io/ex/am/example-a holds a line that is not an index entry"):
                    self.module.index_versions("example-a")

    def fetch(self, answer, url: str = "https://index.crates.io/ex/am/example-a") -> tuple:
        """fetch with urlopen replaced. `answer` is the body to return, an
        exception for urlopen to raise, or a function that the body's read
        calls. Returns the result and urlopen's call."""
        class Response(io.BytesIO):
            def read(self, *args):
                return answer() if callable(answer) else super().read(*args)

        def urlopen(request, timeout):
            calls.append((request, timeout))
            if isinstance(answer, Exception):
                raise answer
            return Response(b"" if callable(answer) else answer)

        calls = []
        with mock.patch.object(self.module.urllib.request, "urlopen", urlopen):
            result = self.module.fetch(url)
        return result, calls[0]

    def test_fetch_sends_a_user_agent_and_a_timeout(self) -> None:
        result, (request, timeout) = self.fetch(b"line\n")
        self.assertEqual(result, b"line\n")
        self.assertEqual(request.full_url, "https://index.crates.io/ex/am/example-a")
        self.assertIn("ExtendDB/extenddb", request.get_header("User-agent"))
        self.assertEqual(timeout, 30)

    def test_fetch_reads_404_410_and_451_as_no_crate(self) -> None:
        for status in (404, 410, 451):
            with self.subTest(status=status):
                error = urllib.error.HTTPError("https://index.crates.io/x", status, "gone", {}, None)
                self.assertIsNone(self.fetch(error)[0])

    def test_fetch_fails_on_any_other_answer(self) -> None:
        for status in (403, 500):
            with self.subTest(status=status):
                error = urllib.error.HTTPError("https://index.crates.io/x", status, "no", {}, None)
                with self.assertRaisesRegex(self.module.Failure, f"answered {status} no"):
                    self.fetch(error)

    def test_fetch_fails_when_the_index_cannot_be_reached(self) -> None:
        with self.assertRaisesRegex(self.module.Failure, "^cannot read https://index.crates.io/ex/am/example-a"):
            self.fetch(urllib.error.URLError("no route to host"))

    def test_fetch_fails_with_its_url_on_a_broken_http_answer(self) -> None:
        def incomplete():
            raise http.client.IncompleteRead(b"par", 10)

        for label, answer in (("bad status line", http.client.BadStatusLine("HTTP/1.1 9x")),
                              ("incomplete body", incomplete)):
            with self.subTest(answer=label):
                with self.assertRaisesRegex(self.module.Failure, "^cannot read https://index.crates.io/ex/am/example-a"):
                    self.fetch(answer)

    def test_trusted_publishing_only_reads_the_crate_from_the_api(self) -> None:
        for flag in (True, False):
            with self.subTest(trustpub_only=flag):
                body = json.dumps({"crate": {"name": "example-a", "trustpub_only": flag}, "versions": []}).encode()
                with mock.patch.object(self.module, "fetch", return_value=body) as fetch, \
                        mock.patch.object(self.module, "api_pause") as pause:
                    self.assertIs(self.module.trusted_publishing_only("example-a"), flag)
                fetch.assert_called_once_with("https://crates.io/api/v1/crates/example-a")
                pause.assert_called_once_with()

    def test_trusted_publishing_only_needs_the_value_true(self) -> None:
        body = json.dumps({"crate": {"name": "example-a", "trustpub_only": "true"}}).encode()
        with mock.patch.object(self.module, "fetch", return_value=body), mock.patch.object(self.module, "api_pause"):
            self.assertIs(self.module.trusted_publishing_only("example-a"), False)

    def test_trusted_publishing_only_fails_on_an_answer_without_the_flag(self) -> None:
        bodies = {"not JSON": b"<html>", "no crate": b'{"errors": []}', "no flag": b'{"crate": {"name": "example-a"}}',
                  "a JSON list": b"[]"}
        for label, body in bodies.items():
            with self.subTest(body=label), mock.patch.object(self.module, "fetch", return_value=body), \
                    mock.patch.object(self.module, "api_pause"):
                with self.assertRaisesRegex(self.module.Failure,
                                            "^https://crates.io/api/v1/crates/example-a sent no crate.trustpub_only"):
                    self.module.trusted_publishing_only("example-a")

    def test_trusted_publishing_only_fails_on_a_crate_the_api_does_not_have(self) -> None:
        with mock.patch.object(self.module, "fetch", return_value=None), mock.patch.object(self.module, "api_pause"):
            with self.assertRaisesRegex(self.module.Failure, "has no crate example-a, but the crates.io index lists it"):
                self.module.trusted_publishing_only("example-a")


class PlanTest(FakeTestCase):
    def test_a_version_on_crates_io_is_skipped(self) -> None:
        self.three_crates()
        self.index["a"] = ["0.9.0", "1.0.0"]
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.published(), ["b", "c"])
        self.assertIn("  1. a 1.0.0: on crates.io, skip\n  2. b 1.0.0: new version, publish\n", output)
        self.assertIn("Published 2 crates, and skipped 1 crate that crates.io has.", output)

    def test_a_version_below_the_newest_is_skipped(self) -> None:
        self.three_crates()
        self.index["a"] = ["0.9.0", "1.0.0", "1.1.0"]
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.published(), ["b", "c"])

    def test_the_plan_reads_each_crate_from_the_index(self) -> None:
        self.three_crates()
        self.main("--dry-run")
        self.assertEqual(self.fetched, [INDEX + "1/a", INDEX + "1/b", INDEX + "1/c"])

    def test_a_crate_of_another_version_stops_the_run_before_the_index_is_read(self) -> None:
        self.members = [member("b", "a", version="1.0.1"), member("a"), member("c", version="2.0.0")]
        code, _, errors = self.main("--version", "1.0.0")
        self.assertEqual(code, 1)
        self.assertIn("error: the release is 1.0.0, and these crates have another version: b 1.0.1, c 2.0.0", errors)
        self.assertEqual((self.fetched, self.commands), ([], []))

    def test_crates_of_the_release_version_pass_the_version_check(self) -> None:
        self.three_crates()
        code, output, _ = self.main("--version", "1.0.0")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.published(), ["a", "b", "c"])

    def test_a_dry_run_prints_the_plan_and_the_stop_and_publishes_nothing(self) -> None:
        self.new_crate_between()
        self.index["a"].append("1.0.0")
        code, output, errors = self.main("--dry-run", "--no-verify")
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(self.commands, [])
        self.assertIn("  1. a 1.0.0: on crates.io, skip\n  2. b 1.0.0: new version, publish\n"
                      "  3. n 1.0.0: new crate, stop here\n  4. c 1.0.0: new version, after the stop\n"
                      "This run publishes 1 crate, then stops before n.\nDry run: nothing was published.\n", output)
        self.assertEqual(errors, "")

    def test_a_dry_run_with_new_crates_prints_its_stop(self) -> None:
        self.new_crate_between()
        self.index["b"].append("1.0.0")
        code, output, _ = self.main("--dry-run", "--new-crates")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.commands, [])
        self.assertIn("with an API token, new crates only:\n  1. a 1.0.0: new version, stop here\n"
                      "  2. b 1.0.0: on crates.io, after the stop\n  3. n 1.0.0: new crate, after the stop\n", output)
        self.assertIn("This run publishes 0 crates, then stops before a.\n", output)

    def test_a_dry_run_of_a_first_publish_plans_every_crate(self) -> None:
        self.three_crates()
        self.index = {}
        code, output, _ = self.main("--dry-run", "--new-crates")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.commands, [])
        self.assertIn("  3. c 1.0.0: new crate, publish\nThis run publishes 3 crates.\n", output)

    def test_the_plan_output_counts_the_crates_before_the_first_new_crate(self) -> None:
        self.new_crate_between()
        self.index["a"].append("1.0.0")
        output_file = Path(self.tmp.name) / "github-output"
        output_file.write_text("version=1.0.0\n")
        code, output, _ = self.main("--dry-run", "--plan-output", str(output_file))
        self.assertEqual(code, 0, output)
        self.assertEqual(output_file.read_text(), "version=1.0.0\npublish_count=1\nfirst_new=n\n")

    def test_the_plan_output_of_a_release_without_a_new_crate(self) -> None:
        self.three_crates()
        output_file = Path(self.tmp.name) / "github-output"
        self.main("--dry-run", "--plan-output", str(output_file))
        self.assertEqual(output_file.read_text(), "publish_count=3\nfirst_new=\n")

    def test_the_plan_output_of_a_first_publish(self) -> None:
        self.three_crates()
        self.index = {}
        output_file = Path(self.tmp.name) / "github-output"
        self.main("--dry-run", "--plan-output", str(output_file))
        self.assertEqual(output_file.read_text(), "publish_count=0\nfirst_new=a\n")

    def test_a_plan_output_that_cannot_be_written_fails(self) -> None:
        self.three_crates()
        code, _, errors = self.main("--dry-run", "--plan-output", str(Path(self.tmp.name) / "missing" / "out"))
        self.assertEqual(code, 1)
        self.assertIn("error: cannot write", errors)

    def test_a_negative_wait_cap_is_refused(self) -> None:
        with self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(io.StringIO()):
            self.module.main(["--max-wait", "-1"])
        self.assertEqual(caught.exception.code, 2)

    def test_expect_new_with_new_crates_is_refused(self) -> None:
        err = io.StringIO()
        with self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(err):
            self.module.main(["--new-crates", "--expect-new", "n"])
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("--expect-new applies only without --new-crates", err.getvalue())


class TrustedPublishingTest(FakeTestCase):
    def test_each_crate_on_crates_io_is_read_from_the_api_after_a_pause(self) -> None:
        self.new_crate_between()
        code, output, _ = self.main("--dry-run")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.api_reads, ["a", "b", "c"])
        self.assertEqual(self.pauses, 3)

    def test_a_crate_that_accepts_api_tokens_stops_the_run(self) -> None:
        self.three_crates()
        self.trustpub.update({"a": False, "c": False})
        for argv in (["--dry-run"], []):
            with self.subTest(argv=argv):
                code, _, errors = self.main(*argv)
                self.assertEqual(code, 1)
                self.assertEqual(self.commands, [])
                self.assertIn('error: crates.io accepts API tokens for a, c. On the Settings tab of each, select '
                              '"Require trusted publishing for all new versions"', errors)

    def test_the_mode_new_crates_reads_no_api(self) -> None:
        self.new_crate_between()
        self.trustpub["a"] = False
        code, output, _ = self.main("--dry-run", "--new-crates")
        self.assertEqual(code, 0, output)
        self.assertEqual((self.api_reads, self.pauses), ([], 0))


class ModeTest(FakeTestCase):
    def test_trusted_publishing_stops_before_the_first_new_crate(self) -> None:
        self.new_crate_between()
        code, output, errors = self.main("--no-verify")
        self.assertEqual(code, 1, output)
        self.assertEqual(self.published(), ["a", "b"])
        self.assertIn("This run publishes 2 crates, then stops before n.\n", output)
        self.assertIn("Published 2 crates, and skipped 0 crates that crates.io has.\n", output)
        self.assertIn("crates.io has no crate named n, and trusted publishing cannot create a\ncrate.", output)
        self.assertIn("runs devtools/publish-crates --new-crates for it", output)
        self.assertIn('packaging/README.md, "Publishing new crates with an API token"', output)
        self.assertEqual(errors, "")

    def test_the_summary_counts_only_the_crates_before_the_stop(self) -> None:
        self.new_crate_between()
        self.index["a"].append("1.0.0")
        self.index["c"].append("1.0.0")
        code, output, _ = self.main()
        self.assertEqual(code, 1, output)
        self.assertEqual(self.published(), ["b"])
        self.assertIn("  4. c 1.0.0: on crates.io, after the stop\n", output)
        self.assertIn("Published 1 crate, and skipped 1 crate that crates.io has.\n", output)

    def test_a_stop_before_the_expected_new_crate_exits_0(self) -> None:
        self.new_crate_between()
        code, output, errors = self.main("--expect-new", "n")
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(self.published(), ["a", "b"])
        self.assertIn("Stopped before n, the new crate that the gate found.", output)
        self.assertNotIn("trusted publishing cannot create", output)

    def test_a_stop_before_another_new_crate_than_expected_exits_1(self) -> None:
        self.new_crate_between()
        code, output, errors = self.main("--expect-new", "c")
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a", "b"])
        self.assertIn("crates.io has no crate named n", output)
        self.assertIn("error: the gate found c as the first new crate, and this run stops before n", errors)

    def test_expect_new_must_name_a_crate_of_the_release(self) -> None:
        self.new_crate_between()
        for argv in (["--dry-run"], []):
            with self.subTest(argv=argv):
                code, output, errors = self.main("--expect-new", "x", *argv)
                self.assertEqual(code, 1)
                self.assertEqual(self.commands, [])
                self.assertIn("error: --expect-new names x, which is not a crate of this release", errors)
                self.assertNotIn("Dry run", output)

    def test_a_run_that_does_not_stop_says_why_the_expected_stop_did_not_happen(self) -> None:
        # The gate found n new. A token run published n, and its owners
        # required trusted publishing, before this run started.
        self.new_crate_between()
        self.index["n"] = ["1.0.0"]
        for argv, published in ((["--sha256sums", str(self.sums_file())], ["a", "b", "c"]), ([], [])):
            with self.subTest(argv=argv):
                self.commands.clear()
                code, output, errors = self.main("--expect-new", "n", *argv)
                self.assertEqual(code, 0, output + errors)
                self.assertEqual(self.published(), published)
                self.assertIn("  3. n 1.0.0: on crates.io, skip\n", output)
                self.assertTrue(output.endswith(
                    "The gate found n new, but crates.io has it now, so this run did not stop before it.\n"), output)
                self.assertNotIn("Stopped before", output)
                self.assertEqual(errors, "")

    def test_a_run_without_expect_new_that_does_not_stop_prints_no_line_about_it(self) -> None:
        self.three_crates()
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertNotIn("The gate found", output)

    def test_new_crates_stops_before_the_first_crate_that_crates_io_has(self) -> None:
        self.members = [member("z", "y"), member("y", "x"), member("x")]
        self.index = {"y": ["0.9.0"]}
        code, output, _ = self.main("--new-crates", "--no-verify")
        self.assertEqual(code, 1, output)
        self.assertEqual(self.published(), ["x"])
        self.assertEqual(self.commands[0], ["cargo", "publish", "-p", "x", "--locked", "--no-verify"])
        self.assertIn("  2. y 1.0.0: new version, stop here\n  3. z 1.0.0: new crate, after the stop\n", output)
        self.assertIn("crates.io has y, and an API token must not publish it.", output)
        self.assertIn("dispatch release-crates again with the tag", output)

    def test_new_crates_never_publishes_a_crate_that_crates_io_has(self) -> None:
        self.members = [member("n", "a"), member("a")]
        self.index = {"a": ["0.9.0"]}
        code, output, _ = self.main("--new-crates")
        self.assertEqual(code, 1, output)
        self.assertEqual(self.commands, [])
        self.assertIn("This run publishes 0 crates, then stops before a.\n", output)

    def test_new_crates_without_a_stop_exits_0(self) -> None:
        self.three_crates()
        self.index = {}
        code, output, _ = self.main("--new-crates")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.published(), ["a", "b", "c"])
        self.assertIn("Revoke the token now", output)
        self.assertIn("dispatch release-crates again with the tag, so that its index and smoke", output)


class SequenceTest(FakeTestCase):
    """Whole releases, run by run, against the fake crates.io that each
    publish updates."""

    def run_mode(self, *argv: str) -> tuple[int, list[str]]:
        self.commands.clear()
        code, output, errors = self.main("--no-verify", *argv)
        return code, self.published()

    def test_the_first_publish(self) -> None:
        self.three_crates()
        self.index = {}
        code, output, _ = self.main("--dry-run")
        self.assertEqual(code, 0, output)
        self.assertIn("This run publishes 0 crates, then stops before a.", output)
        self.assertEqual(self.run_mode("--new-crates"), (0, ["a", "b", "c"]))
        # Each crate refuses API tokens only after a maintainer sets it.
        self.assertEqual(self.run_mode(), (1, []))
        self.trustpub.update(dict.fromkeys("abc", True))
        self.assertEqual(self.run_mode(), (0, []))

    def test_a_release_that_adds_a_crate(self) -> None:
        self.new_crate_between()
        self.assertEqual(self.run_mode(), (1, ["a", "b"]))
        self.assertEqual(self.run_mode("--new-crates"), (1, ["n"]))
        self.trustpub["n"] = True
        self.assertEqual(self.run_mode(), (0, ["c"]))
        self.assertEqual(self.index, {"a": ["0.9.0", "1.0.0"], "b": ["0.9.0", "1.0.0"], "c": ["0.9.0", "1.0.0"],
                                      "n": ["1.0.0"]})

    def test_two_new_crates_with_a_crate_between_them(self) -> None:
        self.members = [member("c", "n2"), member("n2", "b"), member("b", "n1"), member("n1", "a"), member("a")]
        self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}
        self.assertEqual(self.run_mode(), (1, ["a"]))
        self.assertEqual(self.run_mode("--new-crates"), (1, ["n1"]))
        self.trustpub["n1"] = True
        self.assertEqual(self.run_mode(), (1, ["b"]))
        self.assertEqual(self.run_mode("--new-crates"), (1, ["n2"]))
        self.trustpub["n2"] = True
        self.assertEqual(self.run_mode(), (0, ["c"]))

    def test_two_new_crates_next_to_each_other(self) -> None:
        self.members = [member("c", "n2"), member("n2", "n1"), member("n1", "a"), member("a")]
        self.index = {"a": ["0.9.0"], "c": ["0.9.0"]}
        self.assertEqual(self.run_mode(), (1, ["a"]))
        self.assertEqual(self.run_mode("--new-crates"), (1, ["n1", "n2"]))
        self.trustpub.update({"n1": True, "n2": True})
        self.assertEqual(self.run_mode(), (0, ["c"]))

    def test_the_run_after_a_token_run_names_the_crate_that_accepts_tokens(self) -> None:
        self.new_crate_between()
        self.run_mode()
        self.run_mode("--new-crates")
        code, output, errors = self.main()
        self.assertEqual(code, 1)
        self.assertIn("error: crates.io accepts API tokens for n.", errors)


class PublishTest(FakeTestCase):
    def test_each_crate_is_published_locked_in_order(self) -> None:
        self.three_crates()
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.commands, [["cargo", "publish", "-p", name, "--locked"] for name in "abc"])
        # A complete run with trusted publishing prints none of the steps
        # for an API token.
        for text in ("Revoke the token", "an API token must not publish", "trusted publishing cannot create",
                     "Publishing new crates with an API token"):
            self.assertNotIn(text, output)

    def test_no_verify_reaches_cargo(self) -> None:
        self.three_crates()
        self.main("--no-verify")
        self.assertEqual(self.commands, [["cargo", "publish", "-p", name, "--locked", "--no-verify"] for name in "abc"])

    def test_the_cargo_variable_names_the_cargo_program(self) -> None:
        self.three_crates()
        os.environ["CARGO"] = "/opt/rust/bin/cargo"
        self.main()
        self.assertEqual({command[0] for command in self.commands}, {"/opt/rust/bin/cargo"})

    def test_a_failed_publish_stops_the_run(self) -> None:
        self.three_crates()
        self.results["b"] = [(101, "error: failed to prepare local package for uploading\n")]
        code, _, errors = self.main()
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a", "b"])
        self.assertEqual(self.slept, [])
        self.assertIn("error: `cargo publish` failed for b 1.0.0 with exit code 101. Published in this run: a. "
                      "Run the script again to continue, or in CI dispatch release-crates again.", errors)

    def test_a_failed_first_publish_reports_that_none_was_published(self) -> None:
        self.three_crates()
        self.results["a"] = [(101, "error: crate version `1.0.0` is already uploaded\n")]
        _, _, errors = self.main()
        self.assertIn("Published in this run: none.", errors)

    def test_uploads_that_match_the_verified_list_pass(self) -> None:
        self.three_crates()
        code, output, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(self.published(), ["a", "b", "c"])

    def test_an_upload_that_differs_from_the_verified_list_stops_the_run(self) -> None:
        self.three_crates()
        self.uploaded["b"] = b"other bytes\n"
        code, _, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a", "b"])
        other = hashlib.sha256(b"other bytes\n").hexdigest()
        verified = hashlib.sha256(package_bytes("b", "1.0.0")).hexdigest()
        self.assertIn(f"error: cargo uploaded b-1.0.0.crate with SHA-256 {other}, and the verified package has "
                      f"{verified}. crates.io keeps the uploaded bytes. Published in this run: a, b. Do not run the "
                      'script again for this release. Follow packaging/README.md, "A failure after the upload".', errors)

    def test_an_upload_without_a_package_file_stops_the_run(self) -> None:
        self.three_crates()
        self.no_file.add("a")
        code, _, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a"])
        self.assertIn(f"error: cannot read {self.build / 'package' / 'tmp-crate' / 'a-1.0.0.crate'}, the package that "
                      "cargo uploaded", errors)

    def test_the_hash_check_needs_the_build_directory_before_any_upload(self) -> None:
        self.three_crates()
        self.metadata_extra = {}
        code, _, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 1)
        self.assertEqual(self.commands, [])
        self.assertIn("error: cargo metadata names no build_directory", errors)
        self.commands.clear()
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.published(), ["a", "b", "c"])

    def test_a_list_without_a_crate_to_publish_stops_the_run_before_any_upload(self) -> None:
        self.three_crates()
        sums = self.sums_file(leave_out=("b", "c"))
        code, _, errors = self.main("--sha256sums", str(sums))
        self.assertEqual(code, 1)
        self.assertEqual(self.commands, [])
        self.assertIn(f"error: {sums} has no SHA-256 for b-1.0.0.crate, c-1.0.0.crate, so nothing was published", errors)

    def test_a_stale_package_from_an_earlier_cargo_package_does_not_pass_the_check(self) -> None:
        # The publish job runs cargo package first, which leaves the verified
        # bytes where cargo publish writes its upload. A cargo that wrote its
        # upload elsewhere would leave that file in place.
        self.three_crates()
        stale = self.build / "package" / "tmp-crate" / "a-1.0.0.crate"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(package_bytes("a", "1.0.0"))
        self.no_file.add("a")
        code, _, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a"])
        self.assertFalse(stale.exists())
        self.assertIn(f"error: cannot read {stale}, the package that cargo uploaded", errors)

    def test_without_a_list_the_upload_file_is_left_alone(self) -> None:
        self.three_crates()
        kept = self.build / "package" / "tmp-crate" / "a-1.0.0.crate"
        kept.parent.mkdir(parents=True)
        kept.write_bytes(b"left by cargo package\n")
        self.no_file.add("a")
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(kept.read_bytes(), b"left by cargo package\n")

    def test_an_upload_file_that_cannot_be_deleted_stops_the_run_before_its_upload(self) -> None:
        self.three_crates()
        blocked = self.build / "package" / "tmp-crate" / "b-1.0.0.crate"
        (blocked / "inside").mkdir(parents=True)
        code, _, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 1)
        self.assertEqual(self.published(), ["a"])
        self.assertIn(f"error: cannot delete {blocked} before the upload:", errors)
        self.assertIn("Published in this run: a.", errors)

    def test_a_list_without_a_crate_that_the_run_skips_stops_the_run_before_any_upload(self) -> None:
        self.three_crates()
        self.index["a"].append("1.0.0")
        sums = self.sums_file(leave_out=("a",))
        code, output, errors = self.main("--sha256sums", str(sums))
        self.assertEqual(code, 1)
        self.assertEqual(self.commands, [])
        self.assertIn(f"error: {sums} has no SHA-256 for a-1.0.0.crate, so nothing was published", errors)

    def test_a_skipped_crate_whose_index_cksum_matches_the_list_passes(self) -> None:
        self.three_crates()
        self.index["a"].append("1.0.0")
        code, output, errors = self.main("--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(self.published(), ["b", "c"])

    def test_a_skipped_crate_with_other_bytes_on_crates_io_stops_the_run_before_any_upload(self) -> None:
        # An earlier run uploaded a and b, and the check after the upload
        # stopped it at b. A new run must not publish c on top of b.
        self.three_crates()
        self.index["a"].append("1.0.0")
        self.index["b"].append("1.0.0")
        self.cksums[("b", "1.0.0")] = "f" * 64
        sums = self.sums_file()
        for argv in (["--dry-run"], []):
            with self.subTest(argv=argv):
                code, output, errors = self.main("--sha256sums", str(sums), *argv)
                self.assertEqual(code, 1)
                self.assertEqual(self.commands, [])
                self.assertNotIn("Dry run", output)
                self.assertIn(f"error: the crates.io index lists b-1.0.0.crate with another checksum than {sums}, "
                              "so nothing was published. crates.io serves other bytes for that version. Follow "
                              'packaging/README.md, "A failure after the upload".', errors)

    def test_each_skipped_crate_after_the_stop_is_checked_too(self) -> None:
        self.new_crate_between()
        self.index["c"].append("1.0.0")
        self.cksums[("c", "1.0.0")] = "f" * 64
        code, _, errors = self.main("--sha256sums", str(self.sums_file()), "--expect-new", "n")
        self.assertEqual(code, 1)
        self.assertEqual(self.commands, [])
        self.assertIn("lists c-1.0.0.crate with another checksum", errors)

    def test_a_dry_run_with_a_list_without_a_line_stops_before_the_dry_run_output(self) -> None:
        self.three_crates()
        sums = self.sums_file(leave_out=("b",))
        code, output, errors = self.main("--dry-run", "--sha256sums", str(sums))
        self.assertEqual(code, 1)
        self.assertEqual(self.commands, [])
        self.assertNotIn("Dry run: nothing was published.", output)
        self.assertIn(f"error: {sums} has no SHA-256 for b-1.0.0.crate, so nothing was published", errors)

    def test_a_dry_run_with_a_complete_list_passes(self) -> None:
        self.three_crates()
        code, output, errors = self.main("--dry-run", "--sha256sums", str(self.sums_file()))
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(self.commands, [])
        self.assertIn("Dry run: nothing was published.", output)

    def test_a_list_that_is_not_sha256sum_output_stops_the_run(self) -> None:
        self.three_crates()
        sums = Path(self.tmp.name) / "SHA256SUMS"
        sums.write_text(f"{'0' * 64}  a-1.0.0.crate\n\nnot a line\n")
        code, _, errors = self.main("--sha256sums", str(sums))
        self.assertEqual(code, 1)
        self.assertIn(f"error: {sums}:3 is not a line of sha256sum output", errors)
        self.assertEqual((self.fetched, self.commands), ([], []))

    def test_a_list_line_with_text_after_the_name_or_upper_case_hex_stops_the_run(self) -> None:
        self.three_crates()
        digest = hashlib.sha256(package_bytes("a", "1.0.0")).hexdigest()
        lines = {"text after the name": f"{digest}  a-1.0.0.crate extra",
                 "a blank after the name": f"{digest}  a-1.0.0.crate ",
                 "upper-case hex": f"{digest.upper()}  a-1.0.0.crate"}
        for label, line in lines.items():
            with self.subTest(line=label):
                sums = Path(self.tmp.name) / "SHA256SUMS"
                sums.write_text(line + "\n")
                code, _, errors = self.main("--sha256sums", str(sums))
                self.assertEqual(code, 1)
                self.assertIn(f"error: {sums}:1 is not a line of sha256sum output", errors)
                self.assertEqual((self.fetched, self.commands), ([], []))

    def test_a_list_in_binary_mode_is_read(self) -> None:
        self.three_crates()
        lines = "".join(f"{hashlib.sha256(package_bytes(n, '1.0.0')).hexdigest()} *{n}-1.0.0.crate\n" for n in "abc")
        sums = Path(self.tmp.name) / "SHA256SUMS"
        sums.write_text(lines)
        code, output, errors = self.main("--sha256sums", str(sums))
        self.assertEqual(code, 0, output + errors)

    def test_a_list_that_cannot_be_read_stops_the_run(self) -> None:
        self.three_crates()
        code, _, errors = self.main("--sha256sums", str(Path(self.tmp.name) / "missing"))
        self.assertEqual(code, 1)
        self.assertIn("error: cannot read", errors)


class ListPackagesTest(FakeTestCase):
    def test_the_list_names_each_package_in_publish_order(self) -> None:
        self.members = [member("c", "b"), member("x", publish=[]), member("b", "a"), member("a"),
                        member("y", publish=["my-registry"])]
        code, output, _ = self.main("--list-packages")
        self.assertEqual(code, 0, output)
        self.assertEqual(output, "a-1.0.0.crate\nb-1.0.0.crate\nc-1.0.0.crate\n")
        self.assertEqual((self.fetched, self.api_reads, self.commands), ([], [], []))

    def test_the_list_applies_the_version_check(self) -> None:
        self.members = [member("a", version="1.0.1")]
        code, output, errors = self.main("--list-packages", "--version", "1.0.0")
        self.assertEqual(code, 1)
        self.assertEqual(output, "")
        self.assertIn("these crates have another version: a 1.0.1", errors)


class RateLimitTest(FakeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.three_crates()

    def test_a_429_waits_until_the_time_crates_io_gives(self) -> None:
        self.results["b"] = [self.later(600)]
        code, output, _ = self.main()
        self.assertEqual(code, 0, output)
        self.assertEqual(self.slept, [605])
        self.assertEqual(self.published(), ["a", "b", "b", "c"])
        self.assertIn("crates.io answered 429 for b 1.0.0: waiting 605 s, 605 s of 7200 s in total", output)

    def test_a_time_already_past_waits_the_margin_only(self) -> None:
        self.results["b"] = [self.later(-30)]
        self.main()
        self.assertEqual(self.slept, [5])

    def test_part_of_a_second_rounds_up(self) -> None:
        self.clock += timedelta(milliseconds=500)
        self.results["b"] = [lambda: too_many_requests(START + timedelta(seconds=60))]
        self.main()
        self.assertEqual(self.slept, [65])

    def test_a_retry_after_header_in_seconds(self) -> None:
        self.results["b"] = [code_answer(429, "content-type: text/plain", "retry-after: 30")]
        self.main()
        self.assertEqual(self.slept, [35])

    def test_a_retry_after_header_with_a_date(self) -> None:
        self.results["b"] = [code_answer(429, "retry-after: Tue, 10 Nov 2026 14:02:00 GMT")]
        self.main()
        self.assertEqual(self.slept, [125])

    def test_a_429_without_a_time_waits_one_minute(self) -> None:
        self.results["b"] = [code_answer(429, "content-type: text/plain")]
        self.main()
        self.assertEqual(self.slept, [60])

    def test_a_429_with_a_date_that_does_not_exist_waits_one_minute(self) -> None:
        self.results["b"] = [(101, "(status 429 Too Many Requests): Please try again after Sat, 31 Feb 2026 10:00:00 GMT")]
        self.main()
        self.assertEqual(self.slept, [60])

    def test_other_answers_are_not_tried_again(self) -> None:
        answers = {
            "403": (101, "the remote server responded with an error (status 403 Forbidden): New versions of this "
                         "crate can only be published using Trusted Publishing"),
            "500": code_answer(500, "retry-after: 30"),
            "429 in a file name": (101, "error: failed to read `/work/crates/b/tests/http-429.json`: no such file\n"),
            "429 in a version": (101, "error: crate version `0.429.0` is already uploaded\n"),
        }
        for label, answer in answers.items():
            with self.subTest(answer=label):
                self.commands.clear()
                self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}
                self.results["b"] = [answer]
                code, _, _ = self.main()
                self.assertEqual(code, 1)
                self.assertEqual(self.published(), ["a", "b"])
                self.assertEqual(self.slept, [])

    def test_the_daily_limit_for_one_crate_stops_the_run_at_once(self) -> None:
        self.results["b"] = [DAILY_LIMIT_ANSWER]
        code, _, errors = self.main()
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [])
        self.assertEqual(self.published(), ["a", "b"])
        self.assertIn("error: crates.io refuses more versions of b for 24 hours, its limit for one crate. "
                      "Published in this run: a. Run the script again to continue", errors)

    def test_the_total_wait_is_capped(self) -> None:
        self.results["b"] = [self.later(600), self.later(600)]
        code, _, errors = self.main("--max-wait", "1000")
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [605])
        self.assertEqual(self.published(), ["a", "b", "b"])
        self.assertIn("error: crates.io answered 429, and a wait of 605 s before b 1.0.0 takes the total wait to "
                      "1210 s, over --max-wait 1000. Published in this run: a.", errors)

    def test_waits_add_up_across_crates(self) -> None:
        self.results = {"a": [self.later(95)], "b": [self.later(95)], "c": [self.later(95)]}
        code, _, errors = self.main("--max-wait", "250")
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [100, 100])
        self.assertIn("takes the total wait to 300 s, over --max-wait 250. Published in this run: a, b.", errors)

    def test_a_total_wait_equal_to_the_cap_is_allowed(self) -> None:
        self.results["b"] = [self.later(300), self.later(300)]
        code, output, _ = self.main("--max-wait", "610")
        self.assertEqual(code, 0, output)
        self.assertEqual(self.slept, [305, 305])
        self.assertIn("waiting 305 s, 610 s of 610 s in total", output)

    def test_a_cap_of_zero_stops_at_the_first_429(self) -> None:
        self.results["a"] = [self.later(0)]
        code, _, errors = self.main("--max-wait", "0")
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [])
        self.assertIn("over --max-wait 0. Published in this run: none.", errors)

    def test_the_default_cap_is_two_hours(self) -> None:
        for wait, code in ((7195, 0), (7196, 1)):
            with self.subTest(wait=wait):
                self.commands.clear()
                self.slept.clear()
                self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}
                self.results["b"] = [self.later(wait)]
                self.assertEqual(self.main()[0], code)

    def test_a_crate_this_run_published_and_the_index_does_not_list_is_tried_again(self) -> None:
        for label, answer in (("a crate", missing_crate("a")), ("a version", missing_version("a", "1.0.0"))):
            with self.subTest(missing=label):
                self.commands.clear()
                self.slept.clear()
                self.index = {"a": ["0.9.0"], "b": ["0.9.0"], "c": ["0.9.0"]}
                self.results["b"] = [answer]
                code, output, _ = self.main()
                self.assertEqual(code, 0, output)
                self.assertEqual(self.slept, [30])
                self.assertEqual(self.published(), ["a", "b", "b", "c"])
                self.assertIn("the crates.io index does not list a yet for b 1.0.0: waiting 30 s, 30 s of 7200 s "
                              "in total", output)

    def test_a_missing_crate_that_this_run_did_not_publish_is_not_tried_again(self) -> None:
        self.index["a"].append("1.0.0")
        self.results["b"] = [missing_crate("a")]
        code, _, errors = self.main()
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [])
        self.assertIn("`cargo publish` failed for b 1.0.0", errors)

    def test_the_waits_for_the_index_count_against_the_cap(self) -> None:
        self.results["b"] = [missing_crate("a")] * 5
        code, _, errors = self.main("--max-wait", "100")
        self.assertEqual(code, 1)
        self.assertEqual(self.slept, [30, 30, 30])
        self.assertIn("error: the crates.io index does not list a yet, and a wait of 30 s before b 1.0.0 takes the "
                      "total wait to 120 s, over --max-wait 100. Published in this run: a.", errors)


class HelpersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_script()

    def test_cargo_metadata_reads_the_members_only(self) -> None:
        answer = mock.Mock(returncode=0, stdout='{"packages": []}', stderr="")
        with mock.patch.dict(os.environ, {"CARGO": "/opt/rust/bin/cargo"}), \
                mock.patch.object(self.module.subprocess, "run", return_value=answer) as run:
            self.assertEqual(self.module.cargo_metadata(Path("/work")), {"packages": []})
        self.assertEqual(run.call_args.args[0], ["/opt/rust/bin/cargo", "metadata", "--no-deps", "--format-version", "1"])
        self.assertEqual(run.call_args.kwargs["cwd"], Path("/work"))

    def test_the_clock_is_in_utc(self) -> None:
        output = too_many_requests(datetime.now(UTC) + timedelta(seconds=60))[1]
        self.assertIn(self.module.retry_wait(output, self.module.now()), range(60, 67))

    def test_sleep_sleeps(self) -> None:
        with mock.patch.object(self.module.time, "sleep") as sleep:
            self.module.sleep(3)
        sleep.assert_called_once_with(3)

    def test_the_api_pause_is_one_second(self) -> None:
        with mock.patch.object(self.module.time, "sleep") as sleep:
            self.module.api_pause()
        sleep.assert_called_once_with(1)

    def test_counts_of_crates(self) -> None:
        self.assertEqual([self.module.count(n) for n in (0, 1, 2)], ["0 crates", "1 crate", "2 crates"])


class RunTest(unittest.TestCase):
    """run() with Python child processes in place of cargo."""

    def setUp(self) -> None:
        self.module = load_script()

    def run_child(self, code: str) -> tuple[tuple[int, str], str]:
        out = io.StringIO()
        command = [sys.executable, "-c", code]
        with contextlib.redirect_stdout(out):
            result = self.module.run(command, Path.cwd())
        return result, out.getvalue()

    def test_run_echoes_the_output_and_returns_it(self) -> None:
        result, echoed = self.run_child("print('one'); print('two'); raise SystemExit(3)")
        self.assertEqual(result, (3, "one\ntwo\n"))
        self.assertTrue(echoed.startswith(f"$ {sys.executable} -c "), echoed)
        self.assertTrue(echoed.endswith("one\ntwo\n"), echoed)

    def test_run_merges_the_error_output(self) -> None:
        result, _ = self.run_child("import sys; sys.stderr.write('got 429\\n')")
        self.assertEqual(result, (0, "got 429\n"))

    def test_run_reports_a_program_it_cannot_start(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()), \
                self.assertRaisesRegex(self.module.Failure, "cannot run /nonexistent/cargo"):
            self.module.run(["/nonexistent/cargo", "publish"], Path.cwd())


if __name__ == "__main__":
    unittest.main()
