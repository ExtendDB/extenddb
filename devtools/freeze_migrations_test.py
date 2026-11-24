# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/freeze-migrations.

Each test writes a registry and four RELEASED tables in the shape of the real
files to a temporary directory, and replaces the cargo test run with a fake
that prints the lines `released_line` prints. run_checksum_test is covered
by a fake cargo on PATH. The full round trip, unfreezing the last migration
on the real crates and running the script, is described in CONTRIBUTING.md,
"Adding a catalog migration", step 7.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import contextlib
import hashlib
import io
import os
import tempfile
import unittest
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parent / "freeze-migrations"
_loader = SourceFileLoader("freeze_migrations", str(SCRIPT))
_spec = spec_from_loader("freeze_migrations", _loader)
fm = module_from_spec(_spec)
_loader.exec_module(fm)

REGISTRY = """\
migration_ids! {
    /// The first one.
    Baseline = 1, "baseline",
        "create the catalog";
    Second = 2, "second",
        "change something";
    Third = 3, "third",
        "change something else";
}

pub const RELEASED_THROUGH: u32 = {bound};
"""

TABLE = """\
//! A backend.

pub(crate) const RELEASED: &[Released] = &[
    Released {{
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "{c1}",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    }},
];

const OTHER: &[u32] = &[
    1,
];
"""


def line(variant, kind="Breaking", checksum=None, store="Catalog"):
    checksum = checksum or hashlib.sha256(variant.encode()).hexdigest()
    return (
        f"    Released {{ id: MigrationId::{variant}, store: StoreKind::{store}, "
        f"checksum: \"{checksum}\", fingerprint: None, kind: Kind::{kind}, "
        "convergent: true, rebless: None },"
    )


class Fake:
    """Stands in for cargo: prints every line, records the calls."""

    def __init__(self, kinds=None, fail=(), omit=(), order=("Baseline", "Second", "Third")):
        self.kinds = kinds or {}
        self.order = order
        self.fail = set(fail)
        self.omit = set(omit)
        self.calls = []
        self.formatted = []

    def run_test(self, root, backend):
        self.calls.append(backend)
        printed = ["running 1 test"]
        for variant in self.order:
            if (backend, variant) in self.omit:
                continue
            printed.append(line(variant, self.kinds.get(variant, "Breaking")))
        printed.append("test migrations::tests::the_released_bodies_match_their_checksums ... ok")
        return backend not in self.fail, "\n".join(printed)

    def fmt(self, root, backend):
        self.formatted.append(backend)


class FreezeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.write_tree(bound=1)
        self.logs = []

    def tearDown(self):
        self.tmp.cleanup()

    def write_tree(self, bound):
        registry = self.root / fm.REGISTRY
        registry.parent.mkdir(parents=True, exist_ok=True)
        registry.write_text(REGISTRY.replace("{bound}", str(bound)), encoding="utf-8")
        for backend in fm.BACKENDS:
            path = fm.table_file(self.root, backend)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(TABLE.format(c1="b" * 64), encoding="utf-8")

    def snapshot(self):
        return {p: p.read_text(encoding="utf-8") for p in sorted(self.root.rglob("*.rs"))}

    def freeze(self, fake, version=None):
        return fm.freeze(self.root, version, run_test=fake.run_test, fmt=fake.fmt, log=self.logs.append)

    def test_raises_the_bound_and_adds_the_frozen_lines_in_id_order(self):
        fake = Fake()
        self.assertTrue(self.freeze(fake))
        registry = (self.root / fm.REGISTRY).read_text(encoding="utf-8")
        self.assertIn("pub const RELEASED_THROUGH: u32 = 3;", registry)
        for backend in fm.BACKENDS:
            text = fm.table_file(self.root, backend).read_text(encoding="utf-8")
            table = text[text.index(fm.TABLE_START) : text.index("const OTHER")]
            self.assertEqual(table.count("MigrationId::Baseline"), 1, "an existing entry is not repeated")
            self.assertLess(table.index(line("Second")), table.index(line("Third")))
            self.assertTrue(table.rstrip().endswith(line("Third") + "\n];"))
            # The next table is untouched.
            self.assertIn("const OTHER: &[u32] = &[\n    1,\n];\n", text)
        self.assertEqual(fake.formatted, list(fm.BACKENDS))
        # Four runs before the writes and four after.
        self.assertEqual(fake.calls, list(fm.BACKENDS) * 2)

    def test_sorts_the_lines_by_id_whatever_order_the_test_prints(self):
        self.assertTrue(self.freeze(Fake(order=("Third", "Baseline", "Second"))))
        for backend in fm.BACKENDS:
            text = fm.table_file(self.root, backend).read_text(encoding="utf-8")
            self.assertLess(text.index(line("Second")), text.index(line("Third")))

    def test_changes_nothing_when_the_bound_is_the_last_id(self):
        self.write_tree(bound=3)
        before = self.snapshot()
        fake = Fake()
        self.assertFalse(self.freeze(fake))
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(fake.calls, [])
        self.assertIn("nothing to freeze", self.logs[0])

    def test_refuses_a_bound_above_the_last_id(self):
        self.write_tree(bound=4)
        with self.assertRaisesRegex(fm.FreezeError, "above the last migration"):
            self.freeze(Fake())

    def test_refuses_a_breaking_migration_in_a_patch_release_and_writes_nothing(self):
        before = self.snapshot()
        fake = Fake(kinds={"Second": "Additive", "Third": "Breaking"})
        with self.assertRaisesRegex(
            fm.FreezeError,
            r"Third \(3\).*Release 1\.3\.0 instead, with the footer `Release-As: 1\.3\.0` "
            r"at the end of the description of a PR merged to main\.$",
        ):
            self.freeze(fake, version="1.2.4")
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(fake.formatted, [])

    def test_allows_a_breaking_migration_in_a_minor_release(self):
        self.assertTrue(self.freeze(Fake(), version="1.3.0"))

    def test_allows_an_additive_migration_in_a_patch_release(self):
        fake = Fake(kinds={"Second": "Additive", "Third": "Additive"})
        self.assertTrue(self.freeze(fake, version="1.2.4"))

    def test_refuses_a_breaking_migration_in_the_first_patch_release(self):
        with self.assertRaisesRegex(fm.FreezeError, "Release-As: 1.3.0"):
            self.freeze(Fake(), version="1.2.1")

    def test_allows_a_breaking_migration_in_a_0x_patch_release(self):
        self.assertTrue(self.freeze(Fake(), version="0.1.16"))

    def test_refuses_a_malformed_release_version_before_running_cargo(self):
        fake = Fake()
        with self.assertRaisesRegex(fm.FreezeError, "not MAJOR.MINOR.PATCH"):
            self.freeze(fake, version="v1.2.4")
        self.assertEqual(fake.calls, [])

    def test_refuses_a_malformed_release_version_with_additive_migrations(self):
        with self.assertRaisesRegex(fm.FreezeError, "not MAJOR.MINOR.PATCH"):
            self.freeze(Fake(kinds={"Second": "Additive", "Third": "Additive"}), version="1.2")

    def test_refuses_a_malformed_release_version_with_nothing_to_freeze(self):
        self.write_tree(bound=3)
        with self.assertRaisesRegex(fm.FreezeError, "not MAJOR.MINOR.PATCH"):
            self.freeze(Fake(), version="v1.2")

    def test_refuses_when_a_test_fails_before_the_freeze(self):
        before = self.snapshot()
        with self.assertRaisesRegex(fm.FreezeError, "mongodb: the checksum test fails before"):
            self.freeze(Fake(fail={"mongodb"}))
        self.assertEqual(self.snapshot(), before)

    def test_refuses_when_a_backend_prints_no_line_for_a_frozen_id(self):
        before = self.snapshot()
        with self.assertRaisesRegex(fm.FreezeError, "cassandra: .*no line for Third"):
            self.freeze(Fake(omit={("cassandra", "Third")}))
        self.assertEqual(self.snapshot(), before)

    def test_reports_a_test_that_fails_after_the_freeze(self):
        fake = Fake()
        original = fake.run_test

        def fails_after_writes(root, backend):
            passed, output = original(root, backend)
            return (passed and not fake.formatted), output

        fake.run_test = fails_after_writes
        with self.assertRaisesRegex(fm.FreezeError, "sqlite: the checksum test fails after.*git checkout"):
            self.freeze(fake)

    def test_refuses_a_table_file_without_one_released_table(self):
        path = fm.table_file(self.root, "postgres")
        path.write_text("const NOTHING: u32 = 0;\n", encoding="utf-8")
        before = self.snapshot()
        with self.assertRaisesRegex(fm.FreezeError, "postgres: expected exactly one"):
            self.freeze(Fake())
        self.assertEqual(self.snapshot(), before)

    def test_refuses_a_table_file_with_two_released_tables(self):
        path = fm.table_file(self.root, "mongodb")
        path.write_text(path.read_text(encoding="utf-8") * 2, encoding="utf-8")
        before = self.snapshot()
        with self.assertRaisesRegex(fm.FreezeError, "mongodb: expected exactly one"):
            self.freeze(Fake())
        self.assertEqual(self.snapshot(), before)

    def test_refuses_an_empty_or_commented_released_table(self):
        # rustfmt writes an empty table on one line. Neither shape may send
        # the lines into the array that follows.
        tail = "\nconst OTHER: &[u32] = &[\n    1,\n];\n"
        shapes = {
            "empty": fm.TABLE_START + "];\n" + tail,
            "commented": TABLE.format(c1="b" * 64).replace("    },\n];\n", "    },\n]; // end\n"),
        }
        self.assertNotEqual(shapes["commented"], TABLE.format(c1="b" * 64))
        for name, text in shapes.items():
            with self.subTest(name):
                path = fm.table_file(self.root, "sqlite")
                path.write_text(text, encoding="utf-8")
                before = self.snapshot()
                with self.assertRaisesRegex(fm.FreezeError, "sqlite: expected exactly one"):
                    self.freeze(Fake())
                self.assertEqual(self.snapshot(), before)

    def test_parses_only_whole_released_lines(self):
        output = "\n".join(
            [
                line("Second"),
                "  " + line("Third"),  # indented differently: not a pasted line
                line("Third").replace("rebless: None", 'rebless: Some("x")'),
                "panicked at: " + line("Third"),
            ]
        )
        self.assertEqual([v for v, _, _ in fm.parse_lines(output)], ["Second"])

    def test_ignores_a_line_with_trailing_text_or_a_short_checksum(self):
        output = "\n".join([line("Second") + " x", line("Third", checksum="ab")])
        self.assertEqual(fm.parse_lines(output), [])

    def test_ignores_a_line_with_an_unknown_kind(self):
        self.assertEqual(fm.parse_lines(line("Third", kind="Other")), [])

    def test_the_line_pattern_matches_what_released_line_prints(self):
        # The exact format string of `released_line` in
        # crates/storage/src/migrations/checksum.rs, with a code body's
        # fingerprint.
        printed = (
            '    Released { id: MigrationId::UniqueAccessKeyIds, store: StoreKind::Data, '
            'checksum: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", '
            'fingerprint: Some("0012_unique_access_key_ids/1"), kind: Kind::Additive, '
            'convergent: false, rebless: None },'
        )
        self.assertEqual(fm.parse_lines(printed)[0][:2], ("UniqueAccessKeyIds", "Additive"))


    def fake_cargo(self, script):
        """A cargo on PATH that runs script; it records its arguments in calls."""
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        cargo = bin_dir / "cargo"
        cargo.write_text('#!/bin/sh\necho "$@" >> "$(dirname "$0")/calls"\n' + script)
        cargo.chmod(0o755)
        return bin_dir

    def test_runs_cargo_locked_and_reads_its_exit_code(self):
        bin_dir = self.fake_cargo("cat <<'EOF'\n" + line("Third") + "\nEOF\nexit 101\n")
        with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
            passed, output = fm.run_checksum_test(self.root, "sqlite")
        self.assertFalse(passed)
        self.assertIn(line("Third"), output)
        args = (bin_dir / "calls").read_text().split()
        self.assertEqual(args[:5], ["test", "--locked", "-p", "extenddb-storage-sqlite", "--lib"])
        self.assertIn("--nocapture", args)

    def run_main(self, argv):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = fm.main(argv)
        return code, stderr.getvalue()

    def test_main_exits_1_on_a_refusal(self):
        self.write_tree(bound=4)
        code, stderr = self.run_main(["--root", str(self.root)])
        self.assertEqual(code, 1)
        self.assertIn("FAIL: RELEASED_THROUGH is 4", stderr)

    def test_main_exits_0_when_there_is_nothing_to_freeze(self):
        self.write_tree(bound=3)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.run_main(["--root", str(self.root)])[0], 0)

    def test_main_reports_a_missing_cargo(self):
        empty = self.root / "empty"
        empty.mkdir()
        with mock.patch.dict(os.environ, {"PATH": str(empty)}):
            code, stderr = self.run_main(["--root", str(self.root)])
        self.assertEqual(code, 1)
        self.assertIn("FAIL:", stderr)

    def test_main_reports_a_failing_cargo_fmt(self):
        printed = "\n".join(line(v) for v in ("Baseline", "Second", "Third"))
        bin_dir = self.fake_cargo(
            'if [ "$1" = fmt ]; then exit 1; fi\ncat <<\'EOF\'\n' + printed + "\nEOF\n"
        )
        with mock.patch.dict(os.environ, {"PATH": f"{bin_dir}:{os.environ['PATH']}"}):
            code, stderr = self.run_main(["--root", str(self.root)])
        self.assertEqual(code, 1)
        self.assertIn("FAIL:", stderr)
        self.assertIn("fmt", stderr)


if __name__ == "__main__":
    unittest.main()
