# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/check-pr-title.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import subprocess
import sys
import unittest
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "check-pr-title"
_loader = SourceFileLoader("check_pr_title", str(SCRIPT))
_spec = spec_from_loader("check_pr_title", _loader)
cpt = module_from_spec(_spec)
_loader.exec_module(cpt)


class CheckPrTitleTest(unittest.TestCase):
    def test_accepts_the_shapes_used_on_main(self):
        for title in (
            "fix(storage-cassandra): retry a contended DeleteItem",
            "feat(backup): add backup sync",
            "ci: pin the trivy image",
            "test(devtools): cover the shard weights",
            "docs(server): describe the health endpoint",
            "chore(release): 1.1.0",
            "feat(engine)!: drop the legacy reader",
            "fix!: refuse a malformed key",
            "perf(storage-postgres): batch the index writes",
            "revert: fix(app): retry the bind",
            "build(deps): bump tokio to 1.48",
            "style: format the storage crate",
            "fix(2fa): retry",
            "fix(a.b/c-d_e): retry",
        ):
            with self.subTest(title=title):
                self.assertIsNone(cpt.check(title))

    def test_refuses_titles_outside_the_accepted_shape(self):
        shape = "not `type(scope): summary`"
        space = "not followed by one space and the summary"
        scope = "is not lowercase letters, digits"
        cases = {
            "Add new code owners for multiple directories": shape,
            "Merge main into ci/fast-checks": shape,
            'Revert "fix(app): retry the bind"': shape,
            "fix(app) : retry": shape,
            "fix(app)!!: retry": shape,
            "fix!(app): retry": shape,
            "devtools: add test-shards": "`devtools` is not one of",
            "Fix(app): retry": "`Fix` is not one of",
            "feature(app): add": "`feature` is not one of",
            "fix(): retry": "the parentheses hold no scope",
            "fix(App): retry": "the scope `App` " + scope,
            "fix(app,engine): retry": "the scope `app,engine` " + scope,
            "fix(app engine): retry": "the scope `app engine` " + scope,
            "fix(-app): retry": "the scope `-app` " + scope,
            "fix(.app): retry": "the scope `.app` " + scope,
            "fix(app):retry": space,
            "fix(app):  retry": space,
            "fix(app):\tretry": space,
            "fix(app):": space,
            "fix!:retry": space,
            " fix(app): retry": "white space",
            "fix(app): retry ": "white space",
            "fix(app): retry\nsecond line": "more than one line",
            "fix(app): retry\rsecond line": "more than one line",
        }
        for title, reason in cases.items():
            with self.subTest(title=title):
                self.assertIn(reason, cpt.check(title) or "")

    def test_the_script_exits_1_with_the_reason(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "Adding a feature"],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("not `type(scope): summary`", result.stderr)

    def test_the_script_exits_0_on_a_good_title(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "fix(app): retry"],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_script_exits_2_without_exactly_one_argument(self):
        for args in ([], ["fix: a", "fix: b"]):
            with self.subTest(args=args):
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
