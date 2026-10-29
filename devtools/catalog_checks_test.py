# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/catalog_checks.py, which the catalog check scripts
report through: a check passes only when the command's exit code and its
output both match, every failure is counted, and finish returns 1 after one.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import contextlib
import io
import unittest

import catalog_checks


class CatalogChecksTest(unittest.TestCase):
    def setUp(self) -> None:
        catalog_checks.failures = 0

    def check(self, *args, **kwargs) -> tuple[str, str]:
        """Run expect_exit, returning its output and what it printed."""
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            output = catalog_checks.expect_exit(*args, **kwargs)
        return output, printed.getvalue()

    def test_an_expected_exit_code_passes(self) -> None:
        for want, cmd in ((0, ["true"]), (1, ["false"]), (6, ["sh", "-c", "exit 6"])):
            _, printed = self.check(want, "the code", cmd)
            self.assertTrue(printed.startswith(f"ok   exit {want}: the code"), printed)
        self.assertEqual(catalog_checks.failures, 0)

    def test_another_exit_code_fails_with_no_expected_text(self) -> None:
        _, printed = self.check(0, "the code", ["sh", "-c", "exit 6"])
        self.assertTrue(printed.startswith("FAIL exit 6, expected 0: the code"), printed)
        _, printed = self.check(6, "the code", ["false"])
        self.assertTrue(printed.startswith("FAIL exit 1, expected 6: the code"), printed)
        self.assertEqual(catalog_checks.failures, 2)

    def test_the_expected_text_must_be_in_the_output_as_well(self) -> None:
        output, printed = self.check(6, "the text", ["sh", "-c", "echo held; exit 6"], contains="held")
        self.assertEqual(output, "held\n")
        self.assertTrue(printed.startswith("ok   exit 6: the text"), printed)
        _, printed = self.check(6, "the text", ["sh", "-c", "echo held; exit 6"], contains="free")
        self.assertTrue(printed.startswith("FAIL exit 6, expected 6"), printed)
        self.assertIn("     | held", printed)
        _, printed = self.check(0, "the text", ["sh", "-c", "echo held; exit 6"], contains="held")
        self.assertTrue(printed.startswith("FAIL exit 6, expected 0"), printed)
        self.assertEqual(catalog_checks.failures, 2)

    def test_a_command_past_its_time_limit_is_killed_and_fails(self) -> None:
        code, output = catalog_checks.run(["sleep", "5"], timeout=0.2)
        self.assertEqual(code, -1)
        self.assertIn("still running after 0.2 s", output)

    def test_finish_returns_1_after_a_failure_and_0_without(self) -> None:
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(catalog_checks.finish(), 0)
            catalog_checks.report(False, "a check", "line one\nline two")
            self.assertEqual(catalog_checks.finish(), 1)
        text = printed.getvalue()
        self.assertIn("all checks passed", text)
        self.assertIn("FAIL a check\n     | line one\n     | line two\n", text)
        self.assertIn("1 check(s) failed", text)


if __name__ == "__main__":
    unittest.main()
