# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/check-crate-packages.

Each test builds a two-crate cargo workspace and its `.crate` files in a
temporary directory, then runs the script, which reads the workspace through
`cargo metadata`. The script passes the complete workspace and names each
problem a test introduces. The crates.io limits are tested by calling
`check_package` with the limits lowered.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import importlib.util
import io
import os
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parent / "check-crate-packages"
VERSION = "1.2.3"

ROOT_MANIFEST = f"""\
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.package]
version = "{VERSION}"
edition = "2021"

[workspace.dependencies]
example-a = {{ path = "crates/a", version = "={VERSION}" }}
example-b = {{ path = "crates/b", version = "={VERSION}" }}
"""

MEMBER_A = """\
[package]
name = "example-a"
version.workspace = true
edition.workspace = true

[dev-dependencies]
example-b = { path = "../b" }
"""

MEMBER_B = """\
[package]
name = "example-b"
version.workspace = true
edition.workspace = true

[dependencies]
example-a = { workspace = true }
"""


def load_script():
    """Import the script, which has no .py suffix, as a module."""
    loader = SourceFileLoader("check_crate_packages", str(SCRIPT))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def packaged_manifest(name: str, extra: str = "", version: str = VERSION, **package) -> str:
    """A manifest in the form `cargo package` writes. `package` overrides or,
    with None, removes a [package] field; `extra` is appended as is."""
    fields = {"name": name, "version": version, "description": "d", "license": "Apache-2.0",
              "repository": "https://example.com"}
    fields.update(package)
    lines = ["[package]"] + [f'{k} = "{v}"' for k, v in fields.items() if v is not None]
    return "\n".join(lines) + "\n" + textwrap.dedent(extra)


B_DEPENDENCY = f'\n[dependencies.example-a]\nversion = "={VERSION}"\n'


class CheckCratePackagesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.packages = self.root / "target" / "package"
        self.packages.mkdir(parents=True)
        self.write("Cargo.toml", ROOT_MANIFEST)
        self.member("a", MEMBER_A)
        self.member("b", MEMBER_B)
        self.write("LICENSE", "license text\n")
        self.write("NOTICE", "notice text\n")
        self.package("example-a", {"Cargo.toml": packaged_manifest("example-a")})
        self.package("example-b", {"Cargo.toml": packaged_manifest("example-b", B_DEPENDENCY)})

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def member(self, directory: str, manifest: str) -> None:
        self.write(f"crates/{directory}/Cargo.toml", manifest)
        self.write(f"crates/{directory}/src/lib.rs", "")

    def package(self, name: str, files: dict, licenses: bool = True, version: str = VERSION,
                into: Path | None = None) -> None:
        """Write <name>-<version>.crate holding `files`, plus the root LICENSE
        and NOTICE unless `licenses` is false."""
        contents = dict(files)
        if licenses:
            contents.setdefault("LICENSE", (self.root / "LICENSE").read_bytes())
            contents.setdefault("NOTICE", (self.root / "NOTICE").read_bytes())
        directory = into or self.packages
        directory.mkdir(parents=True, exist_ok=True)
        with tarfile.open(directory / f"{name}-{version}.crate", "w:gz") as tar:
            for filename, data in contents.items():
                raw = data.encode() if isinstance(data, str) else data
                info = tarfile.TarInfo(f"{name}-{version}/{filename}")
                info.size = len(raw)
                tar.addfile(info, io.BytesIO(raw))

    def run_check(self, env: dict | None = None) -> tuple[int, str]:
        environment = {k: v for k, v in os.environ.items() if k != "CARGO_TARGET_DIR"}
        environment.update(env or {})
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root)],
            capture_output=True, text=True, env=environment, check=False,
        )
        return result.returncode, result.stdout + result.stderr

    def assert_passes(self, env: dict | None = None) -> None:
        code, output = self.run_check(env)
        self.assertEqual(code, 0, output)
        self.assertIn("ok: every published workspace crate is ready for crates.io", output)

    def assert_fails_with(self, *fragments: str) -> None:
        code, output = self.run_check()
        self.assertEqual(code, 1, output)
        for fragment in fragments:
            self.assertIn(fragment, output)

    # The workspace as cargo reads it.

    def test_a_complete_workspace_passes(self) -> None:
        self.assert_passes()

    def test_a_workspace_pin_that_is_not_exact_fails(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace(f'"crates/a", version = "={VERSION}"', f'"crates/a", version = "{VERSION}"'))
        self.assert_fails_with(
            f"crates/b/Cargo.toml: normal dependency example-a requires '^{VERSION}'", f'"={VERSION}"')

    def test_a_workspace_pin_left_at_the_last_release_fails(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace(f'"crates/a", version = "={VERSION}"', '"crates/a", version = "=1.2.2"'))
        self.assert_fails_with("normal dependency example-a requires '=1.2.2'")

    def test_a_renamed_dependency_is_checked_under_its_package_name(self) -> None:
        self.member("b", MEMBER_B.replace("example-a = { workspace = true }",
                                          f'alias = {{ package = "example-a", path = "../a", version = "{VERSION}" }}'))
        self.assert_fails_with(f"normal dependency alias requires '^{VERSION}'")

    def test_a_build_dependency_needs_the_exact_pin(self) -> None:
        self.member("b", MEMBER_B + f'\n[build-dependencies]\nexample-a = {{ path = "../a", version = "{VERSION}" }}\n')
        self.assert_fails_with(f"build dependency example-a requires '^{VERSION}'")

    def test_an_internal_dev_dependency_through_the_workspace_fails(self) -> None:
        self.member("a", MEMBER_A.replace('{ path = "../b" }', "{ workspace = true }"))
        self.assert_fails_with(f"crates/a/Cargo.toml: dev-dependency example-b requires '={VERSION}'")

    def test_an_internal_dev_dependency_with_a_version_fails(self) -> None:
        self.member("a", MEMBER_A.replace('{ path = "../b" }', f'{{ path = "../b", version = "={VERSION}" }}'))
        self.assert_fails_with("dev-dependency example-b requires")

    def test_an_internal_dev_dependency_without_a_path_fails(self) -> None:
        self.member("a", MEMBER_A.replace('{ path = "../b" }', '"*"'))
        self.assert_fails_with("dev-dependency example-b requires '*'; declare it by path")

    def test_a_target_specific_dev_dependency_is_checked(self) -> None:
        self.member("a", MEMBER_A + "\n[target.'cfg(unix)'.dev-dependencies]\nexample-b = { workspace = true }\n")
        self.assert_fails_with(f"dev-dependency example-b requires '={VERSION}'")

    def test_a_missing_package_fails(self) -> None:
        (self.packages / f"example-b-{VERSION}.crate").unlink()
        self.assert_fails_with(f"example-b-{VERSION}.crate: missing")

    def test_an_unpublished_member_needs_no_package_and_no_pins(self) -> None:
        self.member("b", MEMBER_B.replace("edition.workspace = true", "edition.workspace = true\npublish = false")
                    .replace("{ workspace = true }", '{ path = "../a" }'))
        (self.packages / f"example-b-{VERSION}.crate").unlink()
        self.assert_passes()

    def test_publish_inherited_from_the_workspace_is_read(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace('edition = "2021"', 'edition = "2021"\npublish = false'))
        self.member("b", MEMBER_B.replace("edition.workspace = true", "edition.workspace = true\npublish.workspace = true"))
        (self.packages / f"example-b-{VERSION}.crate").unlink()
        self.assert_passes()

    def test_an_excluded_directory_is_not_a_member(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace('members = ["crates/*"]', 'members = ["crates/*"]\nexclude = ["crates/x"]'))
        self.write("crates/x/Cargo.toml", '[package]\nname = "example-x"\nversion = "0.1.0"\nedition = "2021"\n')
        self.write("crates/x/src/lib.rs", "")
        self.assert_passes()

    def test_a_member_with_its_own_version_fails_and_its_pins_are_checked_at_it(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace(f'"crates/a", version = "={VERSION}"', '"crates/a", version = "=2.0.0"'))
        self.member("a", MEMBER_A.replace("version.workspace = true", 'version = "2.0.0"'))
        (self.packages / f"example-a-{VERSION}.crate").unlink()
        self.package("example-a", {"Cargo.toml": packaged_manifest("example-a", version="2.0.0")}, version="2.0.0")
        self.package("example-b", {"Cargo.toml": packaged_manifest("example-b", '\n[dependencies.example-a]\nversion = "=2.0.0"\n')})
        code, output = self.run_check()
        self.assertEqual(code, 1, output)
        self.assertIn(f"error: crates/a/Cargo.toml: example-a is version 2.0.0, and the workspace version is {VERSION}.",
                      output)
        self.assertIn("1 problem(s) found", output)

    def test_a_root_manifest_without_a_workspace_version_fails(self) -> None:
        self.write("Cargo.toml", ROOT_MANIFEST.replace(f'version = "{VERSION}"\nedition', "edition"))
        for directory, manifest in (("a", MEMBER_A), ("b", MEMBER_B)):
            self.member(directory, manifest.replace("version.workspace = true", f'version = "{VERSION}"'))
        self.assert_fails_with("error: the root Cargo.toml sets no [workspace.package] version")

    def test_a_member_published_only_to_another_registry_needs_no_package(self) -> None:
        self.member("b", MEMBER_B.replace("edition.workspace = true", 'edition.workspace = true\npublish = ["my-registry"]')
                    .replace("{ workspace = true }", '{ path = "../a" }'))
        (self.packages / f"example-b-{VERSION}.crate").unlink()
        self.assert_passes()

    def test_a_member_whose_publish_list_names_crates_io_is_checked(self) -> None:
        self.member("b", MEMBER_B.replace("edition.workspace = true",
                                          'edition.workspace = true\npublish = ["my-registry", "crates-io"]'))
        (self.packages / f"example-b-{VERSION}.crate").unlink()
        self.assert_fails_with(f"example-b-{VERSION}.crate: missing")

    def test_cargo_target_dir_moves_the_packages(self) -> None:
        moved = self.root / "elsewhere"
        for crate in self.packages.iterdir():
            moved.joinpath("package").mkdir(parents=True, exist_ok=True)
            crate.rename(moved / "package" / crate.name)
        self.assert_fails_with(f"example-a-{VERSION}.crate: missing")
        self.assert_passes({"CARGO_TARGET_DIR": str(moved)})

    # The packages.

    def test_a_package_without_the_license_files_fails(self) -> None:
        self.package("example-a", {"Cargo.toml": packaged_manifest("example-a")}, licenses=False)
        self.assert_fails_with(f"example-a-{VERSION}.crate: no LICENSE", f"example-a-{VERSION}.crate: no NOTICE")

    def test_a_license_file_that_differs_from_the_root_fails(self) -> None:
        self.package("example-a", {"Cargo.toml": packaged_manifest("example-a"), "LICENSE": "other\n", "NOTICE": "other\n"})
        self.assert_fails_with(f"example-a-{VERSION}.crate: LICENSE differs from the root LICENSE",
                               f"example-a-{VERSION}.crate: NOTICE differs from the root NOTICE")

    def test_a_package_without_a_manifest_fails(self) -> None:
        self.package("example-a", {"README.md": "r\n"})
        self.assert_fails_with(f"example-a-{VERSION}.crate: no Cargo.toml")

    def test_each_required_manifest_field_is_checked(self) -> None:
        for field in ("description", "license", "repository"):
            with self.subTest(field=field):
                self.package("example-a", {"Cargo.toml": packaged_manifest("example-a", **{field: None})})
                self.assert_fails_with(f"example-a-{VERSION}.crate: the packaged manifest has no {field}")

    def test_each_packaged_dependency_table_needs_the_exact_pin(self) -> None:
        tables = {
            "dependencies": f'\n[dependencies.example-a]\nversion = "^{VERSION}"\n',
            "build-dependencies": f'\n[build-dependencies.example-a]\nversion = "^{VERSION}"\n',
            "target.cfg(unix).dependencies": f'\n[target."cfg(unix)".dependencies.example-a]\nversion = "^{VERSION}"\n',
            "dependencies (renamed)": f'\n[dependencies.alias]\npackage = "example-a"\nversion = "^{VERSION}"\n',
            "dependencies (string form)": f'\n[dependencies]\nexample-a = "^{VERSION}"\n',
        }
        for label, extra in tables.items():
            with self.subTest(table=label):
                self.package("example-b", {"Cargo.toml": packaged_manifest("example-b", extra)})
                key = "alias" if "renamed" in label else "example-a"
                table = label.split(" ")[0]
                self.assert_fails_with(f"example-b-{VERSION}.crate: [{table}] {key} requires '^{VERSION}'")

    def test_a_dev_dependency_cargo_kept_in_the_package_fails(self) -> None:
        tables = {
            "dev-dependencies": '\n[dev-dependencies.example-a]\nversion = "*"\n',
            "target.cfg(unix).dev-dependencies": f'\n[target."cfg(unix)".dev-dependencies.example-a]\nversion = "={VERSION}"\n',
        }
        for label, extra in tables.items():
            with self.subTest(table=label):
                self.package("example-b", {"Cargo.toml": packaged_manifest("example-b", B_DEPENDENCY + extra)})
                self.assert_fails_with(
                    f"example-b-{VERSION}.crate: [{label}] example-a: cargo kept this dev-dependency")

    def test_a_blank_description_fails(self) -> None:
        self.package("example-a", {"Cargo.toml": packaged_manifest("example-a", description="   ")})
        self.assert_fails_with(f"example-a-{VERSION}.crate: the packaged manifest has no description")

    def test_a_failing_cargo_metadata_fails(self) -> None:
        self.write("Cargo.toml", "[workspace\n")
        self.assert_fails_with("error: `cargo metadata` failed in")

    def test_a_missing_root_fails_with_an_error_line(self) -> None:
        self.root = self.root / "does-not-exist"
        code, output = self.run_check()
        self.assertEqual(code, 1, output)
        self.assertIn("error: cannot run `cargo metadata`", output)
        self.assertNotIn("Traceback", output)


class LimitsTest(unittest.TestCase):
    """check_package against crates.io's limits, lowered so a fixture can
    cross each one."""

    def setUp(self) -> None:
        self.module = load_script()
        self.tmp = tempfile.TemporaryDirectory()
        self.crate = Path(self.tmp.name) / f"example-a-{VERSION}.crate"
        self.root_files = {"LICENSE": b"l\n", "NOTICE": b"n\n"}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_crate(self, manifest: str, extra_files: int = 0) -> None:
        files = {"Cargo.toml": manifest.encode(), **self.root_files}
        files.update({f"f{i}.rs": b"x" * 100 for i in range(extra_files)})
        with tarfile.open(self.crate, "w:gz") as tar:
            for filename, raw in files.items():
                info = tarfile.TarInfo(f"example-a-{VERSION}/{filename}")
                info.size = len(raw)
                tar.addfile(info, io.BytesIO(raw))

    def check(self, **limits) -> list[str]:
        """check_package on the crate written last, with `limits` patched in."""
        def run() -> list[str]:
            return self.module.check_package(self.crate, "example-a", VERSION, {"example-a": VERSION}, self.root_files)

        if not limits:
            return run()
        with mock.patch.multiple(self.module, **limits):
            return run()

    def problems(self, manifest: str, extra_files: int = 0, **limits) -> list[str]:
        self.write_crate(manifest, extra_files)
        return self.check(**limits)

    def test_a_package_exactly_at_each_limit_passes_and_one_over_fails(self) -> None:
        extra = ('\n[dependencies.x]\nversion = "1"\n\n[dev-dependencies.y]\nversion = "1"\n'
                 '\n[features]\none = ["two", "dep:x"]\ntwo = []\n')
        manifest = packaged_manifest("example-a", extra)
        self.write_crate(manifest, 3)
        with tarfile.open(self.crate, "r:gz") as tar:
            entries = tar.getmembers()
        measured = {
            "UPLOAD_SIZE": self.crate.stat().st_size,
            "TARBALL_ENTRIES": len(entries),
            "UNPACK_SIZE": sum(entry.size for entry in entries),
            "METADATA_FILE_SIZE": len(manifest.encode()),
            "DEPENDENCIES": 2,
            # Two features, and feature `one` enables two entries.
            "FEATURES": 2,
        }
        for limit, value in measured.items():
            with self.subTest(limit=limit):
                self.assertEqual(self.check(**{limit: value}), [])
                self.assertNotEqual(self.check(**{limit: value - 1}), [])

    def test_the_crates_io_field_rules_are_checked(self) -> None:
        at_the_limits = {"description": "d" * 1000, "keywords": ["a", "b+c", "d_e", "f-g", "h" * 20],
                         "categories": ["a", "b", "c", "d", "e"]}
        self.assertEqual(self.module.check_metadata("x.crate", at_the_limits), [])
        cases = {
            "the description is 1001 bytes": {"description": "d" * 1001},
            "6 keywords, over": {"keywords": list("abcdef")},
            "is over the crates.io limit of 20 bytes": {"keywords": ["k" * 21]},
            "'-k' is not a valid crates.io keyword": {"keywords": ["-k"]},
            "'a b' is not a valid crates.io keyword": {"keywords": ["a b"]},
            "'\u00e9t\u00e9' is not a valid crates.io keyword": {"keywords": ["\u00e9t\u00e9"]},
            "'caf\u00e9' is not a valid crates.io keyword": {"keywords": ["caf\u00e9"]},
            "6 categories, over": {"categories": list("abcdef")},
        }
        for fragment, package in cases.items():
            with self.subTest(rule=fragment):
                problems = self.module.check_metadata("x.crate", package)
                self.assertTrue(any(fragment in p for p in problems), problems)

    def test_the_field_rules_run_on_each_package(self) -> None:
        manifest = packaged_manifest("example-a", 'keywords = ["a", "b", "c", "d", "e", "f"]\n')
        self.assertEqual(self.problems(manifest), [f"example-a-{VERSION}.crate: 6 keywords, over the crates.io limit of 5"])

    def test_a_package_inside_every_limit_passes(self) -> None:
        self.assertEqual(self.problems(packaged_manifest("example-a")), [])

    def test_each_limit_is_enforced(self) -> None:
        features = '\n[features]\none = ["two", "three"]\ntwo = []\nthree = []\n'
        dependencies = '\n[dependencies.x]\nversion = "1"\n\n[dev-dependencies.y]\nversion = "1"\n'
        cases = {
            "upload limit": ({"UPLOAD_SIZE": 10}, "", 0),
            "entries, over the crates.io limit of 2": ({"TARBALL_ENTRIES": 2}, "", 0),
            "bytes unpacked": ({"UNPACK_SIZE": 500}, "", 10),
            "Cargo.toml is": ({"METADATA_FILE_SIZE": 20}, "", 0),
            "2 dependencies, over": ({"DEPENDENCIES": 1}, dependencies, 0),
            "3 features, over": ({"FEATURES": 2}, features, 0),
        }
        for fragment, (limits, extra, files) in cases.items():
            with self.subTest(limit=fragment):
                problems = self.problems(packaged_manifest("example-a", extra), files, **limits)
                self.assertTrue(any(fragment in p for p in problems), problems)

    def test_the_entries_one_feature_enables_are_counted(self) -> None:
        features = '\n[features]\none = ["two", "dep:x", "x/serde"]\ntwo = []\n'
        problems = self.problems(packaged_manifest("example-a", features), FEATURES=2)
        self.assertEqual(
            problems, [f"example-a-{VERSION}.crate: feature one enables 3 entries, over the crates.io limit of 2"])


if __name__ == "__main__":
    unittest.main()
