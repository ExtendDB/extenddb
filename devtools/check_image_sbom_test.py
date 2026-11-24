# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests for devtools/check-image-sbom."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "check-image-sbom"
IMAGE_ID = "sha256:" + "ab" * 32


def sbom(components, image_ids=(IMAGE_ID,), bom_format="CycloneDX"):
    return {
        "bomFormat": bom_format,
        "metadata": {
            "component": {
                "properties": [{"name": "aquasecurity:trivy:ImageID", "value": i} for i in image_ids]
            }
        },
        "components": components,
    }


DEB = {"purl": "pkg:deb/debian/libc6@2.36"}
CRATE = {"purl": "pkg:cargo/tokio@1.0.0"}


class CheckImageSbomTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)

    def tearDown(self):
        self.dir.cleanup()

    def run_check(self, bom, dockerfile_text, env=None):
        sbom_path = self.root / "sbom.cdx.json"
        sbom_path.write_text(json.dumps(bom) if isinstance(bom, dict) else bom)
        dockerfile = self.root / "Dockerfile"
        dockerfile.write_text(dockerfile_text)
        full_env = {k: v for k, v in os.environ.items() if k != "GITHUB_ACTIONS"}
        full_env.update(env or {})
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(sbom_path), str(dockerfile)],
            capture_output=True, text=True, env=full_env,
        )

    def test_an_auditable_build_with_crates_passes(self):
        r = self.run_check(sbom([DEB, CRATE]), "RUN cargo auditable build --release\n")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("2 components, 1 of them crates; image " + IMAGE_ID, r.stdout)

    def test_an_auditable_build_without_crates_fails(self):
        r = self.run_check(sbom([DEB]), "RUN cargo auditable build --release\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("lists no crates, but", r.stdout)
        self.assertIn("builds with cargo-auditable", r.stdout)

    def test_a_plain_build_without_crates_warns_and_passes(self):
        r = self.run_check(sbom([DEB]), "RUN cargo build --release\n")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("warning: ", r.stdout)
        self.assertIn("does not build with cargo-auditable", r.stdout)

    def test_a_second_image_id_fails(self):
        r = self.run_check(sbom([DEB, CRATE], image_ids=(IMAGE_ID, IMAGE_ID)), "RUN cargo auditable build\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("is not a CycloneDX SBOM of one image", r.stdout)

    def test_a_malformed_image_id_fails(self):
        r = self.run_check(sbom([DEB], image_ids=("sha256:short",)), "RUN cargo build\n")
        self.assertEqual(r.returncode, 1)

    def test_no_components_fails(self):
        r = self.run_check(sbom([]), "RUN cargo build\n")
        self.assertEqual(r.returncode, 1)

    def test_another_format_fails(self):
        r = self.run_check(sbom([DEB], bom_format="SPDX"), "RUN cargo build\n")
        self.assertEqual(r.returncode, 1)

    def test_unreadable_json_fails(self):
        r = self.run_check("{", "RUN cargo build\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("is not readable JSON", r.stdout)

    def test_messages_are_annotations_under_actions(self):
        r = self.run_check(sbom([DEB]), "RUN cargo build\n", env={"GITHUB_ACTIONS": "true"})
        self.assertEqual(r.returncode, 0)
        self.assertIn("::warning::", r.stdout)
        r = self.run_check(sbom([DEB]), "RUN cargo auditable build\n", env={"GITHUB_ACTIONS": "true"})
        self.assertIn("::error::", r.stdout)

    def test_usage_error(self):
        r = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertIn("check-image-sbom SBOM.cdx.json DOCKERFILE", r.stderr)


if __name__ == "__main__":
    unittest.main()
