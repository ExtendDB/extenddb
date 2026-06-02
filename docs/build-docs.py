#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Build HTML and PDF documentation from Markdown sources.

Pipeline: Markdown → HTML fragment (Python markdown) → PDF (weasyprint).

Usage:
    python3 docs/build-docs.py              # Build all documents
    python3 docs/build-docs.py --list       # List available documents
    python3 docs/build-docs.py --doc 4      # Build only document #4

Prerequisites:
    pip install -r requirements.txt

Version stamping:
    Git commit hash and catalog version are injected automatically.
    Placeholders in Markdown sources:
        {{GIT_HASH}}         → short git commit hash
        {{CATALOG_VERSION}}  → from `extenddb version` or cargo metadata
        {{BUILD_DATE}}       → ISO 8601 UTC date
        {{EXTENDDB_VERSION}}     → package version from Cargo.toml

Build order:
    This script must run BEFORE `cargo build`. The server loads the rendered
    HTML and PDF files from the `docs_dir` configured in `extenddb.toml`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
MANUALS_DIR = ROOT / "docs" / "manuals"
DOCS_DIR = ROOT / "docs"
OUTPUT_DIR = ROOT / "pdfs"
RENDERED_DIR = ROOT / "docs" / "rendered"

# ---------------------------------------------------------------------------
# Document catalog
# ---------------------------------------------------------------------------
# (number, slug, title, category, source_path_relative_to_ROOT)
# Categories: "getting-started", "usage", "architecture", "reference"
DOCUMENTS = [
    (1, "architecture-guide", "Architecture Guide", "architecture",
     "docs/manuals/01-architecture-guide.md"),
    (2, "design-guide", "Design Guide", "architecture",
     "docs/manuals/02-design-guide.md"),
    (3, "usage-guide", "Usage Guide", "usage",
     "docs/manuals/03-usage-guide.md"),
    (4, "quickstart-setup-guide", "Quick Start & Setup Guide", "getting-started",
     "docs/manuals/04-quickstart-setup-guide.md"),
    (5, "admin-guide", "Admin Guide", "usage",
     "docs/manuals/05-admin-guide.md"),
    (6, "developer-test-guide", "Developer & Test Guide", "reference",
     "docs/manuals/06-developer-test-guide.md"),
    (7, "upgrade-manual", "Upgrade Manual", "usage",
     "docs/manuals/07-upgrade-manual.md"),
    (8, "install-linux", "Linux Installation Guide", "getting-started",
     "docs/manuals/08-install-linux.md"),
    (9, "install-macos", "macOS Installation Guide", "getting-started",
     "docs/manuals/09-install-macos.md"),
    (10, "security-model", "Security Model", "architecture",
     "docs/manuals/10-security-model.md"),
    (11, "deployment-guide", "Deployment Guide", "architecture",
     "docs/manuals/11-deployment-guide.md"),
    (12, "extending-extenddb-storage", "Extending extenddb Storage", "architecture",
     "docs/manuals/12-extending-extenddb-storage.md"),
    (13, "getting-started", "Getting Started", "getting-started",
     "docs/getting-started.md"),
    (14, "troubleshooting", "Troubleshooting", "usage",
     "docs/troubleshooting.md"),
    (15, "differences-from-dynamodb", "Differences from DynamoDB", "reference",
     "docs/differences-from-dynamodb.md"),
    (16, "dynamodb-limits", "DynamoDB Limits", "reference",
     "docs/dynamodb-limits.md"),
    (17, "local-postgres-setup", "Local PostgreSQL Setup", "getting-started",
     "docs/local-postgres-setup.md"),
    (18, "event-ticketing-demo", "Event Ticketing Platform Demo", "getting-started",
     "docs/manuals/13-event-ticketing-demo.md"),
]


def get_git_hash() -> str:
    """Return short git commit hash or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True, cwd=ROOT,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def get_extenddb_version() -> str:
    """Extract version from workspace Cargo.toml."""
    cargo_toml = ROOT / "Cargo.toml"
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            # Fallback: regex against [workspace.package] section.
            in_section = False
            for line in cargo_toml.read_text().splitlines():
                if line.strip() == "[workspace.package]":
                    in_section = True
                elif line.startswith("["):
                    in_section = False
                elif in_section:
                    m = re.match(r'^version\s*=\s*"(.+)"', line)
                    if m:
                        return m.group(1)
            return "0.0.0"
    with open(cargo_toml, "rb") as f:
        data = tomllib.load(f)
    return data.get("workspace", {}).get("package", {}).get("version", "0.0.0")


def get_catalog_version() -> str:
    """Extract CATALOG_VERSION from the default TiDB backend."""
    lib_rs = ROOT / "crates" / "storage-tidb" / "src" / "lib.rs"
    text = lib_rs.read_text()
    m = re.search(r"CatalogVersion::new\((\d+),\s*(\d+),\s*(\d+)\)", text)
    if m:
        return f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    return "unknown"


def get_build_date() -> str:
    """Return current UTC date in ISO 8601."""
    try:
        result = subprocess.run(
            ["date", "-u", "+%Y-%m-%d"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def substitute_placeholders(text: str, variables: dict[str, str]) -> str:
    """Replace {{KEY}} placeholders with values."""
    for key, value in variables.items():
        text = text.replace("{{" + key + "}}", value)
    # AI-3: Replace relative NOTICE.md links with inline text. The relative
    # link works in the repo filesystem but breaks in rendered PDFs.
    text = re.sub(
        r'See \[NOTICE\]\(\.\./NOTICE\.md\) for important disclaimers\.',
        'See the NOTICE file in the project root for important disclaimers.',
        text,
    )
    text = re.sub(
        r'See \[NOTICE\]\(NOTICE\.md\) for important disclaimers\.',
        'See the NOTICE file in the project root for important disclaimers.',
        text,
    )
    return text


CSS = """
@page {{
    size: letter;
    margin: 2.5cm 2cm;
    @bottom-left {{
        content: "{doc_title}";
        font-size: 8pt;
        color: #999;
    }}
    @bottom-right {{
        content: "Page " counter(page) " of " counter(pages);
        font-size: 9pt;
        color: #666;
    }}
}}
body {{
    font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 11pt;
    line-height: 1.6;
    color: #1a1a1a;
}}
h1 {{ font-size: 22pt; margin-top: 1.5em; page-break-after: avoid; }}
h2 {{ font-size: 16pt; margin-top: 1.2em; page-break-after: avoid; }}
h3 {{ font-size: 13pt; margin-top: 1em; page-break-after: avoid; }}
code {{
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
    font-size: 9.5pt;
    background: #f5f5f5;
    padding: 0.15em 0.3em;
    border-radius: 3px;
}}
pre {{
    background: #f5f5f5;
    padding: 0.8em 1em;
    border-radius: 4px;
    overflow-x: auto;
    font-size: 9pt;
    line-height: 1.4;
    page-break-inside: avoid;
}}
pre code {{ background: none; padding: 0; }}
table {{
    width: 100%;
    border-collapse: collapse;
    margin: 1em 0;
    font-size: 10pt;
    page-break-inside: avoid;
}}
th, td {{
    border: 1px solid #ddd;
    padding: 0.4em 0.6em;
    text-align: left;
}}
th {{ background: #f0f0f0; font-weight: 600; }}
blockquote {{
    border-left: 3px solid #2563eb;
    margin: 1em 0;
    padding: 0.5em 1em;
    background: #f8f9fa;
    font-size: 10pt;
}}
.cover {{
    text-align: center;
    padding-top: 30%;
}}
.cover h1 {{ font-size: 28pt; }}
.cover .meta {{ color: #666; font-size: 11pt; margin-top: 2em; }}
.license-page {{
    font-size: 10pt;
    color: #444;
    padding-top: 2em;
}}
.license-page h2 {{ font-size: 14pt; }}
"""

LICENSE_HTML = """<div class="license-page">
<h2>License &amp; Disclaimer</h2>
<p>Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See the LICENSE file in the project root for the full text.</p>
<p>This software is provided &ldquo;as is&rdquo; without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. &ldquo;DynamoDB&rdquo; is a trademark
of Amazon.com, Inc.</p>
</div>
<div style="page-break-after: always"></div>
"""


def build_html_fragment(md_path: Path, html_path: Path, variables: dict[str, str]) -> bool:
    """Convert a Markdown file to an HTML fragment (no <html>/<body> wrapper).

    Returns True on success.
    """
    try:
        import markdown as md_lib
    except ImportError as e:
        print(f"ERROR: Missing dependency: {e}", file=sys.stderr)
        return False

    raw = md_path.read_text()
    raw = substitute_placeholders(raw, variables)

    body_html = md_lib.markdown(
        raw,
        extensions=["tables", "fenced_code", "toc"],
    )

    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(body_html)
    return True


def build_pdf(md_path: Path, pdf_path: Path, variables: dict[str, str], title: str) -> bool:
    """Convert a Markdown file to PDF. Returns True on success."""
    try:
        import markdown as md_lib
        from weasyprint import HTML
    except ImportError as e:
        print(f"ERROR: Missing dependency: {e}", file=sys.stderr)
        print("Install with: pip install markdown weasyprint", file=sys.stderr)
        return False

    raw = md_path.read_text()
    raw = substitute_placeholders(raw, variables)

    body_html = md_lib.markdown(
        raw,
        extensions=["tables", "fenced_code", "toc"],
    )

    # Build cover page
    cover = f"""<div class="cover">
<h1>{title}</h1>
<p class="meta">
extenddb {variables['EXTENDDB_VERSION']} · catalog {variables['CATALOG_VERSION']}<br>
commit {variables['GIT_HASH']} · {variables['BUILD_DATE']}
</p>
</div>
<div style="page-break-after: always"></div>
"""

    # Per-document CSS with title in footer
    doc_css = CSS.format(doc_title=title)

    full_html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>{doc_css}</style>
</head><body>
{cover}
{LICENSE_HTML}
{body_html}
</body></html>"""

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=full_html).write_pdf(str(pdf_path))
    return True


def build_manifest(documents: list, variables: dict[str, str]) -> None:
    """Write a JSON manifest of all documents for the Rust embed code."""
    manifest = []
    for num, slug, title, category, _ in documents:
        manifest.append({
            "slug": slug,
            "title": title,
            "category": category,
            "number": num,
        })
    manifest_path = RENDERED_DIR / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build extenddb documentation (HTML + PDF)")
    parser.add_argument("--list", action="store_true", help="List available documents")
    parser.add_argument("--doc", type=int, help="Build only document N")
    args = parser.parse_args()

    if args.list:
        for num, slug, title, category, src in DOCUMENTS:
            md = ROOT / src
            exists = "✓" if md.exists() else "✗"
            print(f"  {exists} {num}. [{category}] {title} ({md.name})")
        return 0

    variables = {
        "GIT_HASH": get_git_hash(),
        "CATALOG_VERSION": get_catalog_version(),
        "BUILD_DATE": get_build_date(),
        "EXTENDDB_VERSION": get_extenddb_version(),
    }
    print(f"Version: {variables['EXTENDDB_VERSION']}, "
          f"Catalog: {variables['CATALOG_VERSION']}, "
          f"Commit: {variables['GIT_HASH']}, "
          f"Date: {variables['BUILD_DATE']}")

    targets = DOCUMENTS
    if args.doc:
        targets = [(n, s, t, c, p) for n, s, t, c, p in DOCUMENTS if n == args.doc]
        if not targets:
            print(f"ERROR: No document #{args.doc}", file=sys.stderr)
            return 1

    ok_html = 0
    ok_pdf = 0
    fail = 0
    for num, slug, title, category, src_rel in targets:
        md_path = ROOT / src_rel
        html_path = RENDERED_DIR / f"{slug}.html"
        pdf_path_rendered = RENDERED_DIR / f"{slug}.pdf"
        pdf_path_legacy = OUTPUT_DIR / f"extenddb-{num:02d}-{slug}.pdf"

        if not md_path.exists():
            print(f"  SKIP {num}. {title} — source not found: {md_path}")
            fail += 1
            continue

        # Build HTML fragment
        print(f"  HTML {num}. {title}...", end=" ", flush=True)
        if build_html_fragment(md_path, html_path, variables):
            size_kb = html_path.stat().st_size / 1024
            print(f"OK ({size_kb:.0f} KB)")
            ok_html += 1
        else:
            print("FAILED")
            fail += 1
            continue

        # Build PDF
        print(f"  PDF  {num}. {title}...", end=" ", flush=True)
        if build_pdf(md_path, pdf_path_rendered, variables, title):
            size_kb = pdf_path_rendered.stat().st_size / 1024
            print(f"OK ({size_kb:.0f} KB)")
            ok_pdf += 1
            # Also write to legacy pdfs/ location for backward compat
            if num <= 12:
                pdf_path_legacy.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(pdf_path_rendered, pdf_path_legacy)
        else:
            print("FAILED")
            fail += 1

    # Write manifest
    build_manifest(targets if not args.doc else DOCUMENTS, variables)
    print(f"\nDone: {ok_html} HTML, {ok_pdf} PDF built, {fail} skipped/failed.")
    print(f"Output: {RENDERED_DIR}/")
    return 1 if fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
