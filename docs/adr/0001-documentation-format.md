# ADR: Documentation Format — Markdown over LaTeX

## Context

extenddb needs a standard format for all documentation: design docs, ADRs, getting-started guides, and troubleshooting references. The two candidates are Markdown and LaTeX.

## Options Considered

1. **Markdown** — lightweight, renders natively on GitHub/GitLab, editable in any text editor, supported by all Rust documentation tooling (`rustdoc`, `mdbook`).
2. **LaTeX** — typesetting system designed for academic papers and technical documents. Produces high-quality PDF output but requires a TeX distribution to build.

## Decision

Markdown for all documentation.

## Rationale

- All existing extenddb documentation is already Markdown. Switching would require migrating dozens of files.
- Markdown renders in-place on code hosting platforms — no build step, no PDF artifacts to manage.
- Rust's `rustdoc` uses Markdown natively. Keeping external docs in the same format reduces context switching.
- LaTeX adds a build dependency (TeX Live is ~4 GB) with no corresponding benefit for a software project's documentation needs.
- Contributors can edit Markdown with zero tooling beyond a text editor.

## Consequences

- All docs remain `.md` files in the `docs/` tree.
- If publication-quality PDF output is ever needed (e.g., for a formal specification), `pandoc` can convert Markdown to PDF on demand without changing the source format.

---

## License

Copyright 2026 ExtendDB contributors. Licensed under the Apache License, Version 2.0.
See [LICENSE](../../LICENSE) for the full text.

This software is provided "as is" without warranty of any kind. ExtendDB is not
affiliated with, endorsed by, or sponsored by Amazon Web Services. "DynamoDB" is a trademark
of Amazon.com, Inc.
