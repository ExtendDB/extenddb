# ExtendDB documentation site

This directory holds the configuration for the documentation site built from
`../docs` with [Docusaurus](https://docusaurus.io/). The Markdown files in
`docs/` are the single source; nothing here duplicates them.

## Local preview

```bash
cd website
npm ci
npm start          # live-reloading dev server on http://localhost:3000
npm run build      # strict production build into website/build
npm run serve      # serve the production build
```

A broken link, a missing anchor or a Markdown parse error fails `npm run build`.
The same build runs as a check on every pull request that touches `docs/`,
`website/` or `Cargo.toml` (`.github/workflows/docs-site.yml`); a push to
`main` publishes the result to GitHub Pages.

## Structure

- `docusaurus.config.ts`: site settings, navbar, footer, search, theme.
- `sidebars.ts`: the navigation. Document ids are file paths under `docs/`
  without the extension; a two digit `NN-` prefix is stripped, four digit ADR
  and RFC numbers are kept.
- `src/pages/index.tsx`: the landing page.
- `src/css/custom.css`: brand tokens shared with extenddb.org.
- `static/`: favicon, logo, social image.

Files kept out of the site are listed under `exclude` in
`docusaurus.config.ts` (project ledgers, templates, build inputs).

## Hosting

The repository variable `DOCS_CUSTOM_DOMAIN` selects the base path. Unset, the
site builds for `https://extenddb.github.io/extenddb/`; set to a domain, it
builds for the root of that domain and writes a `CNAME` file.
