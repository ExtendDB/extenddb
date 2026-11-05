// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, resolve} from 'node:path';
import remarkStripBoilerplate from './src/remark/stripBoilerplate';
import {darkTheme, lightTheme} from './src/prism/themes';

// The package version from the workspace Cargo.toml, so the site never
// carries a version string of its own.
const here = dirname(fileURLToPath(import.meta.url));
const cargoToml = readFileSync(resolve(here, '..', 'Cargo.toml'), 'utf8');
const versionMatch = cargoToml.match(/^\[workspace\.package\][\s\S]*?^version\s*=\s*"([^"]+)"/m);
const extenddbVersion = versionMatch ? versionMatch[1] : 'unknown';

// GitHub Pages serves the project site at /extenddb/ until the custom domain
// is attached; the workflow sets DOCS_BASE_URL and DOCS_URL for each case.
const url = process.env.DOCS_URL ?? 'https://docs.extenddb.org';
const baseUrl = process.env.DOCS_BASE_URL ?? '/';

const config: Config = {
  title: 'ExtendDB',
  tagline: 'The DynamoDB API, everywhere you run code.',
  favicon: 'img/favicon.svg',
  url,
  baseUrl,
  trailingSlash: true,

  organizationName: 'ExtendDB',
  projectName: 'extenddb',

  // A dead link or a missing anchor fails the build.
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  onDuplicateRoutes: 'throw',

  markdown: {
    // `.md` files are parsed as CommonMark, so the existing documentation
    // builds without MDX escaping; `.mdx` files get MDX.
    format: 'detect',
    mermaid: false,
    hooks: {
      onBrokenMarkdownLinks: process.env.DOCS_LINKS_WARN ? 'warn' : 'throw',
    },
  },

  customFields: {
    extenddbVersion,
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: ({docPath}) => `https://github.com/ExtendDB/extenddb/edit/main/docs/${docPath}`,
          beforeDefaultRemarkPlugins: [remarkStripBoilerplate],
          showLastUpdateTime: false,
          // Strip a two digit "NN-" ordering prefix from ids and URLs
          // (manuals, design docs); keep four digit ADR and RFC numbers.
          numberPrefixParser: (filename: string) => {
            const m = filename.match(/^(\d{2})-(.+)$/);
            return m
              ? {filename: m[2], numberPrefix: Number.parseInt(m[1], 10)}
              : {filename, numberPrefix: undefined};
          },
          // Project-internal ledgers, templates and build inputs stay in the
          // repository and out of the site.
          exclude: [
            '**/backlog.md',
            '**/todo-index.md',
            '**/technical-debt.md',
            '**/0000-template.md',
            '**/NOTICE.md',
            // Directory readme that only lists files; the site generates the index.
            '**/design/README.md',
            // Draft under review (version 0.5); published when accepted.
            '**/design/11-high-availability.md',
            '**/demo/**',
            '**/rendered/**',
            '**/build-docs.py',
          ],
        },
        blog: false,
        pages: {
          path: 'src/pages',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        indexBlog: false,
        indexPages: true,
        docsRouteBasePath: '/',
        highlightSearchTermsOnTargetPage: true,
        searchResultLimits: 10,
        searchBarShortcutHint: true,
        // The Architecture section (the two guides, component designs,
        // decision records and RFCs) is browsed from its sidebar; keeping it
        // out of the index means a search for a feature name returns the
        // user-facing page first. The directory index pages have no
        // trailing path segment, so the patterns match a bare name too.
        ignoreFiles: [/^(design|adr|rfcs)(\/|$)/, /^manuals\/(0[12]-)?(architecture|design)-guide/],
      },
    ],
  ],

  themeConfig: {
    image: 'img/og-image.png',
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: true,
      },
    },
    navbar: {
      title: 'ExtendDB',
      logo: {
        alt: '',
        src: 'img/logo.svg',
        href: '/',
      },
      items: [
        {type: 'docSidebar', sidebarId: 'gettingStarted', label: 'Getting started', position: 'left'},
        {type: 'docSidebar', sidebarId: 'guides', label: 'Guides', position: 'left'},
        {type: 'docSidebar', sidebarId: 'reference', label: 'Reference', position: 'left'},
        {type: 'docSidebar', sidebarId: 'contributing', label: 'Contributing', position: 'left'},
        {type: 'docSidebar', sidebarId: 'architecture', label: 'Architecture', position: 'left'},
        {
          href: 'https://extenddb.org/',
          label: 'extenddb.org',
          position: 'right',
        },
        {
          href: 'https://github.com/ExtendDB/extenddb',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'ExtendDB on GitHub',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Getting started', to: '/start/'},
            {label: 'Differences from DynamoDB', to: '/differences-from-dynamodb/'},
            {label: 'Troubleshooting', to: '/troubleshooting/'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/ExtendDB/extenddb'},
            {label: 'Releases', href: 'https://github.com/ExtendDB/extenddb/releases'},
            {label: 'Issues', href: 'https://github.com/ExtendDB/extenddb/issues'},
            {label: 'Security policy', href: 'https://github.com/ExtendDB/extenddb/blob/main/SECURITY.md'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'extenddb.org', href: 'https://extenddb.org/'},
            {label: 'Playground', href: 'https://extenddb.org/playground/'},
            {label: 'Docker Hub', href: 'https://hub.docker.com/u/extenddb'},
            {label: 'npm: @extenddb/dev', href: 'https://www.npmjs.com/package/@extenddb/dev'},
          ],
        },
      ],
      copyright:
        `Copyright ${new Date().getUTCFullYear()} ExtendDB contributors. Licensed under the Apache License, Version 2.0. ` +
        'ExtendDB is an independent open source project managed by Amazon Web Services. It is not Amazon DynamoDB and contains no DynamoDB source code. ' +
        '"DynamoDB" is a trademark of Amazon.com, Inc.',
    },
    prism: {
      theme: lightTheme,
      darkTheme: darkTheme,
      additionalLanguages: ['bash', 'toml', 'rust', 'python', 'json', 'sql', 'yaml', 'diff'],
    },
    tableOfContents: {
      minHeadingLevel: 2,
      maxHeadingLevel: 3,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
