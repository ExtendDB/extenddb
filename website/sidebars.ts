// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';
import {readdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, resolve} from 'node:path';

// Document ids are the file paths under docs/ without the extension. A two
// digit "NN-" prefix is stripped from the id and the URL (see
// numberPrefixParser in docusaurus.config.ts); four digit ADR and RFC numbers
// are kept because they are how those records are cited.

const repo = 'https://github.com/ExtendDB/extenddb';
const docsDir = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'docs');

// Every ADR in docs/adr, in file order, except the index and the template.
const adrItems = readdirSync(resolve(docsDir, 'adr'))
  .filter((f) => /^\d{4}-.+\.md$/.test(f) && !f.startsWith('0000-'))
  .sort()
  .map((f) => ({type: 'doc' as const, id: `adr/${f.replace(/\.md$/, '')}`}));

const sidebars: SidebarsConfig = {
  gettingStarted: [
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      link: {type: 'doc', id: 'start'},
      items: [
        {
          type: 'category',
          label: 'Install',
          collapsed: false,
          link: {
            type: 'generated-index',
            title: 'Install ExtendDB',
            slug: '/start/install',
            description:
              'Two container images for a server in one command, or a build from source on Linux, macOS or any platform with a Rust toolchain.',
          },
          items: [
            {type: 'doc', id: 'dev-image', label: 'Development image'},
            {type: 'doc', id: 'local-dev-docker-compose', label: 'Docker Compose with PostgreSQL'},
            {type: 'doc', id: 'manuals/install-linux', label: 'Linux'},
            {type: 'doc', id: 'manuals/install-macos', label: 'macOS'},
            {type: 'doc', id: 'manuals/quickstart-setup-guide', label: 'Quick start from source'},
            {type: 'doc', id: 'getting-started', label: 'Full walkthrough from source'},
          ],
        },
      ],
    },
  ],

  guides: [
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Guides',
        slug: '/guides',
        description: 'Using, administering, deploying, securing and upgrading an ExtendDB server.',
      },
      items: [
        {type: 'doc', id: 'manuals/usage-guide', label: 'Using ExtendDB'},
        {type: 'doc', id: 'manuals/admin-guide', label: 'Administration'},
        {type: 'doc', id: 'manuals/deployment-guide', label: 'Deployment'},
        {type: 'doc', id: 'manuals/security-model', label: 'Security model'},
        {type: 'doc', id: 'manuals/upgrade-manual', label: 'Upgrading'},
        {type: 'doc', id: 'storage-backends', label: 'Storage backends'},
        {type: 'doc', id: 'manuals/event-ticketing-demo', label: 'Event ticketing demo'},
      ],
    },
  ],

  reference: [
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Reference',
        slug: '/reference',
        description: 'Behaviour that differs from the DynamoDB service, limits, configuration and troubleshooting.',
      },
      items: [
        {type: 'doc', id: 'differences-from-dynamodb', label: 'Differences from DynamoDB'},
        {type: 'doc', id: 'dynamodb-limits', label: 'Limits'},
        {
          type: 'link',
          label: 'Configuration',
          href: '/manuals/admin-guide/#configuration-reference',
          description: 'Every key in extenddb.toml, its default and what it controls (in the administration guide).',
        },
        {type: 'doc', id: 'troubleshooting', label: 'Troubleshooting'},
      ],
    },
  ],

  contributing: [
    {
      type: 'category',
      label: 'Contributing',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Contributing',
        slug: '/contributing',
        description: 'Building, testing and extending ExtendDB.',
      },
      items: [
        {
          type: 'link',
          label: 'How to contribute',
          href: `${repo}/blob/main/CONTRIBUTING.md`,
          description: 'Filing issues, the pull request process and the review rules, on GitHub.',
        },
        {type: 'doc', id: 'manuals/developer-test-guide', label: 'Developer and test guide'},
        {type: 'ref', id: 'design/testing', label: 'Test strategy'},
        {
          type: 'category',
          label: 'Local database setup',
          link: {
            type: 'generated-index',
            title: 'Local database setup',
            slug: '/contributing/local-databases',
            description: 'Notes for running a PostgreSQL or MongoDB instance on a development machine for building and testing ExtendDB.',
          },
          items: [
            {type: 'doc', id: 'local-postgres-setup', label: 'Local PostgreSQL setup (development)'},
            {type: 'doc', id: 'local-mongodb-setup', label: 'Local MongoDB setup (development)'},
          ],
        },
        {
          type: 'category',
          label: 'Adding a storage backend',
          link: {
            type: 'generated-index',
            title: 'Adding a storage backend',
            slug: '/contributing/backends',
            description: 'The storage trait, the plugin policy and the acceptance criteria a new backend meets before it ships.',
          },
          items: [
            {type: 'doc', id: 'manuals/extending-extenddb-storage', label: 'Extending ExtendDB Storage'},
            {type: 'ref', id: 'rfcs/0002-backend-plugin-policy', label: 'Backend plugin policy'},
            {type: 'ref', id: 'rfcs/0003-backend-acceptance-criteria', label: 'Backend acceptance criteria'},
          ],
        },
        {
          type: 'link',
          label: 'Security policy',
          href: `${repo}/blob/main/SECURITY.md`,
          description: 'How to report a vulnerability and what to expect, on GitHub.',
        },
        {
          type: 'link',
          label: 'Code of conduct',
          href: `${repo}/blob/main/CODE_OF_CONDUCT.md`,
          description: 'The standards of behaviour for everyone taking part in the project, on GitHub.',
        },
      ],
    },
  ],

  architecture: [
    {
      type: 'category',
      label: 'Architecture',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Architecture',
        slug: '/architecture',
        description: 'How ExtendDB is built: the architecture and design guides, component designs, decision records and RFCs.',
      },
      items: [
        {type: 'doc', id: 'manuals/architecture-guide', label: 'Architecture guide'},
        {type: 'doc', id: 'manuals/design-guide', label: 'Design guide'},
        {
          type: 'category',
          label: 'Component designs',
          link: {
            type: 'generated-index',
            title: 'Component designs',
            slug: '/design',
            description: 'The requirements and the design of each component: core, storage, authentication, server, streams, configuration and testing.',
          },
          items: [
            {type: 'doc', id: 'design/requirements', label: 'Requirements'},
            {type: 'doc', id: 'design/high-level-design', label: 'High-level design'},
            {type: 'doc', id: 'design/component-core', label: 'Core'},
            {type: 'doc', id: 'design/component-storage', label: 'Storage'},
            {type: 'doc', id: 'design/storage-mongodb', label: 'Storage: MongoDB'},
            {type: 'doc', id: 'design/component-auth', label: 'Authentication and authorization'},
            {type: 'doc', id: 'design/component-auth-v3', label: 'Authentication v3'},
            {type: 'doc', id: 'design/auth-authz-cache', label: 'Authorization cache'},
            {type: 'doc', id: 'design/component-server', label: 'Server'},
            {type: 'doc', id: 'design/component-streams', label: 'Streams'},
            {type: 'doc', id: 'design/component-config', label: 'Configuration and operations'},
            {type: 'doc', id: 'design/testing', label: 'Test strategy'},
            {type: 'doc', id: 'design/dependency-licenses', label: 'Dependency licenses'},
          ],
        },
        {
          type: 'category',
          label: 'Decision records',
          link: {type: 'doc', id: 'adr/README'},
          items: adrItems,
        },
        {
          type: 'category',
          label: 'RFCs',
          link: {type: 'doc', id: 'rfcs/README'},
          items: [
            {type: 'doc', id: 'rfcs/0002-backend-plugin-policy', label: 'RFC 0002: Backend plugin policy'},
            {type: 'doc', id: 'rfcs/0003-backend-acceptance-criteria', label: 'RFC 0003: Backend acceptance criteria'},
            {type: 'doc', id: 'rfcs/0000-mongodb-backend', label: 'MongoDB backend'},
            {type: 'doc', id: 'rfcs/draft-cassandra-backend', label: 'Cassandra backend (draft)'},
          ],
        },
      ],
    },
  ],
};

export default sidebars;
