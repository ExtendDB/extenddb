// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const devImage = `docker run -d --name extenddb -p 127.0.0.1:18080:18080 \\
  -v extenddb:/var/lib/extenddb extenddb/extenddb-dev

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \\
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \\
aws dynamodb list-tables --region us-east-1 \\
  --endpoint-url http://127.0.0.1:18080`;

const compose = `curl -fsSLO https://raw.githubusercontent.com/ExtendDB/extenddb/main/docker-compose.yml
EXTENDDB_IMAGE=extenddb/extenddb-postgres:latest \\
  docker compose up -d
curl -sk https://127.0.0.1:18443/health`;

type Card = {title: string; body: string; to: string};

const cards: Card[] = [
  {
    title: 'Getting started',
    body: 'A server in one command, your first requests, and the SDK connection. Then the install options: containers, Linux, macOS, from source.',
    to: '/start/',
  },
  {
    title: 'Guides',
    body: 'Day-to-day use, administration, deployment, the security model, upgrades, and the storage backends.',
    to: '/guides/',
  },
  {
    title: 'Reference',
    body: 'Every behavior that differs from the DynamoDB service, the limits, configuration keys, and troubleshooting.',
    to: '/reference/',
  },
  {
    title: 'Contributing',
    body: 'Build and test the code, and add a storage backend under the plugin policy and acceptance criteria.',
    to: '/contributing/',
  },
  {
    title: 'Architecture',
    body: 'The architecture and design guides, component designs, decision records and RFCs.',
    to: '/architecture/',
  },
];

function Hero(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const version = String(siteConfig.customFields?.extenddbVersion ?? '');
  return (
    <header className={styles.hero}>
      <div className={styles.heroInner}>
        <p className={styles.eyebrow}>ExtendDB documentation{version ? ` · v${version}` : ''}</p>
        <Heading as="h1" className={styles.title}>
          The DynamoDB API, everywhere you run code.
        </Heading>
        <p className={styles.lede}>
          ExtendDB is a DynamoDB-compatible API server with pluggable storage. It speaks the DynamoDB wire
          protocol, so the AWS SDKs, the AWS CLI and the tools that work with DynamoDB work with ExtendDB once
          their endpoint URL points at it. It stores data in PostgreSQL, MongoDB, SQLite or Cassandra, and runs on
          a laptop, in CI, on your own servers, or wherever a container runs.
        </p>
        <div className={styles.actions}>
          <Link className="button button--primary button--lg" to="/start/">
            Get started
          </Link>
          <Link className="button button--secondary button--lg" to="/differences-from-dynamodb/">
            Differences from DynamoDB
          </Link>
        </div>
      </div>
    </header>
  );
}

function QuickStarts(): ReactNode {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Two ways to a running server
        </Heading>
        <div className="row">
          <div className="col col--6">
            <div className={styles.panel}>
              <Heading as="h3">For local development and CI</Heading>
              <p>
                One container over SQLite, plain HTTP, a seeded example credential. Nothing to initialize and no
                certificate to trust. Bound to the loopback interface; not for anything reachable from other
                machines.
              </p>
              <CodeBlock language="bash">{devImage}</CodeBlock>
              <Link to="/dev-image/">About the development image</Link>
            </div>
          </div>
          <div className="col col--6">
            <div className={styles.panel}>
              <Heading as="h3">For a local stack with PostgreSQL</Heading>
              <p>
                The extenddb/extenddb-postgres image with TLS on, started together with a PostgreSQL container by the
                repository&apos;s Compose file, which runs the initialization for you.
              </p>
              <CodeBlock language="bash">{compose}</CodeBlock>
              <Link to="/local-dev-docker-compose/">Docker Compose walkthrough</Link>
              {' · '}
              <Link to="/manuals/deployment-guide/">Deploying on your own servers</Link>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function Sections(): ReactNode {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Find what you need
        </Heading>
        <div className={styles.cards}>
          {cards.map((c) => (
            <Link key={c.to} to={c.to} className={styles.card}>
              <Heading as="h3">{c.title}</Heading>
              <p>{c.body}</p>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="ExtendDB documentation"
      description="Install, run, operate and extend ExtendDB, the DynamoDB-compatible API server with pluggable storage.">
      <Hero />
      <main>
        <QuickStarts />
        <Sections />
      </main>
    </Layout>
  );
}
