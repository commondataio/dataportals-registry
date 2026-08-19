import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import DocsContents from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  const logoSrc = useBaseUrl('img/logo.svg');
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className="container">
        <img
          className={styles.heroLogo}
          src={logoSrc}
          alt=""
          width={96}
          height={96}
        />
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <p className={styles.heroMeta}>
          Part of the{' '}
          <Link href="https://dateno.io">Dateno</Link> open-source project.
          Code is MIT; data and documentation are{' '}
          <Link href="https://creativecommons.org/licenses/by/4.0/">
            CC BY 4.0
          </Link>
          .
        </p>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Documentation"
      description="dataportals-registry documentation: catalog YAML, exports, quality pipeline, and agent workflows.">
      <HomepageHeader />
      <main>
        <DocsContents />
      </main>
    </Layout>
  );
}
