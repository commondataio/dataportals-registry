import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type DocLink = {
  to: string;
  label: string;
  description: string;
};

type DocSection = {
  title: string;
  items: DocLink[];
};

const sections: DocSection[] = [
  {
    title: 'Overview',
    items: [
      {
        to: '/docs/getting-started',
        label: 'Getting started',
        description: 'DuckDB, Parquet, and YAML paths into the registry.',
      },
      {
        to: '/docs/when-to-use',
        label: 'When to use',
        description: 'What this registry is for, and what lives elsewhere.',
      },
      {
        to: '/docs/architecture',
        label: 'Architecture',
        description: 'Source YAML, validation, enrichment, and exports.',
      },
      {
        to: '/docs/discovery',
        label: 'Discover catalogs',
        description: 'Find portals not yet in the registry, then add them.',
      },
      {
        to: '/docs/directory-layout',
        label: 'Directory layout',
        description: 'Country, type, and filename conventions.',
      },
      {
        to: '/docs/cli',
        label: 'CLI',
        description: 'builder.py commands for build, validate, and quality.',
      },
      {
        to: '/docs/scheduled',
        label: 'Scheduled entries',
        description: 'Promote unverified YAML from scheduled to entities.',
      },
      {
        to: '/docs/releasing',
        label: 'Releases',
        description: 'Tag, changelog, and GitHub release checklist.',
      },
    ],
  },
  {
    title: 'Data contracts',
    items: [
      {
        to: '/docs/ai-consumers',
        label: 'AI consumer guide',
        description: 'Join keys, scope, and how to consume exports.',
      },
      {
        to: '/docs/data-model',
        label: 'Data model',
        description: 'Required and recommended catalog fields.',
      },
      {
        to: '/docs/catalog-types',
        label: 'Catalog types',
        description: 'Open data, geo, scientific, and related types.',
      },
      {
        to: '/docs/software-taxonomy',
        label: 'Software taxonomy',
        description: 'Platform IDs, categories, and subtypes.',
      },
      {
        to: '/docs/vocabularies',
        label: 'Vocabularies',
        description: 'Geographic levels, identifiers, endpoints, topics.',
      },
      {
        to: '/docs/exports',
        label: 'Exports',
        description: 'JSONL, Parquet, and DuckDB exports.',
      },
      {
        to: '/docs/metadata-quality',
        label: 'Metadata quality',
        description: 'Recommended fields and quality-report tracks.',
      },
      {
        to: '/docs/quality-rules',
        label: 'Quality issue types',
        description: 'Every analyze-quality code, track, and fix hint.',
      },
      {
        to: '/docs/trust-score',
        label: 'Trust score',
        description: '0–100 scoring components and interpretation.',
      },
    ],
  },
    {
      title: 'Pipelines',
      items: [
        {
          to: '/docs/re3data',
          label: 'Re3Data enrichment',
          description: 'Fill _re3data from re3data.org identifiers.',
        },
        {
          to: '/docs/ckan-sync',
          label: 'CKAN ecosystem sync',
          description: 'Import CKAN sites from ecosystem.ckan.org.',
        },
      ],
    },
    {
      title: 'Query examples',
      items: [
      {
        to: '/docs/query-examples',
        label: 'DuckDB / Parquet',
        description: 'Verified filters by country, software, and type.',
      },
    ],
  },
  {
    title: 'Agent workflows',
    items: [
      {
        to: '/docs/agents/query',
        label: 'Query workflow',
        description: 'Look up catalogs from exports, not YAML.',
      },
      {
        to: '/docs/agents/discover',
        label: 'Discover workflow',
        description: 'Find unregistered catalogs without duplicating records.',
      },
      {
        to: '/docs/agents/contribute',
        label: 'Contribute workflow',
        description: 'Add or edit catalog YAML safely.',
      },
      {
        to: '/docs/agents/openspec-quickstart',
        label: 'OpenSpec quickstart',
        description: 'Schema and pipeline changes.',
      },
    ],
  },
];

function DocItem({to, label, description}: DocLink) {
  return (
    <li className={styles.item}>
      <Link className={styles.itemLink} to={to}>
        <span className={styles.itemLabel}>{label}</span>
        <span className={styles.itemDescription}>{description}</span>
      </Link>
    </li>
  );
}

export default function DocsContents(): ReactNode {
  return (
    <section className={styles.contents}>
      <div className="container">
        <Heading as="h2" className={styles.contentsTitle}>
          Documentation contents
        </Heading>
        <div className={styles.grid}>
          {sections.map((section) => (
            <section key={section.title} className={styles.section}>
              <Heading as="h3" className={styles.sectionTitle}>
                {section.title}
              </Heading>
              <ul className={styles.list}>
                {section.items.map((item) => (
                  <DocItem key={item.to} {...item} />
                ))}
              </ul>
            </section>
          ))}
        </div>
      </div>
    </section>
  );
}
