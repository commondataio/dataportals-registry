import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  mainSidebar: [
    {
      type: 'category',
      label: 'Overview',
      items: [
        'getting-started',
        'when-to-use',
        'architecture',
        'directory-layout',
        'cli',
        'scheduled',
        'releasing',
      ],
    },
    {
      type: 'category',
      label: 'Discovery',
      items: [
        {
          type: 'doc',
          id: 'discovery',
          label: 'Overview',
        },
        {
          type: 'doc',
          id: 'discovery-search-tools',
          label: 'Search engines',
        },
        {
          type: 'doc',
          id: 'discovery-agent-tools',
          label: 'Agents and LLM clients',
        },
        {
          type: 'doc',
          id: 'discovery-opendata',
          label: 'Open data portals',
        },
        {
          type: 'doc',
          id: 'discovery-geoportals',
          label: 'Geoportals',
        },
        {
          type: 'doc',
          id: 'discovery-geoportals-sdi',
          label: 'Geoportals — SDI platforms',
        },
        {
          type: 'doc',
          id: 'discovery-geoportals-viewers',
          label: 'Geoportals — viewers',
        },
        {
          type: 'doc',
          id: 'discovery-scientific',
          label: 'Scientific repositories',
        },
        {
          type: 'doc',
          id: 'discovery-scientific-domain',
          label: 'Scientific — domain repositories',
        },
        {
          type: 'doc',
          id: 'discovery-metadata',
          label: 'Metadata catalogs',
        },
        {
          type: 'doc',
          id: 'discovery-indicators',
          label: 'Indicators and microdata',
        },
        {
          type: 'doc',
          id: 'discovery-other',
          label: 'Search, ML, API, marketplaces',
        },
      ],
    },
    {
      type: 'category',
      label: 'Harvest',
      items: [
        {
          type: 'doc',
          id: 'harvest',
          label: 'Overview',
        },
        {
          type: 'doc',
          id: 'harvest-scientific',
          label: 'Scientific repositories',
        },
        {
          type: 'doc',
          id: 'harvest-scientific-domain',
          label: 'Scientific — domain repositories',
        },
        {
          type: 'doc',
          id: 'harvest-opendata',
          label: 'Open data portals',
        },
        {
          type: 'doc',
          id: 'harvest-geoportals',
          label: 'Geoportals',
        },
        {
          type: 'doc',
          id: 'harvest-indicators',
          label: 'Indicators and microdata',
        },
        {
          type: 'doc',
          id: 'harvest-metadata',
          label: 'Metadata catalogs',
        },
        {
          type: 'doc',
          id: 'harvest-other',
          label: 'Search, ML, API, marketplaces',
        },
        {
          type: 'doc',
          id: 'harvest-protocols',
          label: 'Protocols',
        },
        {
          type: 'doc',
          id: 'harvest-incremental',
          label: 'Incremental harvests',
        },
        {
          type: 'doc',
          id: 'harvest-earthdata',
          label: 'Earth observation',
        },
        {
          type: 'doc',
          id: 'harvest-biodiversity',
          label: 'Biodiversity and genomics',
        },
        {
          type: 'doc',
          id: 'harvest-viewers',
          label: 'Map viewers',
        },
        {
          type: 'doc',
          id: 'harvest-identifiers',
          label: 'Dataset identifiers',
        },
        {
          type: 'doc',
          id: 'harvest-output',
          label: 'Harvest output',
        },
      ],
    },
    {
      type: 'category',
      label: 'Data contracts',
      items: [
        'ai-consumers',
        'data-model',
        'catalog-types',
        'software-taxonomy',
        'software-index',
        'vocabularies',
        'exports',
        'metadata-quality',
        'quality-rules',
        'trust-score',
      ],
    },
    {
      type: 'category',
      label: 'Pipelines',
      items: ['re3data', 'ckan-sync', 'apidetect', 'liveness', 'enrichment'],
    },
    {
      type: 'category',
      label: 'Query examples',
      items: ['query-examples'],
    },
    {
      type: 'category',
      label: 'Agent workflows',
      items: [
        'agents/query',
        'agents/discover',
        'agents/harvest',
        'agents/contribute',
        'agents/openspec-quickstart',
      ],
    },
  ],
};

export default sidebars;
