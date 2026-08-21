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
          id: 'discovery-scientific',
          label: 'Scientific repositories',
        },
        {
          type: 'doc',
          id: 'discovery-indicators',
          label: 'Indicators and microdata',
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
      items: ['re3data', 'ckan-sync'],
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
        'agents/contribute',
        'agents/openspec-quickstart',
      ],
    },
  ],
};

export default sidebars;
