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
        'discovery',
        'directory-layout',
        'cli',
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
        'exports',
        'metadata-quality',
        'trust-score',
      ],
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
