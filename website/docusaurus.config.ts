import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'dataportals-registry',
  tagline: 'Registry of data portals, catalogs, and repositories',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  url: 'https://datenoio.github.io',
  baseUrl: '/dataportals-registry/',
  trailingSlash: true,

  organizationName: 'datenoio',
  projectName: 'dataportals-registry',

  onBrokenLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          path: '../docs',
          routeBasePath: '/docs',
          editUrl:
            'https://github.com/datenoio/dataportals-registry/edit/main/docs/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.svg',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'dataportals-registry',
      logo: {
        alt: 'dataportals-registry',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'mainSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://datenoio.github.io/dataportals-registry/llms.txt',
          label: 'llms.txt',
          position: 'right',
        },
        {
          href: 'https://github.com/datenoio/dataportals-registry',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting started',
              to: '/docs/getting-started',
            },
            {
              label: 'Architecture',
              to: '/docs/architecture',
            },
            {
              label: 'AI consumers',
              to: '/docs/ai-consumers',
            },
            {
              label: 'Query examples',
              to: '/docs/query-examples',
            },
          ],
        },
        {
          title: 'For coding agents',
          items: [
            {
              label: 'Query workflow',
              to: '/docs/agents/query',
            },
            {
              label: 'Contribute workflow',
              to: '/docs/agents/contribute',
            },
            {
              label: 'llms.txt',
              href: 'https://datenoio.github.io/dataportals-registry/llms.txt',
            },
          ],
        },
        {
          title: 'License',
          items: [
            {
              label: 'Code: MIT',
              href: 'https://github.com/datenoio/dataportals-registry/blob/main/LICENSE',
            },
            {
              label: 'Data: CC BY 4.0',
              href: 'https://creativecommons.org/licenses/by/4.0/',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Dateno. dataportals-registry is part of the Dateno open-source project. Code is MIT; data and documentation are CC BY 4.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['python', 'bash', 'yaml', 'sql', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
