import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type {Plugin} from '@docusaurus/types';

const config: Config = {
  title: 'dagron',
  tagline: 'High-performance DAG execution engine for Python, powered by Rust',
  favicon: 'img/favicon.ico',

  url: 'https://dagron.dev',
  baseUrl: '/',

  organizationName: 'dagron',
  projectName: 'dagron',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  themes: ['@docusaurus/theme-mermaid'],

  plugins: [
    function ignoreVscodeLanguageServerWarning(): Plugin {
      return {
        name: 'ignore-vscode-languageserver-warning',
        configureWebpack() {
          return {
            ignoreWarnings: [
              {module: /vscode-languageserver-types/},
            ],
          };
        },
      };
    },
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          path: 'pages',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/dagron-social-card.png',
    navbar: {
      title: 'dagron',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'guideSidebar',
          position: 'left',
          label: 'Guide',
        },
        {
          type: 'docSidebar',
          sidebarId: 'apiSidebar',
          position: 'left',
          label: 'API Reference',
        },
        {
          href: 'https://github.com/pratyush618/dagron/blob/master/CHANGELOG.md',
          label: 'Changelog',
          position: 'right',
        },
        {
          href: 'https://github.com/pratyush618/dagron/blob/master/CONTRIBUTING.md',
          label: 'Contributing',
          position: 'right',
        },
        {
          href: 'https://github.com/dagron/dagron',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Guide', to: '/guide/getting-started'},
            {label: 'API Reference', to: '/api/core/core'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/dagron/dagron'},
            {label: 'PyPI', href: 'https://pypi.org/project/dagron/'},
            {label: 'Changelog', href: 'https://github.com/pratyush618/dagron/blob/master/CHANGELOG.md'},
            {label: 'Contributing', href: 'https://github.com/pratyush618/dagron/blob/master/CONTRIBUTING.md'},
          ],
        },
      ],
      copyright: `Copyright &copy; ${new Date().getFullYear()} dagron contributors.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['python', 'rust', 'typescript', 'bash', 'json', 'toml'],
    },
    mermaid: {
      theme: {light: 'default', dark: 'dark'},
      options: {},
    },
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
