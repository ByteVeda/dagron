import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  guideSidebar: [
    'intro',
    'guide/why-dagron',
    {
      type: 'category',
      label: 'Guide',
      collapsed: false,
      link: { type: 'generated-index', title: 'User Guide', slug: '/guide' },
      items: [
        'guide/getting-started',
        'guide/benchmarks',
        'guide/cookbook',
        {
          type: 'category',
          label: 'Core Concepts',
          collapsed: false,
          items: [
            'guide/core-concepts/building-dags',
            'guide/core-concepts/executing-tasks',
            'guide/core-concepts/inspecting-graphs',
            'guide/core-concepts/transforms',
            'guide/core-concepts/serialization',
          ],
        },
        {
          type: 'category',
          label: 'Execution Strategies',
          collapsed: false,
          items: [
            'guide/execution-strategies/incremental',
            'guide/execution-strategies/conditional',
            'guide/execution-strategies/dynamic-dags',
            'guide/execution-strategies/checkpointing',
            'guide/execution-strategies/caching',
            'guide/execution-strategies/resource-scheduling',
            'guide/execution-strategies/approval-gates',
            'guide/execution-strategies/distributed',
          ],
        },
        {
          type: 'category',
          label: 'Observability',
          collapsed: true,
          items: [
            'guide/observability/tracing-profiling',
            'guide/observability/visualization',
            'guide/observability/error-handling',
          ],
        },
        {
          type: 'category',
          label: 'Advanced Features',
          collapsed: true,
          items: [
            'guide/advanced/templates',
            'guide/advanced/versioning',
            'guide/advanced/contracts',
            'guide/advanced/dataframes',
            'guide/advanced/plugins-hooks',
          ],
        },
        'guide/architecture',
      ],
    },
  ],
  apiSidebar: [
    {
      type: 'category',
      label: 'API Reference',
      collapsed: false,
      link: { type: 'generated-index', title: 'API Reference', slug: '/api' },
      items: [
        {
          type: 'category',
          label: 'Core',
          collapsed: false,
          items: ['api/core/core', 'api/core/builder', 'api/core/errors'],
        },
        {
          type: 'category',
          label: 'Execution',
          collapsed: false,
          items: [
            'api/execution/execution',
            'api/execution/pipeline',
            'api/execution/incremental',
            'api/execution/caching',
            'api/execution/checkpoint',
            'api/execution/conditions',
            'api/execution/dynamic',
            'api/execution/gates',
            'api/execution/resources',
            'api/execution/distributed',
            'api/execution/reactive',
          ],
        },
        {
          type: 'category',
          label: 'Observability',
          collapsed: true,
          items: ['api/observability/tracing', 'api/observability/profiling'],
        },
        {
          type: 'category',
          label: 'Analysis & Validation',
          collapsed: true,
          items: [
            'api/analysis/analysis',
            'api/analysis/contracts',
            'api/analysis/dataframe',
          ],
        },
        {
          type: 'category',
          label: 'Utilities',
          collapsed: true,
          items: [
            'api/utilities/template',
            'api/utilities/versioning',
            'api/utilities/compose',
            'api/utilities/display',
            'api/utilities/integration',
            'api/utilities/plugins',
          ],
        },
      ],
    },
  ],
};

export default sidebars;
