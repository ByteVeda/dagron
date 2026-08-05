# dagron docs (Fumadocs)

Side-by-side replacement for `../docs/` (Docusaurus). Once verified, this
directory will be swapped in as `docs/`.

## Develop

```bash
pnpm install
pnpm dev          # http://localhost:3000
```

## Build for GitHub Pages

```bash
DOCS_BASE_PATH=/dagron pnpm build
npx serve out/    # then visit http://localhost:3000/dagron/
```

Local builds without `DOCS_BASE_PATH` serve cleanly from `/`.

## Lint & types

```bash
pnpm lint         # biome
pnpm types:check  # fumadocs-mdx + next typegen + tsc --noEmit
```

## Layout

```
src/
├── app/              # Next.js App Router
│   ├── (home)/       # marketing landing
│   ├── (docs)/       # docs sidebar + page renderer
│   └── api/          # Orama search endpoint, llms.txt routes
├── components/
│   ├── ui/           # generic primitives (Button, CodePanel, SectionHeader)
│   ├── mdx.tsx       # global MDX component map
│   ├── mermaid.tsx   # client-side mermaid with theme awareness
│   └── ...           # dagron-specific (DagDiagram, StatusBadge, FeatureCard, …)
└── lib/
    ├── source.ts     # Fumadocs source loader
    ├── shared.ts     # appName, gitConfig, route constants
    └── layout.shared.tsx  # nav + sidebar config

content/docs/         # 54 MDX files, organised by guide/ + api/
```

Components are registered globally in `src/components/mdx.tsx`, so MDX
authors don't need to write `import` lines.
