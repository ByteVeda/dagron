import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { ApiSignature } from "./api-signature";
import { DagDiagram } from "./dag-diagram";
import { DiagramCarousel, DiagramSlide } from "./diagram-carousel";
import { EffectBadge } from "./effect-badge";
import { FeatureCard, FeatureGrid } from "./feature-card";
import { Mermaid } from "./mermaid";
import { ParamTable } from "./param-table";
import { StatusBadge } from "./status-badge";
import { Button, CodePanel, SectionHeader } from "./ui";

/**
 * Components made available to every MDX file globally — authors don't need
 * to write `import` lines. Pass `components` to override or add per-page.
 */
export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    // Diagram primitives
    Mermaid,
    DagDiagram,
    DiagramCarousel,
    DiagramSlide,
    // Annotation pills
    StatusBadge,
    EffectBadge,
    // API reference helpers
    ApiSignature,
    ParamTable,
    // Card / grid
    FeatureCard,
    FeatureGrid,
    // UI primitives (reusable across MDX and React pages)
    Button,
    CodePanel,
    SectionHeader,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
