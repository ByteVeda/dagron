import { cn } from "@/lib/cn";
import { Mermaid } from "./mermaid";

export type DagDiagramProps = {
  chart: string;
  caption?: string;
  className?: string;
};

/**
 * MDX-friendly wrapper around `Mermaid` that adds an optional caption and
 * matches dagron's documentation styling. Falls back to vanilla `<Mermaid>`
 * if you only need a diagram with no caption.
 */
export function DagDiagram({ chart, caption, className }: DagDiagramProps) {
  return (
    <figure className={cn("my-6", className)}>
      <Mermaid chart={chart} />
      {caption ? (
        <figcaption className="mt-2 text-center text-sm text-fd-muted-foreground">
          {caption}
        </figcaption>
      ) : null}
    </figure>
  );
}
