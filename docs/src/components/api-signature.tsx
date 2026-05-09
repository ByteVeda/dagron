import { DynamicCodeBlock } from "fumadocs-ui/components/dynamic-codeblock";
import { cn } from "@/lib/cn";

export type ApiSignatureProps = {
  name: string;
  signature: string;
  language?: string;
  className?: string;
};

function toAnchorId(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Anchored signature block used for API reference pages. Renders the symbol
 * name as a deep-linkable header above a syntax-highlighted code block.
 */
export function ApiSignature({
  name,
  signature,
  language = "python",
  className,
}: ApiSignatureProps) {
  const id = toAnchorId(name);
  return (
    <div
      id={id}
      className={cn(
        "my-6 rounded-md border border-fd-border bg-fd-card",
        className,
      )}
    >
      <a
        href={`#${id}`}
        className="block px-4 pt-3 pb-1 font-mono text-sm font-semibold text-fd-primary hover:underline"
      >
        {name}
      </a>
      <div className="px-4 pb-3">
        <DynamicCodeBlock lang={language} code={signature} />
      </div>
    </div>
  );
}
