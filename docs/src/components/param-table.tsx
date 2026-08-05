import { cn } from "@/lib/cn";

export type Param = {
  name: string;
  type: string;
  default?: string;
  description: string;
};

export type ParamTableProps = {
  params: Param[];
  className?: string;
};

/**
 * Renders a parameter reference table with name / type / default / description
 * columns. Used heavily on API reference pages.
 */
export function ParamTable({ params, className }: ParamTableProps) {
  return (
    <div className={cn("my-6 overflow-x-auto", className)}>
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="bg-fd-muted text-left">
            <th className="border-b border-fd-border px-3 py-2 font-semibold">
              Parameter
            </th>
            <th className="border-b border-fd-border px-3 py-2 font-semibold">
              Type
            </th>
            <th className="border-b border-fd-border px-3 py-2 font-semibold">
              Default
            </th>
            <th className="border-b border-fd-border px-3 py-2 font-semibold">
              Description
            </th>
          </tr>
        </thead>
        <tbody>
          {params.map((p) => (
            <tr key={p.name}>
              <td className="border-b border-fd-border px-3 py-2 align-top">
                <code className="font-mono text-xs">{p.name}</code>
              </td>
              <td className="border-b border-fd-border px-3 py-2 align-top">
                <code className="font-mono text-xs">{p.type}</code>
              </td>
              <td className="border-b border-fd-border px-3 py-2 align-top">
                {p.default !== undefined ? (
                  <code className="font-mono text-xs">{p.default}</code>
                ) : (
                  <em className="text-xs text-fd-muted-foreground">required</em>
                )}
              </td>
              <td className="border-b border-fd-border px-3 py-2 align-top">
                {p.description}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
