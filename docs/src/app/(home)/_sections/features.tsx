import { FeatureCard, FeatureGrid } from "@/components/feature-card";
import { SectionHeader } from "@/components/ui/section-header";

const FEATURES = [
  {
    title: "Typed Node Handles",
    description:
      "NodeRef carries an Arc<str>+epoch handle returned by add_node — every API accepts str | NodeRef, stale handles error fast.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "@dagron.flow",
    description:
      "Tawazi-style: write a Python function, let the call structure become the DAG. Pythonic, no string IDs.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "Reactive Engine",
    description:
      "Signal/Computed/Watcher with auto-tracked deps. ~10 µs to recompute one branch out of 10k after upstream mutation.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "Content-Addressed Cache",
    description:
      "Nix-flake-style cross-process cache backed by the filesystem. Two CI workers share intermediates without coordination.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "Time-Travel Replay",
    description:
      "Append-only JSONL traces + payload-deduped CAS. replay(at=t) reconstructs any past run state.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "Effect-Typed Tasks",
    description:
      "PURE / READ / WRITE / NETWORK / NONDETERMINISTIC tags drive cache opt-in, replay safety, and executor isolation.",
    guideHref: "/typed-and-reactive",
  },
  {
    title: "DAG Builder",
    description:
      "Fluent builder, from_records, and Pipeline / @task decorator for defining DAGs.",
    guideHref: "/guide/core-concepts/building-dags",
    apiHref: "/api/core/builder",
  },
  {
    title: "Parallel Execution",
    description:
      "Thread-pool and async executors with topological scheduling and cost-aware planning.",
    guideHref: "/guide/core-concepts/executing-tasks",
    apiHref: "/api/execution/execution",
  },
  {
    title: "Incremental Execution",
    description: "Early-cutoff recomputation — only re-execute what changed.",
    guideHref: "/guide/execution-strategies/incremental",
    apiHref: "/api/execution/incremental",
  },
  {
    title: "Checkpointing",
    description: "Save progress to disk and resume after failures.",
    guideHref: "/guide/execution-strategies/checkpointing",
    apiHref: "/api/execution/checkpoint",
  },
  {
    title: "Conditional & Dynamic DAGs",
    description:
      "Predicate-gated edges, runtime expansion based on node results.",
    guideHref: "/guide/execution-strategies/conditional",
    apiHref: "/api/execution/conditions",
  },
  {
    title: "Resource & Approval Gates",
    description:
      "GPU/CPU/memory-aware scheduling; human-in-the-loop pauses until approved.",
    guideHref: "/guide/execution-strategies/resource-scheduling",
    apiHref: "/api/execution/resources",
  },
  {
    title: "Distributed Execution",
    description: "Pluggable backends: threads, multiprocessing, Ray, Celery.",
    guideHref: "/guide/execution-strategies/distributed",
    apiHref: "/api/execution/distributed",
  },
  {
    title: "Tracing & Profiling",
    description:
      "Chrome-compatible execution traces and critical-path analysis.",
    guideHref: "/guide/observability/tracing-profiling",
    apiHref: "/api/observability/tracing",
  },
  {
    title: "Graph Analysis",
    description:
      "Explain, what-if, lineage tracking, linting, and a query DSL.",
    guideHref: "/guide/core-concepts/inspecting-graphs",
    apiHref: "/api/analysis/analysis",
  },
  {
    title: "Contracts & DataFrames",
    description:
      "Type contracts across edges, validated at build time. Schema validation for pandas/polars pipelines.",
    guideHref: "/guide/advanced/contracts",
    apiHref: "/api/analysis/contracts",
  },
  {
    title: "Templates & Versioning",
    description:
      "Parameterised templates with placeholder expansion; append-only mutation log with diffing and forking.",
    guideHref: "/guide/advanced/templates",
    apiHref: "/api/utilities/template",
  },
  {
    title: "Plugins & Hooks",
    description:
      "Event-driven plugin system with hook registry and auto-discovery.",
    guideHref: "/guide/advanced/plugins-hooks",
    apiHref: "/api/utilities/plugins",
  },
  {
    title: "Visualization",
    description: "ASCII, SVG, Mermaid, and a live web dashboard (Axum + SSE).",
    guideHref: "/guide/observability/visualization",
    apiHref: "/api/utilities/display",
  },
];

export function Features() {
  return (
    <section className="py-16">
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
        <SectionHeader
          title="Everything you need to ship a DAG"
          description="Build it, run it, cache it, replay it. dagron covers the lifecycle from prototype to production."
        />
        <FeatureGrid>
          {FEATURES.map((f) => (
            <FeatureCard key={f.title} {...f} />
          ))}
        </FeatureGrid>
      </div>
    </section>
  );
}
