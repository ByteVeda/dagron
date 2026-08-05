import { DynamicCodeBlock } from "fumadocs-ui/components/dynamic-codeblock";
import { Button } from "@/components/ui/button";

const SAMPLE_CODE = `import dagron

dag = (
    dagron.DAG.builder()
    .add_node("extract")
    .add_node("transform")
    .add_node("load")
    .add_edge("extract", "transform")
    .add_edge("transform", "load")
    .build()
)

result = dagron.DAGExecutor(dag).execute({
    "extract":   lambda: fetch_data(),
    "transform": lambda: clean(result),
    "load":      lambda: write_to_db(result),
})`;

export function Hero() {
  return (
    <section className="py-16 sm:py-24">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl sm:text-6xl font-bold tracking-tight mb-6">
            <span className="text-fd-primary">dagron</span>
          </h1>
          <p className="text-xl sm:text-2xl text-fd-foreground mb-4 font-medium">
            High-performance DAG execution engine for Python, powered by Rust.
          </p>
          <p className="text-base sm:text-lg text-fd-muted-foreground max-w-3xl mx-auto leading-relaxed">
            <strong className="text-fd-foreground">
              Up to 12× faster than NetworkX
            </strong>{" "}
            on 10k-node DAG validation, with sub-microsecond reachability
            queries after index build. Build pipelines, schedulers, build
            systems — anything that runs as a graph.
          </p>
          <div className="mt-8 flex flex-wrap justify-center gap-3">
            <Button href="/guide/getting-started" variant="primary">
              Get Started
            </Button>
            <Button href="/typed-and-reactive" variant="secondary">
              Typed & Reactive →
            </Button>
            <Button
              href="https://github.com/ByteVeda/dagron"
              variant="ghost"
              target="_blank"
              rel="noopener noreferrer"
            >
              GitHub
            </Button>
          </div>
        </div>
        <div className="rounded-lg border border-fd-border bg-fd-card overflow-hidden">
          <DynamicCodeBlock lang="python" code={SAMPLE_CODE} />
        </div>
      </div>
    </section>
  );
}
