# dagron-ui

[![crates.io](https://img.shields.io/crates/v/dagron-ui.svg)](https://crates.io/crates/dagron-ui)
[![docs.rs](https://img.shields.io/docsrs/dagron-ui)](https://docs.rs/dagron-ui)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Live web dashboard for [dagron](https://github.com/ByteVeda/dagron) DAG
execution — an [axum](https://github.com/tokio-rs/axum) server that streams node
state to a single self-contained HTML page.

Used by the `dashboard` feature of the `dagron` Python package; usable directly
from Rust for any execution engine that can push state updates.

## Install

```toml
[dependencies]
dagron-ui = "0.1"
```

## What it does

- Serves a zero-dependency dashboard page (no CDN, no build step) on a bound
  port, in a background thread with its own tokio runtime.
- Streams live node status over SSE as the graph executes.
- Holds the shared `DashboardState` that the executor writes into.
- Supports gating — the UI can hold execution at a node and release it via a
  `GateCallback`, for step-through debugging.

## License

MIT
