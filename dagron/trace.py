"""Time-travel debugging — persistent execution traces with `replay(at=t)`.

Every node execution can be appended to an on-disk trace log. Each log
entry stores the node's fingerprints (input + output) and metadata; the
actual *payload* lives in the Phase 6 `ContentCache` keyed by the output
fingerprint, so storage stays compact and outputs are deduplicated across
runs that produced the same value.

Replaying the run reconstructs the per-node `ExecutionResult`-like state
*as of* a chosen wall-clock time. Pure nodes replay byte-identically;
impure nodes (`WRITE` / `NETWORK` / `NONDETERMINISTIC`) surface a
"non-replayable" marker but still expose their logged output value, so
you can inspect what the run actually produced without claiming
reproducibility.

Example::

    from pathlib import Path
    from dagron import Effect
    from dagron.contentcache import ContentCache
    from dagron.trace import TraceWriter, TraceReader, replay

    cas = ContentCache()
    log_path = Path("/tmp/dagron-traces/myrun.jsonl")
    writer = TraceWriter(log_path, cas=cas)
    writer.record("fetch", value=[1, 2, 3], effect=Effect.PURE)
    writer.record("total", value=6, effect=Effect.PURE)
    writer.close()

    reader = TraceReader(log_path, cas=cas)
    state = replay(reader)
    state["total"].value        # 6
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dagron.contentcache import ContentCache, default_cache_dir

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dagron.effects import Effect


# ---------------------------------------------------------------------------
# Default locations
# ---------------------------------------------------------------------------


def default_trace_dir() -> Path:
    """`$DAGRON_TRACE_DIR` or `~/.cache/dagron/traces`."""
    env = os.environ.get("DAGRON_TRACE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "dagron" / "traces"


def new_run_id() -> str:
    """Random short identifier for a single execution run."""
    return uuid.uuid4().hex[:16]


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TraceRecord:
    """One node's execution recorded to the trace log.

    `output_fp` is the hex digest under which the node's value lives in the
    `ContentCache`. `replayable` mirrors `Effect.is_deterministic` at
    record time so the replayer can flag results that aren't guaranteed
    reproducible (e.g., NETWORK reads).
    """

    timestamp: float
    name: str
    output_fp: str  # hex
    duration_ns: int = 0
    effect: str = "pure"
    replayable: bool = True
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "t": self.timestamp,
                "name": self.name,
                "fp": self.output_fp,
                "dur_ns": self.duration_ns,
                "effect": self.effect,
                "replayable": self.replayable,
                "error": self.error,
                "metadata": self.metadata,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, line: str) -> TraceRecord:
        d = json.loads(line)
        return cls(
            timestamp=d["t"],
            name=d["name"],
            output_fp=d["fp"],
            duration_ns=d.get("dur_ns", 0),
            effect=d.get("effect", "pure"),
            replayable=d.get("replayable", True),
            error=d.get("error"),
            metadata=d.get("metadata", {}),
        )


@dataclass(frozen=True)
class ReplayedNode:
    """One node's state at a given replay timestamp."""

    name: str
    timestamp: float
    value: Any
    effect: str = "pure"
    replayable: bool = True
    duration_ns: int = 0
    error: str | None = None

    @property
    def has_value(self) -> bool:
        return self.error is None


# ---------------------------------------------------------------------------
# TraceWriter — append-only JSONL log
# ---------------------------------------------------------------------------


class TraceWriter:
    """Append-only writer for an execution trace.

    Each call to `record()` writes a JSONL line and stores the node's
    payload in the bound `ContentCache`. Atomic at the line level: the
    log file is opened in append mode with `O_APPEND`, so concurrent
    writers from multiple processes won't tear lines.

    Args:
        path: Path to the trace log file. Parent directories are created.
        cas: ContentCache used to store node payloads. If None, a default
            cache at `~/.cache/dagron/cas` is created.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        cas: ContentCache | None = None,
    ) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._cas = cas if cas is not None else ContentCache(default_cache_dir())
        # Open in append+binary mode so writes are O_APPEND-atomic per write().
        self._fh = self._path.open("ab")

    @property
    def path(self) -> Path:
        return self._path

    @property
    def cas(self) -> ContentCache:
        return self._cas

    def record(
        self,
        name: str,
        *,
        value: Any = None,
        effect: Effect | None = None,
        duration_ns: int = 0,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> TraceRecord:
        """Record a node execution.

        Args:
            name: Node name.
            value: The node's output value (stored in CAS, deduped by hash).
            effect: The node's effect tag. Drives the `replayable` flag.
            duration_ns: How long the node took (nanoseconds).
            error: Error message string if the node failed.
            metadata: Extra fields to round-trip through the log.
            timestamp: Override wall clock (default: now).

        Returns:
            The persisted `TraceRecord`.
        """
        ts = timestamp if timestamp is not None else time.time()
        # Hash + store payload in CAS (only if the node succeeded).
        if error is None:
            output_fp = self._cas.hash(value)
            self._cas.put(output_fp, value)
            output_hex = output_fp.hex()
        else:
            output_hex = ""

        rec = TraceRecord(
            timestamp=ts,
            name=name,
            output_fp=output_hex,
            duration_ns=duration_ns,
            effect=effect.value if effect is not None else "pure",
            replayable=effect.is_deterministic if effect is not None else True,
            error=error,
            metadata=metadata or {},
        )
        line = (rec.to_json() + "\n").encode()
        self._fh.write(line)
        # Don't fsync per write — the OS buffer is fine; close() will flush.
        return rec

    def flush(self) -> None:
        self._fh.flush()
        with contextlib.suppress(OSError):
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._fh.close()

    def __enter__(self) -> TraceWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# TraceReader — read records back, filter by time
# ---------------------------------------------------------------------------


class TraceReader:
    """Read a persisted trace log.

    Args:
        path: Path to the trace log file.
        cas: ContentCache used to fetch node payloads on demand.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        cas: ContentCache | None = None,
    ) -> None:
        self._path = Path(path)
        self._cas = cas if cas is not None else ContentCache(default_cache_dir())

    @property
    def path(self) -> Path:
        return self._path

    @property
    def cas(self) -> ContentCache:
        return self._cas

    def records(self) -> Iterator[TraceRecord]:
        """Yield every record in append order. Skips malformed lines."""
        if not self._path.exists():
            return
        with self._path.open("rb") as fh:
            for raw in fh:
                try:
                    line = raw.decode("utf-8").strip()
                except UnicodeDecodeError:
                    continue
                if not line:
                    continue
                try:
                    yield TraceRecord.from_json(line)
                except (json.JSONDecodeError, KeyError):
                    continue

    def timeline(self) -> list[tuple[float, str]]:
        """Return `[(timestamp, node_name), ...]` in record order."""
        return [(r.timestamp, r.name) for r in self.records()]

    def records_until(self, t: float, *, inclusive: bool = True) -> Iterator[TraceRecord]:
        """Yield only records with `timestamp <= t` (or `<` if not inclusive)."""
        for r in self.records():
            if (r.timestamp <= t) if inclusive else (r.timestamp < t):
                yield r
            else:
                break

    def fetch(self, rec: TraceRecord) -> Any:
        """Resolve a record's payload from the CAS. Returns None for failures
        and for records whose payload is no longer in the cache (cache may
        have been pruned)."""
        if not rec.output_fp:
            return None
        try:
            fp_bytes = bytes.fromhex(rec.output_fp)
        except ValueError:
            return None
        value, hit = self._cas.get(fp_bytes)
        return value if hit else None


# ---------------------------------------------------------------------------
# replay — reconstruct state at a chosen timestamp
# ---------------------------------------------------------------------------


def replay(
    source: TraceReader | Path | str,
    *,
    at: float | None = None,
    cas: ContentCache | None = None,
) -> dict[str, ReplayedNode]:
    """Reconstruct the per-node state of a recorded run, as of time `at`.

    For each node in the log up to `at`:

    * Pure / READ nodes: payload is fetched from CAS and exposed in the
      returned `ReplayedNode.value`. `replayable` is True.
    * Impure nodes (WRITE / NETWORK / NONDETERMINISTIC): payload is still
      fetched (from the CAS where the original run wrote it) and exposed,
      but `replayable` is False — the value is what *that* run produced,
      not what a fresh run would produce.

    If a node was recorded multiple times in the log (e.g., re-runs), the
    *latest* record up to `at` wins.

    Args:
        source: a `TraceReader`, or a path to a trace file.
        at: wall-clock cutoff. None = end of log.
        cas: optional ContentCache override (only used when `source` is a
            path).

    Returns:
        Dict mapping node name to its `ReplayedNode` snapshot.
    """
    reader: TraceReader = (
        source if isinstance(source, TraceReader) else TraceReader(source, cas=cas)
    )

    cutoff = float("inf") if at is None else at
    state: dict[str, ReplayedNode] = {}
    for rec in reader.records_until(cutoff):
        value = reader.fetch(rec)
        state[rec.name] = ReplayedNode(
            name=rec.name,
            timestamp=rec.timestamp,
            value=value,
            effect=rec.effect,
            replayable=rec.replayable,
            duration_ns=rec.duration_ns,
            error=rec.error,
        )
    return state


# ---------------------------------------------------------------------------
# Convenience: gather all run paths under default_trace_dir()
# ---------------------------------------------------------------------------


def list_runs(trace_dir: Path | str | None = None) -> list[Path]:
    """List every trace log file (`*.jsonl`) under `trace_dir`."""
    base = Path(trace_dir) if trace_dir is not None else default_trace_dir()
    if not base.exists():
        return []
    return sorted(base.rglob("*.jsonl"))


__all__ = [
    "ReplayedNode",
    "TraceReader",
    "TraceRecord",
    "TraceWriter",
    "default_trace_dir",
    "list_runs",
    "new_run_id",
    "replay",
]
