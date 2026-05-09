"""Effect tags for `@task`-decorated functions.

Effect tags classify each task by what side effects (if any) it performs.
Phase 4 stores them as metadata on the `TaskSpec` and on each DAG node;
later phases use them to gate parallelism (Phase 4 executor flag), enable
content-addressed caching (Phase 6 — only PURE/READ are cacheable), drive
reactive recomputation (Phase 5 — only PURE auto-recomputes on input
change), and constrain replay (Phase 7 — NONDETERMINISTIC nodes can't be
replayed deterministically).

Default for a `@task` is `Effect.PURE`; an AST-scan heuristic emits a
`UserWarning` at decoration time if a `PURE` task touches obviously-impure
builtins like `time.time()`, `random.*`, `os.*`, or `requests.*`.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
import warnings
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


class Effect(Enum):
    """Purity / side-effect classification for a `@dagron.task`.

    Ordered from most-pure to most-impure:

    * `PURE` — deterministic; no I/O, no clock/random/threading. Cacheable,
      replayable, freely parallelizable. The default.
    * `READ` — reads from a stable external source (file, DB) but is
      idempotent over a snapshot. Cacheable when the source is pinned;
      replayable.
    * `WRITE` — writes to an external system. Not cacheable; replay is
      idempotent only if the receiver is.
    * `NETWORK` — performs network I/O. Output may vary; not cacheable;
      not deterministically replayable.
    * `NONDETERMINISTIC` — uses time, randomness, threading, or other
      sources of non-determinism. Not cacheable, not replayable; serializes
      under effect isolation.
    """

    PURE = "pure"
    READ = "read"
    WRITE = "write"
    NETWORK = "network"
    NONDETERMINISTIC = "nondeterministic"

    @property
    def is_cacheable(self) -> bool:
        """True if this effect class admits content-addressed caching."""
        return self in (Effect.PURE, Effect.READ)

    @property
    def is_deterministic(self) -> bool:
        """True if multiple invocations with the same inputs produce the same output."""
        return self in (Effect.PURE, Effect.READ)

    @property
    def is_isolated(self) -> bool:
        """True if instances of this effect class must serialize under effect isolation."""
        return self == Effect.NONDETERMINISTIC


# ---------------------------------------------------------------------------
# AST-scan heuristic: detect obviously-impure calls inside a PURE task body
# ---------------------------------------------------------------------------


# Module-attribute pairs whose presence in a PURE task body is suspicious.
# Format: (module_name, attribute_name) — None for the attribute means any access.
_IMPURE_CALLS: frozenset[tuple[str, str | None]] = frozenset(
    {
        ("time", "time"),
        ("time", "monotonic"),
        ("time", "perf_counter"),
        ("time", "sleep"),
        ("random", None),
        ("os", None),
        ("requests", None),
        ("urllib", None),
        ("httpx", None),
        ("aiohttp", None),
        ("socket", None),
        ("uuid", "uuid1"),
        ("uuid", "uuid4"),
        ("threading", None),
        ("multiprocessing", None),
    }
)


def _scan_for_impure_calls(fn: Callable[..., object]) -> list[str]:
    """Walk a function's AST and return a list of suspicious call names.

    Returns an empty list if the source can't be retrieved (built-in,
    REPL-defined function, etc.) or if no impure calls are found.
    """
    try:
        source = inspect.getsource(fn)
    except (OSError, TypeError):
        return []

    try:
        tree = ast.parse(textwrap.dedent(source))
    except SyntaxError:
        return []

    findings: list[str] = []
    for node in ast.walk(tree):
        # Pattern: `module.attr(...)` or `module.attr` access inside the function
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            mod = node.value.id
            attr = node.attr
            if (mod, attr) in _IMPURE_CALLS or (mod, None) in _IMPURE_CALLS:
                findings.append(f"{mod}.{attr}")
        # Direct name access (e.g. `random` imported as `from random import *`)
        # is harder to detect reliably without import tracking; skip for now.

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for f in findings:
        if f not in seen:
            seen.add(f)
            deduped.append(f)
    return deduped


def _warn_if_impure(fn: Callable[..., object], effect: Effect) -> None:
    """Emit a UserWarning if a PURE-tagged function appears to be impure."""
    if effect != Effect.PURE:
        return
    findings = _scan_for_impure_calls(fn)
    if not findings:
        return
    qualname = getattr(fn, "__qualname__", fn.__name__)
    warnings.warn(
        f"@task(effect=Effect.PURE) {qualname!r} appears to call impure "
        f"functions: {', '.join(findings)}. If the call is intentional, "
        f"either silence with @task(effect=Effect.NONDETERMINISTIC) (or a "
        f"more specific tag) or refactor to inject the dependency. "
        f"This is a heuristic — false positives are possible.",
        UserWarning,
        stacklevel=3,
    )


def effects_of(dag: object) -> dict[str, Effect]:
    """Read every node's effect tag from a DAG built by `@dagron.flow`.

    Looks up each node's metadata for an `"effect"` string and converts
    it back to an `Effect`. Nodes without an effect tag default to
    `Effect.PURE` (which is also the default for `@task`).

    Args:
        dag: A `dagron.DAG` instance (typed as `object` to avoid an import
            cycle with `dagron._internal`).
    """
    result: dict[str, Effect] = {}
    for node in dag.nodes():  # type: ignore[attr-defined]
        meta = dag.get_metadata(node.name)  # type: ignore[attr-defined]
        if isinstance(meta, dict) and isinstance(meta.get("effect"), str):
            try:
                result[node.name] = Effect(meta["effect"])
                continue
            except ValueError:
                pass
        result[node.name] = Effect.PURE
    return result
