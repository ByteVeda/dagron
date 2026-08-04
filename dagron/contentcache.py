"""Content-addressed cache — Nix-flake-style cross-process compute caching.

Different from the in-process `dagron.execution.content_cache.ContentAddressableCache`:

* **Filesystem is the index.** No `index.json` to keep in sync — each cached
  entry lives at a path derived from its content hash, so independent
  processes (CI workers, two terminals, two Python interpreters) share
  intermediates transparently.
* **Effect-aware.** Only nodes whose `dagron.Effect` tag reports
  `is_cacheable` are stored; `WRITE`/`NETWORK`/`NONDETERMINISTIC` are
  bypassed automatically.
* **Pluggable hashers.** Pickle + blake2b is the default; bring your own
  `Hasher` for numpy arrays, polars frames, or anything tobyte-friendly.

Storage layout::

    ~/.cache/dagron/cas/<aa>/<bb>/<rest>.cache       payload bytes
    ~/.cache/dagron/cas/<aa>/<bb>/<rest>.cache.tmp   atomic temp file

where `<aa>/<bb>/<rest>` are the first two-character shards of the hex
fingerprint. POSIX `rename(2)` makes writes visible atomically.

Example::

    from dagron import Effect
    from dagron.contentcache import ContentCache, fingerprint_node

    cache = ContentCache()  # default location

    def slow_fn(rows: list[int]) -> int:
        return sum(rows)

    fp = fingerprint_node(slow_fn, Effect.PURE, [b"input-fp-bytes"])
    value, hit = cache.get(fp)
    if not hit:
        value = slow_fn([1, 2, 3])
        cache.put(fp, value)
"""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import os
import pickle
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron.effects import Effect


# ---------------------------------------------------------------------------
# Default cache location
# ---------------------------------------------------------------------------


def default_cache_dir() -> Path:
    """Default CAS location — `$DAGRON_CACHE_DIR` or `~/.cache/dagron/cas`."""
    env = os.environ.get("DAGRON_CACHE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "dagron" / "cas"


# ---------------------------------------------------------------------------
# Hasher protocol — pluggable per-type fingerprinting
# ---------------------------------------------------------------------------


@runtime_checkable
class Hasher(Protocol):
    """Compute a stable byte fingerprint for a Python value.

    Implementations should be deterministic (same value → same bytes
    across processes and Python sessions). For non-pickleable types,
    bring your own `Hasher` and register it via `ContentCache.register_hasher`.
    """

    def __call__(self, value: Any) -> bytes: ...


def default_hash(value: Any) -> bytes:
    """Default hasher: pickle + blake2b (32-byte digest).

    Pickle is deterministic enough for hashing in 99% of cases (Python's
    built-in containers, dataclasses, dicts in insertion order, etc.).
    Falls back to `repr()` for unpickleable values.
    """
    try:
        data = pickle.dumps(value, protocol=pickle.DEFAULT_PROTOCOL)
    except (pickle.PicklingError, TypeError, AttributeError):
        data = repr(value).encode("utf-8", errors="replace")
    return hashlib.blake2b(data, digest_size=32).digest()


def numpy_hash(value: Any) -> bytes:
    """Hasher for numpy arrays — uses `array.tobytes()` for byte equality.

    Falls back to `default_hash` for non-array inputs so it can be used
    as a default hasher in mixed pipelines.
    """
    try:
        import numpy as np
    except ImportError:
        return default_hash(value)

    if isinstance(value, np.ndarray):
        h = hashlib.blake2b(digest_size=32)
        h.update(str(value.dtype).encode())
        h.update(str(value.shape).encode())
        h.update(value.tobytes())
        return h.digest()
    return default_hash(value)


# ---------------------------------------------------------------------------
# Function fingerprinting
# ---------------------------------------------------------------------------


def fingerprint_function(fn: Callable[..., Any]) -> bytes:
    """Stable fingerprint for a callable.

    Combines:
    * `__qualname__`
    * `__code__.co_code` bytes (the bytecode)
    * `__code__.co_consts` tuple (constants referenced)
    * Names of free variables
    * Python major.minor version (bytecode is version-specific)

    Closure cell *values* are NOT included — the user must include them
    explicitly via `inputs` if they affect the result, otherwise stale
    closures could yield silent cache hits.
    """
    h = hashlib.blake2b(digest_size=32)
    h.update(f"py{sys.version_info.major}.{sys.version_info.minor}\n".encode())
    h.update(getattr(fn, "__qualname__", fn.__name__).encode())
    h.update(b"\x00")
    code = getattr(fn, "__code__", None)
    if code is not None:
        h.update(code.co_code)
        h.update(b"\x00")
        # co_consts may contain code objects (nested defs); pickle them.
        try:
            h.update(pickle.dumps(code.co_consts, protocol=pickle.DEFAULT_PROTOCOL))
        except Exception:
            h.update(repr(code.co_consts).encode())
        h.update(b"\x00")
        h.update(",".join(code.co_freevars).encode())
    return h.digest()


def fingerprint_node(
    fn: Callable[..., Any],
    effect: Effect | None,
    input_fingerprints: list[bytes],
) -> bytes:
    """Compose a node's full fingerprint from function + effect + inputs.

    Args:
        fn: The task function whose output is being fingerprinted.
        effect: The task's effect tag (or None to skip the tag).
        input_fingerprints: Ordered list of upstream input fingerprints.

    Returns:
        A 32-byte blake2b digest uniquely identifying this (function, inputs)
        combination. Stable across processes and Python invocations as long
        as the function's source and inputs don't change.
    """
    h = hashlib.blake2b(digest_size=32)
    h.update(fingerprint_function(fn))
    h.update(b"\x00")
    if effect is not None:
        h.update(effect.value.encode())
    h.update(b"\x00")
    for fp in input_fingerprints:
        h.update(fp)
    return h.digest()


# ---------------------------------------------------------------------------
# ContentCache — the storage backend
# ---------------------------------------------------------------------------


# Magic bytes prepended to every cache file so we can detect corruption
# and version mismatches.
_MAGIC = b"DAGRON\x06\x01"  # "DAGRON" + format-version major/minor


class ContentCache:
    """Cross-process content-addressed cache backed by the filesystem.

    Each entry is keyed by a 32-byte `bytes` fingerprint. The cache is
    transparent across processes: if process A computes and `put`s a
    fingerprint, process B's `get` for the same fingerprint hits the
    cache without any coordination, because the fingerprint is the
    filesystem path.

    Args:
        cache_dir: Directory to store cache files (default: `~/.cache/dagron/cas`).
        hasher: Optional custom hasher used by `compute_or_cached`.
            Independent of the storage layer.
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        hasher: Hasher | None = None,
    ) -> None:
        self._cache_dir = Path(cache_dir) if cache_dir is not None else default_cache_dir()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._hasher: Hasher = hasher if hasher is not None else default_hash

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def _path_for(self, fingerprint: bytes) -> Path:
        """Map a fingerprint to its on-disk cache file path."""
        hex_fp = fingerprint.hex()
        # Two levels of 2-char sharding to keep dir entries < 65k.
        return self._cache_dir / hex_fp[:2] / hex_fp[2:4] / f"{hex_fp[4:]}.cache"

    # ----- low-level: raw fingerprint → bytes ----------------------------

    def get(self, fingerprint: bytes) -> tuple[Any, bool]:
        """Look up a value. Returns `(value, hit)`."""
        path = self._path_for(fingerprint)
        if not path.exists():
            return None, False
        try:
            with path.open("rb") as f:
                magic = f.read(len(_MAGIC))
                if magic != _MAGIC:
                    return None, False
                payload = f.read()
            return pickle.loads(payload), True
        except (OSError, pickle.UnpicklingError, EOFError):
            # Treat corruption as a miss — the next put will overwrite.
            return None, False

    def put(self, fingerprint: bytes, value: Any) -> None:
        """Store a value. Atomic: temp-file + rename."""
        path = self._path_for(fingerprint)
        path.parent.mkdir(parents=True, exist_ok=True)
        # NamedTemporaryFile in the same directory so rename(2) is atomic
        # (POSIX requires same-filesystem rename).
        try:
            payload = pickle.dumps(value, protocol=pickle.DEFAULT_PROTOCOL)
        except (pickle.PicklingError, TypeError, AttributeError):
            return
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=path.name + ".",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp.write(_MAGIC)
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        try:
            os.replace(tmp_path, path)  # atomic on POSIX
        except OSError:
            # Best-effort cleanup; another process may have raced us.
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()

    def has(self, fingerprint: bytes) -> bool:
        return self._path_for(fingerprint).exists()

    def delete(self, fingerprint: bytes) -> None:
        path = self._path_for(fingerprint)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()

    def clear(self) -> None:
        """Remove all cache entries (but keep the directory tree)."""
        if not self._cache_dir.exists():
            return
        for sub in self._cache_dir.rglob("*.cache"):
            with contextlib.suppress(FileNotFoundError):
                sub.unlink()

    # ----- high-level: hash + cache + compute ----------------------------

    def hash(self, value: Any) -> bytes:
        """Apply the configured Hasher to `value`."""
        return self._hasher(value)

    def compute_or_cached(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        effect: Effect | None = None,
    ) -> tuple[Any, bool]:
        """Compute `fn(*args, **kwargs)`, hitting the cache if possible.

        Effect-aware: if `effect.is_cacheable` is False, runs `fn` directly
        without consulting or writing to the cache. The function's source
        and the input fingerprints together form the cache key.

        Args:
            fn: The function to compute.
            args: Positional arguments. Each is fingerprinted with the configured
                hasher and contributes to the cache key.
            kwargs: Keyword arguments. Same hashing treatment.
            effect: Optional effect tag from `dagron.Effect`. If None, defaults
                to caching (treats fn as PURE).

        Returns:
            `(value, hit)` — `hit` is True if served from cache.
        """
        kwargs = kwargs or {}

        # Effect gate: skip cache entirely for impure tasks.
        if effect is not None and not effect.is_cacheable:
            return fn(*args, **kwargs), False

        input_fps = [self._hasher(a) for a in args] + [
            self._hasher((k, v)) for k, v in sorted(kwargs.items())
        ]
        fp = fingerprint_node(fn, effect, input_fps)

        cached, hit = self.get(fp)
        if hit:
            return cached, True
        value = fn(*args, **kwargs)
        self.put(fp, value)
        return value, False


# Detect numpy at import; not required, but lets us wire the numpy hasher
# automatically when the user opts in.
def has_numpy() -> bool:
    """Return True if numpy is importable in this interpreter."""
    return inspect.ismodule(sys.modules.get("numpy")) or _try_import_numpy()


def _try_import_numpy() -> bool:
    try:
        import numpy  # noqa: F401
    except ImportError:
        return False
    return True


__all__ = [
    "ContentCache",
    "Hasher",
    "default_cache_dir",
    "default_hash",
    "fingerprint_function",
    "fingerprint_node",
    "has_numpy",
    "numpy_hash",
]
