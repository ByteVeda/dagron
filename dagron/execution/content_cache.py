"""Content-addressable caching for DAG execution results."""

from __future__ import annotations

import hashlib
import inspect
import json
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable


@runtime_checkable
class CacheKeyProtocol(Protocol):
    """Protocol for objects that provide their own cache key."""

    def __dagron_cache_key__(self) -> str: ...


@dataclass(frozen=True)
class CachePolicy:
    """Policy for cache eviction."""

    max_entries: int | None = None
    max_size_bytes: int | None = None
    ttl_seconds: float | None = None


@dataclass
class CacheEntryMetadata:
    """Metadata for a cached entry."""

    node_name: str
    cache_key: str
    created_at: float = field(default_factory=time.time)
    size_bytes: int = 0
    hit_count: int = 0
    last_accessed: float = field(default_factory=time.time)


@dataclass
class CacheStats:
    """Statistics for cache operations."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_entries: int = 0
    total_size_bytes: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CacheKeyBuilder:
    """Build Merkle-tree cache keys for DAG nodes.

    Cache key = SHA256(node_name || task_source_hash || sorted(pred_name:pred_result_hash)...)
    """

    @staticmethod
    def hash_task(task_fn: Callable[[], Any]) -> str:
        """Hash a task callable."""
        try:
            source = inspect.getsource(task_fn)
            return hashlib.sha256(source.encode()).hexdigest()[:16]
        except (OSError, TypeError):
            pass
        try:
            code = task_fn.__code__.co_code
            return hashlib.sha256(code).hexdigest()[:16]
        except AttributeError:
            pass
        return hashlib.sha256(repr(task_fn).encode()).hexdigest()[:16]

    @staticmethod
    def hash_value(value: Any) -> str:
        """Hash a result value."""
        if isinstance(value, CacheKeyProtocol):
            return hashlib.sha256(value.__dagron_cache_key__().encode()).hexdigest()[:16]
        try:
            data = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            return hashlib.sha256(data).hexdigest()[:16]
        except (pickle.PicklingError, TypeError, AttributeError):
            return hashlib.sha256(repr(value).encode()).hexdigest()[:16]

    @staticmethod
    def build_key(
        node_name: str,
        task_hash: str,
        predecessor_hashes: dict[str, str],
    ) -> str:
        """Build a content-addressable cache key.

        Args:
            node_name: Name of the node.
            task_hash: Hash of the task callable.
            predecessor_hashes: Dict mapping predecessor names to their result hashes.

        Returns:
            SHA256 hex digest cache key.
        """
        parts = [node_name, task_hash]
        for pred_name in sorted(predecessor_hashes.keys()):
            parts.append(f"{pred_name}:{predecessor_hashes[pred_name]}")
        combined = "||".join(parts)
        return hashlib.sha256(combined.encode()).hexdigest()


class CacheBackend(Protocol):
    """Protocol for cache storage backends."""

    def get(self, key: str) -> tuple[Any, bool]:
        """Get a cached value. Returns (value, found)."""
        ...

    def put(self, key: str, value: Any, metadata: CacheEntryMetadata) -> None:
        """Store a value in the cache."""
        ...

    def has(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        ...

    def delete(self, key: str) -> None:
        """Delete a cached entry."""
        ...

    def clear(self) -> None:
        """Clear all cached entries."""
        ...

    def stats(self) -> CacheStats:
        """Get cache statistics."""
        ...


class FileSystemCacheBackend:
    """File system-based cache backend.

    Stores values as pickle files and metadata as JSON.
    Uses atomic writes and maintains an index for LRU/TTL tracking.

    Args:
        cache_dir: Directory to store cache files.
        policy: Optional cache eviction policy.
    """

    def __init__(
        self,
        cache_dir: str | Path,
        policy: CachePolicy | None = None,
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._policy = policy or CachePolicy()
        self._index: dict[str, CacheEntryMetadata] = {}
        self._stats = CacheStats()
        self._load_index()

    def _index_path(self) -> Path:
        return self._cache_dir / "index.json"

    def _value_path(self, key: str) -> Path:
        return self._cache_dir / f"{key[:16]}.pkl"

    def _load_index(self) -> None:
        idx_path = self._index_path()
        if idx_path.exists():
            try:
                with open(idx_path) as f:
                    raw = json.load(f)
                for key, meta in raw.items():
                    self._index[key] = CacheEntryMetadata(**meta)
            except (json.JSONDecodeError, TypeError):
                self._index = {}

    def _save_index(self) -> None:
        idx_path = self._index_path()
        raw = {}
        for key, meta in self._index.items():
            raw[key] = {
                "node_name": meta.node_name,
                "cache_key": meta.cache_key,
                "created_at": meta.created_at,
                "size_bytes": meta.size_bytes,
                "hit_count": meta.hit_count,
                "last_accessed": meta.last_accessed,
            }
        tmp_path = idx_path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(raw, f)
        tmp_path.replace(idx_path)

    def get(self, key: str) -> tuple[Any, bool]:
        if key not in self._index:
            self._stats.misses += 1
            return None, False

        meta = self._index[key]

        # TTL check
        if (
            self._policy.ttl_seconds is not None
            and time.time() - meta.created_at > self._policy.ttl_seconds
        ):
            self.delete(key)
            self._stats.misses += 1
            return None, False

        value_path = self._value_path(key)
        if not value_path.exists():
            del self._index[key]
            self._stats.misses += 1
            return None, False

        try:
            with open(value_path, "rb") as f:
                value = pickle.load(f)
        except Exception:
            self.delete(key)
            self._stats.misses += 1
            return None, False

        meta.hit_count += 1
        meta.last_accessed = time.time()
        self._stats.hits += 1
        return value, True

    def put(self, key: str, value: Any, metadata: CacheEntryMetadata) -> None:
        self._evict_if_needed()

        value_path = self._value_path(key)
        tmp_path = value_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "wb") as f:
                pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
            tmp_path.replace(value_path)
            metadata.size_bytes = value_path.stat().st_size
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            return

        self._index[key] = metadata
        self._stats.total_entries = len(self._index)
        self._stats.total_size_bytes = sum(m.size_bytes for m in self._index.values())
        self._save_index()

    def has(self, key: str) -> bool:
        if key not in self._index:
            return False
        if self._policy.ttl_seconds is not None:
            meta = self._index[key]
            if time.time() - meta.created_at > self._policy.ttl_seconds:
                return False
        return self._value_path(key).exists()

    def delete(self, key: str) -> None:
        if key in self._index:
            value_path = self._value_path(key)
            if value_path.exists():
                value_path.unlink()
            del self._index[key]
            self._save_index()

    def clear(self) -> None:
        for key in list(self._index.keys()):
            self.delete(key)
        self._stats = CacheStats()

    def stats(self) -> CacheStats:
        self._stats.total_entries = len(self._index)
        self._stats.total_size_bytes = sum(m.size_bytes for m in self._index.values())
        return CacheStats(
            hits=self._stats.hits,
            misses=self._stats.misses,
            evictions=self._stats.evictions,
            total_entries=self._stats.total_entries,
            total_size_bytes=self._stats.total_size_bytes,
        )

    def _evict_if_needed(self) -> None:
        """Evict entries based on policy (LRU)."""
        # Max entries
        if self._policy.max_entries is not None:
            while len(self._index) >= self._policy.max_entries:
                oldest_key = min(self._index, key=lambda k: self._index[k].last_accessed)
                self.delete(oldest_key)
                self._stats.evictions += 1

        # Max size
        if self._policy.max_size_bytes is not None:
            total_size = sum(m.size_bytes for m in self._index.values())
            while total_size > self._policy.max_size_bytes and self._index:
                oldest_key = min(self._index, key=lambda k: self._index[k].last_accessed)
                total_size -= self._index[oldest_key].size_bytes
                self.delete(oldest_key)
                self._stats.evictions += 1


class ContentAddressableCache:
    """High-level content-addressable cache for DAG execution.

    Provides Merkle-tree key propagation: if any upstream changes,
    all downstream keys change automatically (like Bazel/Nix).
    """

    def __init__(self, backend: CacheBackend) -> None:
        self._backend = backend
        self._key_builder = CacheKeyBuilder()

    @property
    def backend(self) -> CacheBackend:
        return self._backend

    def compute_key(
        self,
        node_name: str,
        task_fn: Callable[[], Any],
        predecessor_result_hashes: dict[str, str],
    ) -> str:
        """Compute the cache key for a node."""
        task_hash = self._key_builder.hash_task(task_fn)
        return self._key_builder.build_key(node_name, task_hash, predecessor_result_hashes)

    def get(self, key: str) -> tuple[Any, bool]:
        """Get a cached value."""
        return self._backend.get(key)

    def put(self, key: str, value: Any, node_name: str) -> None:
        """Store a value in the cache."""
        meta = CacheEntryMetadata(
            node_name=node_name,
            cache_key=key,
        )
        self._backend.put(key, value, meta)

    def has(self, key: str) -> bool:
        """Check if a key is cached."""
        return self._backend.has(key)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._backend.clear()

    def stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._backend.stats()
