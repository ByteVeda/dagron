"""Thread-based distributed backend using ThreadPoolExecutor."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any


class ThreadBackend:
    """Backend that dispatches tasks to a :class:`ThreadPoolExecutor`.

    Suitable for I/O-bound workloads and testing.

    Args:
        max_workers: Maximum number of threads. Defaults to the
            ``ThreadPoolExecutor`` default (typically ``min(32, os.cpu_count() + 4)``).
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers = max_workers
        self._pool: ThreadPoolExecutor | None = None

    def _ensure_pool(self) -> ThreadPoolExecutor:
        if self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._pool

    @property
    def name(self) -> str:
        return "thread"

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Future[Any]:
        pool = self._ensure_pool()
        return pool.submit(fn, *args, **kwargs)

    def result(self, future: Future[Any], timeout: float | None = None) -> Any:
        return future.result(timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=wait)
            self._pool = None
