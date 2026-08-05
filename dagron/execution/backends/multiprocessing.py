"""Multiprocessing-based distributed backend using ProcessPoolExecutor."""

from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any


class MultiprocessingBackend:
    """Backend that dispatches tasks to a :class:`ProcessPoolExecutor`.

    Tasks and their arguments must be picklable.

    Args:
        max_workers: Maximum number of worker processes.
            Defaults to the number of CPUs.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._max_workers = max_workers
        self._pool: ProcessPoolExecutor | None = None

    def _ensure_pool(self) -> ProcessPoolExecutor:
        if self._pool is None:
            self._pool = ProcessPoolExecutor(max_workers=self._max_workers)
        return self._pool

    @property
    def name(self) -> str:
        return "multiprocessing"

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Future[Any]:
        pool = self._ensure_pool()
        return pool.submit(fn, *args, **kwargs)

    def result(self, future: Future[Any], timeout: float | None = None) -> Any:
        return future.result(timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=wait)
            self._pool = None
