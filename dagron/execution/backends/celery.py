"""Celery-based distributed backend (optional dependency)."""

from __future__ import annotations

from typing import Any


class CeleryBackend:
    """Backend that dispatches tasks via Celery.

    Requires ``celery`` to be installed.  Install with::

        pip install dagron[celery]

    Args:
        app: A Celery application instance.
        queue: Optional queue name for task routing.
    """

    def __init__(self, app: Any = None, queue: str | None = None) -> None:
        try:
            import celery
        except ImportError:
            raise ImportError(
                "Celery is required for CeleryBackend. Install with: pip install dagron[celery]"
            ) from None

        self._celery = celery
        self._app = app
        self._queue = queue

    @property
    def name(self) -> str:
        return "celery"

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        celery_task = self._app.task(fn)
        options: dict[str, Any] = {}
        if self._queue:
            options["queue"] = self._queue
        return celery_task.apply_async(args=args, kwargs=kwargs, **options)

    def result(self, future: Any, timeout: float | None = None) -> Any:
        return future.get(timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        pass
