"""Base protocol for distributed execution backends."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DistributedBackend(Protocol):
    """Protocol that all distributed backends must implement.

    Backends wrap different concurrency/distribution primitives
    (threads, processes, Ray, Celery, etc.) behind a uniform interface.
    """

    @property
    def name(self) -> str:
        """Human-readable name of the backend."""
        ...

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Submit a callable for execution.

        Args:
            fn: The callable to execute.
            *args: Positional arguments for the callable.
            **kwargs: Keyword arguments for the callable.

        Returns:
            A future-like object whose result can be retrieved via
            :meth:`result`.
        """
        ...

    def result(self, future: Any, timeout: float | None = None) -> Any:
        """Retrieve the result of a submitted task.

        Args:
            future: The future returned by :meth:`submit`.
            timeout: Optional timeout in seconds.

        Returns:
            The return value of the callable.

        Raises:
            Exception: If the callable raised.
            TimeoutError: If the timeout was exceeded.
        """
        ...

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the backend and release resources.

        Args:
            wait: If True, wait for all pending tasks to finish.
        """
        ...
