"""Ray-based distributed backend (optional dependency)."""

from __future__ import annotations

from typing import Any


class RayBackend:
    """Backend that dispatches tasks via Ray.

    Requires ``ray`` to be installed.  Install with::

        pip install dagron[ray]

    Args:
        num_cpus: Number of CPUs to request from Ray.  Passed to
            ``ray.init()`` if Ray has not been initialised yet.
    """

    def __init__(self, num_cpus: int | None = None) -> None:
        try:
            import ray
        except ImportError:
            raise ImportError(
                "Ray is required for RayBackend. Install with: pip install dagron[ray]"
            ) from None

        self._num_cpus = num_cpus
        self._ray = ray
        if not ray.is_initialized():
            ray.init(num_cpus=num_cpus, ignore_reinit_error=True)

    @property
    def name(self) -> str:
        return "ray"

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        remote_fn = self._ray.remote(fn)
        return remote_fn.remote(*args, **kwargs)

    def result(self, future: Any, timeout: float | None = None) -> Any:
        return self._ray.get(future, timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        if self._ray.is_initialized():
            self._ray.shutdown()
