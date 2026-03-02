"""Pluggable backends for distributed execution."""

from dagron.execution.backends.base import DistributedBackend
from dagron.execution.backends.celery import CeleryBackend
from dagron.execution.backends.multiprocessing import MultiprocessingBackend
from dagron.execution.backends.ray import RayBackend
from dagron.execution.backends.thread import ThreadBackend

__all__ = [
    "CeleryBackend",
    "DistributedBackend",
    "MultiprocessingBackend",
    "RayBackend",
    "ThreadBackend",
]
