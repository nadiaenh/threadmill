"""Job primitives: resource budgets are cooperative, not a sandbox."""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass


class Overloaded(RuntimeError):
    pass


class JobCancelled(RuntimeError):
    pass


class BudgetExceeded(RuntimeError):
    pass


def _encode(value, limit):
    data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
    if len(data) > limit:
        raise BudgetExceeded(f"JSON payload exceeds {limit} bytes")
    return data


def _scrub(exc):
    # Null tracebacks across the cause/context chain so idle workers cannot pin
    # user objects through exception frames. Trusted code only.
    pending, seen = [exc], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        current.__traceback__ = None
        pending.extend(e for e in (current.__cause__, current.__context__) if e is not None)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)


@dataclass(frozen=True)
class Lane:
    workers: int = 2
    capacity: int = 32  # running + queued, not queue depth alone

    def __post_init__(self):
        if (type(self.workers) is not int or type(self.capacity) is not int
                or self.workers < 1 or self.capacity < self.workers):
            raise ValueError("require capacity >= workers >= 1")


class JobContext:
    """Cooperative cancellation, deadlines, and managed-buffer accounting."""

    def __init__(self, stop, timeout, memory_bytes):
        self._stop = stop
        self._deadline = None if timeout is None else time.monotonic() + timeout
        self._budget = memory_bytes
        self._used = 0
        self.peak_managed_bytes = 0

    def checkpoint(self):
        if self._stop.is_set():
            raise JobCancelled("job cancellation requested")
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise TimeoutError("cooperative runtime deadline exceeded")

    def sleep(self, seconds):
        end = time.monotonic() + seconds
        while True:
            self.checkpoint()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self._stop.wait(min(remaining, 0.01))

    @contextmanager
    def buffer(self, size):
        # Accounts only buffers allocated through this API; do not retain them.
        self.checkpoint()
        if size < 0 or self._used + size > self._budget:
            raise BudgetExceeded("managed buffer budget exceeded")
        self._used += size
        self.peak_managed_bytes = max(self.peak_managed_bytes, self._used)
        try:
            yield bytearray(size)
        finally:
            self._used -= size
