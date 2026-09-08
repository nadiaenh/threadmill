"""Trusted-code background jobs. Resource budgets are cooperative, not a sandbox."""
from __future__ import annotations

import contextvars
import json
import math
import queue
import threading
import time
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable


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
    """Null tracebacks across the cause/context chain and any groups.

    Idle workers keep the last exception reachable; its frames would pin user
    objects. Exception attributes can still hold user objects: trusted code only.
    """
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
    """Use checkpoint/sleep/buffer for cooperative job resource control."""

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
        """Account live buffers allocated through this API only; do not retain them."""
        self.checkpoint()
        if size < 0 or self._used + size > self._budget:
            raise BudgetExceeded("managed buffer budget exceeded")
        self._used += size
        self.peak_managed_bytes = max(self.peak_managed_bytes, self._used)
        try:
            yield bytearray(size)
        finally:
            self._used -= size


class Job:
    def __init__(self):
        self._future = Future()
        self._stop = threading.Event()
        self.submitted_at = time.monotonic()
        self.started_at = None
        self.finished_at = None
        self.peak_managed_bytes = 0

    def cancel(self):
        """Request cancellation; True means invocation was prevented."""
        self._stop.set()
        return self._future.cancel()

    def result(self, timeout=None):
        """Waiting timeout does not cancel the job. Returns a fresh JSON value."""
        return json.loads(self._future.result(timeout))

    def done(self):
        return self._future.done()


class Scheduler:
    def __init__(self, lanes=None, *, payload_bytes=1_048_576):
        if payload_bytes < 1:
            raise ValueError("payload_bytes must be positive")
        lanes = {"default": Lane()} if lanes is None else lanes
        if not lanes:
            raise ValueError("at least one lane required")
        if any(not isinstance(config, Lane) for config in lanes.values()):
            raise TypeError("lane values must be Lane instances")
        self._limit = payload_bytes
        self._lock = threading.Lock()
        self._closed = False
        self._lanes = {}
        self._configs = dict(lanes)
        self._threads = []
        for name, config in lanes.items():
            q = queue.Queue()  # admission semaphore bounds entries, including running
            slots = threading.BoundedSemaphore(config.capacity)
            self._lanes[name] = (q, slots)
            for index in range(config.workers):
                thread = threading.Thread(target=self._worker, args=(q, slots),
                                          name=f"threadmill-{name}-{index}")
                self._threads.append(thread)
                thread.start()

    def submit(self, function: Callable, payload=None, *, lane="default",
               timeout=None, memory_bytes=1_048_576):
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be finite and positive")
        if memory_bytes < 0:
            raise ValueError("memory_bytes must be nonnegative")
        # Serialize within admission, ensuring overload cannot accumulate copied payloads.
        with self._lock:
            if self._closed:
                raise RuntimeError("scheduler is closed")
            q, slots = self._lanes[lane]
            if not slots.acquire(blocking=False):
                raise Overloaded(f"lane {lane!r} is full")
            try:
                data = _encode(payload, self._limit)
                job = Job()
                q.put((job, function, data, timeout, memory_bytes))
                return job
            except BaseException:
                slots.release()
                raise

    def _worker(self, q, slots):
        while True:
            item = q.get()
            if item is None:
                q.task_done()
                return
            job, function, data, timeout, memory_bytes = item
            running = job._future.set_running_or_notify_cancel()
            result = error = None
            if running:
                job.started_at = time.monotonic()
                ctx = JobContext(job._stop, timeout, memory_bytes)
                try:
                    def invoke():
                        ctx.checkpoint()
                        value = function(ctx, json.loads(data))
                        ctx.checkpoint()
                        encoded = _encode(value, self._limit)
                        ctx.checkpoint()
                        return encoded
                    result = contextvars.Context().run(invoke)
                except BaseException as exc:
                    _scrub(exc)
                    error = exc
                finally:
                    job.peak_managed_bytes = ctx.peak_managed_bytes
                    del ctx, invoke
            job.finished_at = time.monotonic()
            slots.release()
            q.task_done()
            if running:
                if error is None:
                    job._future.set_result(result)
                else:
                    job._future.set_exception(error)
            item = job = function = data = result = error = None

    def close(self, wait=True):
        if wait and threading.current_thread() in self._threads:
            raise RuntimeError("a worker cannot join its own scheduler")
        with self._lock:
            if not self._closed:
                self._closed = True
                for name, (q, _) in self._lanes.items():
                    for _ in range(self._configs[name].workers):
                        q.put(None)
        if wait:
            for thread in self._threads:
                thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
