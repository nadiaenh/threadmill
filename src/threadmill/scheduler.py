"""Scheduler and Job: dedicated worker lanes over one process."""
from __future__ import annotations

import contextvars
import json
import math
import queue
import threading
import time
from concurrent.futures import Future
from typing import Callable

from .job import JobContext, Lane, Overloaded, _encode, _scrub


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
            q = queue.Queue()
            slots = threading.BoundedSemaphore(config.capacity)  # bounds running + queued
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
        # Encode inside admission so overload cannot accumulate copied payloads.
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
