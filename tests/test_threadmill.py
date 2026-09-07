import contextvars
import os
import threading
import time
import unittest
from concurrent.futures import CancelledError

from threadmill import BudgetExceeded, JobCancelled, Lane, Overloaded, Scheduler


class SchedulerTests(unittest.TestCase):
    def test_concurrent_threads_share_caller_pid(self):
        barrier = threading.Barrier(3)
        def work(ctx, payload):
            barrier.wait(timeout=2)
            return [os.getpid(), threading.get_ident()]
        with Scheduler({"default": Lane(2, 2)}) as scheduler:
            jobs = [scheduler.submit(work) for _ in range(2)]
            barrier.wait(timeout=2)
            results = [job.result(2) for job in jobs]
        self.assertEqual({row[0] for row in results}, {os.getpid()})
        self.assertEqual(len({row[1] for row in results}), 2)

    def test_lane_bulkhead_and_outstanding_capacity(self):
        release = threading.Event()
        entered = threading.Event()
        def blocked(ctx, payload):
            entered.set()
            release.wait(2)
        with Scheduler({"batch": Lane(1, 2), "interactive": Lane(1, 1)}) as scheduler:
            try:
                first = scheduler.submit(blocked, lane="batch")
                self.assertTrue(entered.wait(2))
                second = scheduler.submit(blocked, lane="batch")
                with self.assertRaises(Overloaded):
                    scheduler.submit(blocked, lane="batch")
                self.assertEqual(scheduler.submit(lambda c, p: 42, lane="interactive").result(2), 42)
            finally:
                release.set()
            first.result(2)
            second.result(2)

    def test_payload_and_context_are_per_job(self):
        variable = contextvars.ContextVar("test_job_context", default="clean")
        variable.set("caller")
        payload = {"items": [1]}
        def change(ctx, data):
            self.assertEqual(variable.get(), "clean")
            variable.set("dirty")
            data["items"].append(2)
            return data
        with Scheduler({"default": Lane(1, 2)}) as scheduler:
            job = scheduler.submit(change, payload)
            first = job.result(2)
            first["items"].append(3)
            self.assertEqual(job.result(2), {"items": [1, 2]})
            self.assertEqual(scheduler.submit(lambda c, p: variable.get()).result(2), "clean")
        self.assertEqual(payload, {"items": [1]})
        self.assertEqual(variable.get(), "caller")

    def test_managed_buffer_budget_and_release(self):
        def work(ctx, payload):
            with ctx.buffer(6) as data:
                self.assertEqual(len(data), 6)
                with self.assertRaises(BudgetExceeded):
                    with ctx.buffer(5):
                        pass
            with ctx.buffer(10):
                pass
            return "ok"
        with Scheduler() as scheduler:
            job = scheduler.submit(work, memory_bytes=10)
            self.assertEqual(job.result(2), "ok")
            self.assertEqual(job.peak_managed_bytes, 10)

    def test_invalid_payload_releases_admission_and_result_cap(self):
        with Scheduler({"default": Lane(1, 1)}, payload_bytes=16) as scheduler:
            with self.assertRaises(BudgetExceeded):
                scheduler.submit(lambda c, p: p, "x" * 20)
            with self.assertRaises(ValueError):
                scheduler.submit(lambda c, p: p, float("nan"))
            with self.assertRaises(BudgetExceeded):
                scheduler.submit(lambda c, p: "x" * 20).result(2)

    def test_running_cancellation_keeps_slot_until_return(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(ctx, payload):
            entered.set()
            release.wait(2)
        with Scheduler({"default": Lane(1, 1)}) as scheduler:
            try:
                job = scheduler.submit(blocked)
                self.assertTrue(entered.wait(2))
                self.assertFalse(job.cancel())
                self.assertFalse(job.done())
                with self.assertRaises(Overloaded):
                    scheduler.submit(blocked)
            finally:
                release.set()
            with self.assertRaises(JobCancelled):
                job.result(2)

    def test_deadline_does_not_release_noncooperative_job(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(ctx, payload):
            entered.set()
            release.wait(2)
        with Scheduler({"default": Lane(1, 1)}) as scheduler:
            try:
                job = scheduler.submit(blocked, timeout=0.02)
                self.assertTrue(entered.wait(2))
                time.sleep(0.04)
                self.assertFalse(job.done())
                with self.assertRaises(Overloaded):
                    scheduler.submit(blocked)
            finally:
                release.set()
            with self.assertRaises(TimeoutError):
                job.result(2)

    def test_cooperative_sleep_observes_cancellation(self):
        entered = threading.Event()
        def work(ctx, payload):
            entered.set()
            ctx.sleep(10)
        with Scheduler() as scheduler:
            job = scheduler.submit(work)
            self.assertTrue(entered.wait(2))
            job.cancel()
            with self.assertRaises(JobCancelled):
                job.result(2)

    def test_queued_cancel_prevents_invocation(self):
        entered, release, invoked = threading.Event(), threading.Event(), threading.Event()
        def blocked(ctx, payload):
            entered.set()
            release.wait(2)
        with Scheduler({"default": Lane(1, 2)}) as scheduler:
            try:
                first = scheduler.submit(blocked)
                self.assertTrue(entered.wait(2))
                second = scheduler.submit(lambda c, p: invoked.set())
                self.assertTrue(second.cancel())
            finally:
                release.set()
            first.result(2)
            with self.assertRaises(CancelledError):
                second.result(2)
        self.assertFalse(invoked.is_set())

    def test_exception_does_not_destroy_worker(self):
        def fail(ctx, payload):
            raise ValueError("expected failure")
        with Scheduler({"default": Lane(1, 2)}) as scheduler:
            failed = scheduler.submit(fail)
            next_job = scheduler.submit(lambda c, p: "alive")
            with self.assertRaisesRegex(ValueError, "expected failure"):
                failed.result(2)
            self.assertEqual(next_job.result(2), "alive")

    def test_completion_releases_capacity_for_next_job(self):
        with Scheduler({"default": Lane(1, 1)}) as scheduler:
            for value in range(100):
                self.assertEqual(scheduler.submit(lambda c, p: p, value).result(2), value)

    def test_chained_exception_tracebacks_are_cleared(self):
        def fail(ctx, payload):
            try:
                raise ValueError("inner")
            except ValueError as cause:
                raise RuntimeError("outer") from cause
        with Scheduler() as scheduler:
            job = scheduler.submit(fail, {"retained": [1, 2, 3]})
            try:
                job.result(2)
            except RuntimeError as error:
                self.assertIsNotNone(error.__cause__)
                self.assertIsNone(error.__cause__.__traceback__)
                self.assertIsNone(error.__context__.__traceback__)
            else:
                self.fail("expected RuntimeError")

    def test_invalid_lane_configuration(self):
        with self.assertRaises(ValueError):
            Lane(1.5, 2)
        with self.assertRaises(TypeError):
            Scheduler({"valid": Lane(), "invalid": None})

    def test_shutdown_drains_rejects_and_is_repeatable(self):
        scheduler = Scheduler({"default": Lane(1, 3)})
        jobs = [scheduler.submit(lambda c, p: p, value) for value in range(3)]
        scheduler.close(wait=False)
        with self.assertRaises(RuntimeError):
            scheduler.submit(lambda c, p: p)
        scheduler.close()
        scheduler.close()
        self.assertEqual([job.result(2) for job in jobs], [0, 1, 2])


if __name__ == "__main__":
    unittest.main()
