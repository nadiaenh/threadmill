"""Run to see lane separation and cooperative memory budgets: python demo.py."""
import os
import threading

from threadmill import BudgetExceeded, Lane, Scheduler


def background(ctx, payload):
    with ctx.buffer(4096) as scratch:
        scratch[0] = 42
        ctx.sleep(0.01)
        return {
            "name": payload["name"],
            "pid": os.getpid(),
            "worker": threading.current_thread().name.removeprefix("threadmill-"),
            "answer": scratch[0],
        }


def main():
    ready = threading.Barrier(3)
    release = threading.Event()

    def batch(ctx, payload):
        if payload["index"] < 2:
            ready.wait(timeout=2)
        while not release.is_set():
            ctx.sleep(0.001)
        return background(ctx, payload)

    def show(job):
        result = job.result(timeout=2)
        if result["pid"] != os.getpid() or result["answer"] != 42:
            raise RuntimeError("Unexpected job result")
        print(f"{result['name']:<14} {result['worker']:<16} "
              f"{result['answer']:<8} {'yes':<10} {job.peak_managed_bytes // 1024} KiB")

    print("threadmill | background jobs in one process\n")
    print(f"{'JOB':<14} {'WORKER':<16} {'RESULT':<8} {'SAME PID':<10} BUFFER")
    print("-" * 61)
    with Scheduler({"interactive": Lane(1, 8), "batch": Lane(2, 8)}) as scheduler:
        jobs = [scheduler.submit(batch, {"name": f"batch-{i}", "index": i},
                                 lane="batch", timeout=5, memory_bytes=8192)
                for i in range(4)]
        try:
            ready.wait(timeout=2)
            quick = scheduler.submit(background, {"name": "interactive"},
                                     lane="interactive", timeout=1)
            show(quick)
            if any(job.done() for job in jobs):
                raise RuntimeError("Batch jobs should still be waiting")
            print("Interactive finished while both batch workers were occupied.\n")
        finally:
            release.set()
        for job in jobs:
            show(job)
        budget = scheduler.submit(background, {"name": "over-budget"},
                                  lane="batch", memory_bytes=32)
        try:
            budget.result(timeout=2)
        except BudgetExceeded:
            print("\nBudget check: 4 KiB requested / 32 B allowed -> rejected (expected)")
        else:
            raise RuntimeError("Expected the managed-buffer budget to reject this job")
    print("All jobs verified. Worker threads shut down.")


if __name__ == "__main__":
    main()
