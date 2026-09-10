"""Compare threadmill against stdlib executors on equal JSON-roundtrip jobs."""
import argparse
import concurrent.futures as cf
import json
import multiprocessing
import platform
import statistics
import time

from threadmill import Lane, Scheduler


def work(ctx, payload):
    if payload["sleep"]:
        time.sleep(payload["sleep"])
    return payload["value"] + 1


def wire_work(data):
    return json.dumps(work(None, json.loads(data)), separators=(",", ":")).encode()


def measure(kind, workers, jobs, delay, repeats):
    start = time.perf_counter()
    payload = {"sleep": delay, "value": 41}
    if kind == "threadmill":
        pool = Scheduler({"default": Lane(workers, max(jobs, workers * 4) + workers)})
        submit = lambda: pool.submit(work, payload)
        close = pool.close
    else:
        cls = {"threads": cf.ThreadPoolExecutor, "processes": cf.ProcessPoolExecutor,
               "interpreters": getattr(cf, "InterpreterPoolExecutor", None)}[kind]
        kwargs = {"mp_context": multiprocessing.get_context("spawn")} if kind == "processes" else {}
        pool = cls(max_workers=workers, **kwargs)
        submit = lambda: pool.submit(wire_work, json.dumps(payload, allow_nan=False, separators=(",", ":")).encode())
        close = pool.shutdown
    try:
        # Warmup may not force every lazy executor worker to start for tiny jobs.
        for future in [submit() for _ in range(workers * 4)]:
            future.result()
        startup = time.perf_counter() - start
        samples = []
        for _ in range(repeats):
            start = time.perf_counter()
            futures = [submit() for _ in range(jobs)]
            for future in futures:
                result = future.result()
                assert (result if kind == "threadmill" else json.loads(result)) == 42
            samples.append(time.perf_counter() - start)
        median = statistics.median(samples)
        return {"backend": kind, "startup_plus_warmup_ms": round(startup * 1000, 3),
                "median_batch_ms": round(median * 1000, 3),
                "jobs_per_second": round(jobs / median),
                "amortized_us_per_job": round(median * 1e6 / jobs, 2)}
    finally:
        close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0)
    args = parser.parse_args()
    if min(args.jobs, args.workers, args.repeats) < 1 or args.sleep < 0:
        parser.error("counts must be positive and sleep nonnegative")
    backends = ["threadmill", "threads", "processes"]
    if hasattr(cf, "InterpreterPoolExecutor"):
        backends.append("interpreters")
    print(json.dumps({"python": platform.python_version(), "platform": platform.platform(),
                      "parameters": vars(args),
                      "results": [measure(k, args.workers, args.jobs, args.sleep, args.repeats)
                                  for k in backends]}, indent=2))


if __name__ == "__main__":
    main()
