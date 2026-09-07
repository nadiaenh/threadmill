"""Windmill deployed Python jobs, four outstanding HTTP jobs at a time.

End-to-end measurements include POST, database queue, Python execution and
result polling. Polling is every 10 ms; it adds observation latency. Setup,
deployment and warmup are excluded. No dependency installation is requested.
"""
import concurrent.futures
import json
import time
import urllib.error
import urllib.request


class Backend:
    def __init__(self, base_url="http://127.0.0.1:18081", poll_interval=0.01):
        self.base_url = base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.token = None
        self.workspace = "admins"
        self.path = "u/admin/threadmill_benchmark_" + str(time.time_ns())

    def request(self, path, body=None):
        headers = {}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base_url + "/api" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                raw = response.read().decode()
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{path}: HTTP {error.code}: {error.read().decode()}") from error
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def setup(self):
        deadline = time.monotonic() + 180
        while True:
            try:
                self.version = self.request("/version")
                break
            except (OSError, RuntimeError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        self.token = self.request("/auth/login", {
            "email": "admin@windmill.dev", "password": "changeme"
        })
        script_hash = self.request(f"/w/{self.workspace}/scripts/create", {
            "path": self.path,
            "summary": "Threadmill local benchmark, trusted synthetic workload",
            "content": "import time\n\ndef main(value: int = 41, delay: float = 0.0):\n    if delay:\n        time.sleep(delay)\n    return value + 1\n",
            "language": "python3",
            "lock": "",
        })
        # Script creation schedules dependency resolution. Run-by-path only
        # resolves successfully deployed versions, so wait outside timed batches.
        deadline = time.monotonic() + 240
        while True:
            status = self.request(f"/w/{self.workspace}/scripts/deployment_status/h/{script_hash}")
            if status.get("lock_error_logs"):
                raise RuntimeError(f"Windmill dependency resolution failed: {status['lock_error_logs']}")
            if status.get("lock") is not None:
                self.dependency_lock = status["lock"]
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Windmill deployment not ready: {status}")
            time.sleep(.2)
        # Multiple blocking jobs distribute warmup across the four workers.
        self.batch(8, 0.1)
        return {"version": self.version, "workers": 4,
                "poll_interval_seconds": self.poll_interval,
                "mode": "community standard Python workers; deployed scripts",
                "dependency_lock": self.dependency_lock}

    def run_one(self, delay):
        job_id = self.request(f"/w/{self.workspace}/jobs/run/p/{self.path}", {"value": 41, "delay": delay})
        deadline = time.monotonic() + 180
        while True:
            result = self.request(f"/w/{self.workspace}/jobs_u/completed/get_result_maybe/{job_id}")
            if result["completed"]:
                if result.get("success") is False or result["result"] != 42:
                    raise RuntimeError(f"Windmill job failed: {result}")
                return job_id
            if time.monotonic() > deadline:
                raise TimeoutError(f"Windmill job {job_id} did not finish")
            time.sleep(self.poll_interval)

    def batch(self, jobs, delay):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            start = time.perf_counter()
            results = list(pool.map(self.run_one, [delay] * jobs))
            elapsed = time.perf_counter() - start
        # Internal job timing fetched after the end-to-end stopwatch stops.
        timings = [self.request(f"/w/{self.workspace}/jobs_u/completed/get_timing/{identifier}")
                   for identifier in results]
        return {"elapsed_seconds": elapsed, "verified": len(results),
                "internal_timings": timings}
