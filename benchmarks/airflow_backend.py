"""Airflow LocalExecutor orchestration benchmark; setup/verification untimed."""
import json
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class Backend:
    def __init__(self, base_url="http://127.0.0.1:18082"):
        self.base_url = base_url.rstrip("/")
        self.token = None
        self.dag_path = "/api/v2/dags/threadmill_benchmark"

    def request(self, path, method="GET", body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(self.base_url + path, method=method, headers=headers,
                          data=None if body is None else json.dumps(body).encode())
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            detail = exc.read().decode()
            raise RuntimeError(f"Airflow {method} {path}: HTTP {exc.code}: {detail}") from exc

    def setup(self):
        deadline = time.monotonic() + 240
        last_error = None
        while time.monotonic() < deadline:
            try:
                self.token = self.request("/auth/token")["access_token"]
                dag = self.request(self.dag_path)
                if dag.get("is_paused"):
                    self.request(self.dag_path, "PATCH", {"is_paused": False})
                return {"backend": "airflow", "executor": "LocalExecutor", "version": "3.3.1", "workers": 4}
            except (RuntimeError, URLError, TimeoutError, OSError) as exc:
                last_error = exc
                time.sleep(2)
        raise TimeoutError(f"Airflow not ready: {last_error}")

    def batch(self, jobs, delay):
        if jobs != 20:
            raise ValueError("Airflow benchmark DAG contains exactly 20 tasks; use jobs=20")
        run_id = "bench_" + uuid.uuid4().hex
        run_path = self.dag_path + "/dagRuns/" + run_id
        start = time.perf_counter()
        self.request(self.dag_path + "/dagRuns", "POST", {
            "dag_run_id": run_id, "logical_date": None, "conf": {"delay": delay, "value": 41}
        })
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            run = self.request(run_path)
            if run["state"] in ("success", "failed"):
                elapsed = time.perf_counter() - start
                break
            time.sleep(0.05)
        else:
            raise TimeoutError(f"Airflow run {run_id} exceeded 300 seconds")
        if run["state"] != "success":
            raise RuntimeError(f"Airflow run failed: {run}")
        instances = self.request(run_path + "/taskInstances?limit=100")["task_instances"]
        if len(instances) != jobs or any(t["state"] != "success" for t in instances):
            raise AssertionError(f"Expected {jobs} successful tasks: {instances}")
        for index in range(jobs):
            entry = self.request(run_path + f"/taskInstances/job_{index:02d}/xcomEntries/return_value")
            value = entry["value"]
            if isinstance(value, str):
                value = json.loads(value)
            if type(value) is not int or value != 42:
                raise AssertionError(f"Unexpected XCom return for job_{index:02d}: {entry}")
        return {
            "elapsed_seconds": elapsed,
            "verified": jobs,
            "run_id": run_id,
            "dag_duration_seconds": run.get("duration"),
            "task_duration_seconds": [t.get("duration") for t in instances],
            "poll_interval_seconds": 0.05,
            "timing_scope": "trigger POST through observed successful DAG completion; XCom verification excluded",
        }
