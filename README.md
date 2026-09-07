![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)

**threadmill** — lightweight background jobs on dedicated Python threads, in one process.

Separate worker lanes keep batch jobs from occupying interactive workers. Bounded admission, copied JSON inputs and outputs, cooperative deadlines, and managed-buffer budgets keep trusted jobs predictable. No runtime dependencies.

## One-time setup

Requires Python 3.12 or newer. From this directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

The demo and tests also run directly from the checkout without installation. The optional Windmill/Airflow comparison requires Docker Compose and a running Docker engine with about 8 GiB available.

## Main usage commands

```sh
python demo.py
python -m unittest discover -s tests -v
python benchmark.py --jobs 1000
python benchmark.py --jobs 200 --sleep 0.005
```

Submit a function and JSON-compatible arguments:

```python
from threadmill import Lane, Scheduler


def task(ctx, payload):
    with ctx.buffer(1024):
        ctx.sleep(0.01)
        return {"answer": payload["value"] * 2}


with Scheduler({"interactive": Lane(workers=2, capacity=16)}) as scheduler:
    job = scheduler.submit(
        task, {"value": 21}, lane="interactive", timeout=1, memory_bytes=4096
    )
    print(job.result(timeout=2))  # {"answer": 42}
```

`capacity` counts queued and running jobs; a full lane raises `Overloaded`. `job.cancel()` requests cancellation. Jobs observe cancellation and runtime deadlines through `ctx.checkpoint()` or `ctx.sleep()`; `job.result(timeout=...)` only limits the caller's wait. Exiting the scheduler context drains accepted work.

Use this for **trusted I/O-oriented jobs**. Buffers allocated through `ctx.buffer()` are accounted for, but ordinary allocations, globals, and native code still share the process. Deadlines cannot kill a stuck thread. Jobs are in memory, with no persistence or crash recovery. CPU-bound Python does not gain multicore execution on a normal GIL-enabled build.

To reproduce the local service comparison:

```sh
python benchmarks/local/run.py
```

The [benchmark report](benchmarks/RESULTS.md) includes measured results, raw data, and differences in execution guarantees. It compares local standard Windmill workers and Airflow LocalExecutor with the Threadmill HTTP adapter.

## Demo

The demo occupies both batch workers, completes an interactive job on its own lane, then releases the batch jobs. It verifies that every job shares the caller's PID and demonstrates an expected budget rejection. Worker assignment may vary between runs.

![Threadmill demo: dedicated lanes, shared process, managed-buffer budget](docs/demo.svg)
