# Local Windmill and Airflow comparison

Measured September 7, 2026. These are actual local deployments, not estimates from Python process-pool timings.

## Result

**Threadmill has substantially lower end-to-end overhead for these tiny trusted jobs. This does not establish equivalent features, maximum throughput, or overall product superiority.**

Each cell is the median time to complete **20 independent jobs**, with four execution slots, over three measured batches after warmup.

| Workload | Threadmill HTTP | Windmill CE 1.804.0 | Airflow 3.3.1 LocalExecutor |
| --- | ---: | ---: | ---: |
| Return 41 + 1 | 18.6 ms | 230.9 ms | 5501.6 ms |
| Sleep 5 ms, return 41 + 1 | 84.7 ms | 270.4 ms | 5739.8 ms |

| Workload | Windmill batch time / Threadmill | Airflow batch time / Threadmill |
| --- | ---: | ---: |
| Tiny | 12.4× | 295.7× |
| 5 ms simulated I/O | 3.2× | 67.8× |

Ratios describe these batch completion times through the configured submission paths. They are not claims that a Python calculation itself runs hundreds of times faster. The 5 ms sleep is simulated I/O, not a real network/storage integration. Long jobs, CPU-bound Python, larger payloads, and DAG dependencies were not measured.

## What was actually measured

- All systems ran sequentially in the same Docker Desktop Linux ARM64 VM: 8 virtual CPUs, 7.65 GiB memory. Other benchmark stacks were stopped during each measurement.
- Threadmill: Python 3.12.8, four scheduler threads, in-memory jobs, a minimal HTTP adapter. Windmill: four standard Python worker containers plus server and PostgreSQL 16. Its resolved dependency lock selects Python 3.12. Airflow: Python 3.12 image, PostgreSQL 16, API server, DAG processor, scheduler with LocalExecutor parallelism four.
- Threadmill and Windmill use four closed-loop HTTP clients: each submits a job, retrieves its result, then submits the next. Both poll at 10 ms while pending. This limits offered load and can leave execution slots idle; it is not a saturation/max-throughput test.
- Airflow receives one DAG-run trigger containing 20 independent tasks, with max_active_tasks four. The timer includes scheduling and DAG finalization, observed by 50 ms polling. It can internally queue all 20 tasks, unlike the closed-loop submission pattern.
- Threadmill/Windmill timings include result retrieval and verification. Airflow timing stops when the DAG reports success; all 20 task states and XCom return values are then verified outside the timer. This difference favors Airflow relative to timing all XCom fetches.
- Every measured job returned integer 42. Three batches per workload per backend means 360 verified measured jobs, plus warmups. Setup, script deployment/dependency resolution, image pulls, and warmups are excluded.
- These are warm batch measurements with fixed workload order, three samples, and no confidence intervals. All individual samples and image digests are retained below. Startup-plus-setup fields include different initialization and warmup work and should not be compared as cold-start results.

The differences are expected: Windmill and Airflow maintain database-backed execution state and run separate Python processes. Threadmill skips durable storage, crash recovery, cross-process isolation, authentication, and most workflow machinery. Threadmill is an embedded trusted-job executor, not a feature-equivalent replacement.

Windmill dedicated workers/shared runners reuse long-lived Python subprocesses, but are Cloud/Enterprise features. They were **not tested**. [Dedicated worker documentation](https://www.windmill.dev/docs/core_concepts/dedicated_workers). Airflow was tested through its actual scheduler and LocalExecutor, **not** `dag.test()`. [LocalExecutor documentation](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/executor/local.html).

## Observed memory footprint

Sum of Docker-reported container memory after the measured batches, including each stack's database and services:

| Stack | Observed memory |
| --- | ---: |
| threadmill | 20.5 MiB |
| windmill | 667.1 MiB |
| airflow | 853.6 MiB |

These are idle/after-batch snapshots, not peak RSS or per-job allocations. Docker memory accounting, database caches, default services and retained job histories differ. The snapshots show deployment footprint only; they provide no evidence of hard memory isolation. Threadmill jobs still share an address space.

## Separating executor cost from HTTP/orchestration

The original direct-call benchmark was also rerun inside the same Linux VM with 1,000 tiny jobs and four workers: Threadmill **9.83 µs/job**, raw threads **7.44 µs/job**, and a warm spawn-process pool **72.40 µs/job** (amortized batch time). The direct Threadmill rate was about 102k jobs/s; the HTTP harness is much slower because it includes client requests, polling, and one-job-at-a-time admission per client. Never compare the direct 102k rate against an orchestrator HTTP rate as if they measured the same boundary.

## Reproduce

Install/start Docker Desktop with about 8 GiB available to its Linux VM, then run from the project root:

```sh
python3 benchmarks/local/run.py
# Or one backend:
python3 benchmarks/local/run.py --backend windmill
```

The harness uses namespaced benchmark Compose projects, loopback-only ports 18080/18081/18082, and synthetic credentials. It starts and stops each stack sequentially and retains its benchmark database/cache volumes for reruns. Images download automatically if missing. Successful reruns replace the corresponding raw result JSON. Do not run a second copy concurrently or reuse these benchmark project names for other work.

This harness is for disposable local benchmarking; the Threadmill HTTP wrapper is not a production API. The Airflow DAG contains exactly 20 tasks. Current Compose tags are pinned to application versions; recorded resolved image digests identify what this measurement used, including PostgreSQL 16.

## Raw evidence and sources

- [Threadmill samples and Docker memory](local/results/threadmill.json)
- [Windmill samples, internal job timings, resolved lock and memory](local/results/windmill.json)
- [Airflow samples, task durations, run IDs and memory](local/results/airflow.json)
- [Linux direct executor baseline](local/results/executors-linux.json)
- [Runner](local/run.py)
- [Windmill self-host architecture](https://www.windmill.dev/docs/advanced/self_host)
- [Pinned Windmill API schema](https://raw.githubusercontent.com/windmill-labs/windmill/v1.804.0/backend/windmill-api/openapi.yaml)
- [Airflow Docker setup](https://airflow.apache.org/docs/apache-airflow/3.3.1/howto/docker-compose/index.html)
- [Airflow Simple auth manager](https://airflow.apache.org/docs/apache-airflow/3.3.1/core-concepts/auth-manager/simple/index.html)
