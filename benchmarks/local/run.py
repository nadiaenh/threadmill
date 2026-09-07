"""Reproduce local end-to-end comparison: python3 run.py [--backend all]."""
import argparse
import datetime
import importlib
import json
import pathlib
import platform
import statistics
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent
PROJECTS = {'threadmill': 'threadmill-local-http', 'windmill': 'threadmill-local-windmill',
            'airflow': 'threadmill-local-airflow'}


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()


def compose(backend, *args):
    return docker('compose', '-p', PROJECTS[backend], '-f', str(ROOT / f'{backend}.compose.yaml'), *args)


def memory_snapshot(backend):
    identifiers = compose(backend, 'ps', '-q').split()
    if not identifiers:
        return []
    raw = docker('stats', '--no-stream', '--format', '{{json .}}', *identifiers)
    return [json.loads(line) for line in raw.splitlines() if line]


def run(backend, repeats, delays):
    output = ROOT / 'results'
    output.mkdir(exist_ok=True)
    result = {'backend': backend, 'date_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'host_python': platform.python_version(), 'jobs_per_batch': 20, 'execution_slots': 4,
              'repeats': repeats, 'docker': json.loads(docker('info', '--format', '{{json .}}')),
              'workloads': []}
    # Retain only environment facts, not local engine configuration/paths.
    result['docker'] = {k: result['docker'].get(k) for k in
                        ('ServerVersion', 'Architecture', 'NCPU', 'MemTotal', 'OperatingSystem')}
    start = time.perf_counter()
    try:
        extra = ('--scale', 'worker=4') if backend == 'windmill' else ()
        print(f'{backend}: starting local stack', flush=True)
        compose(backend, 'up', '-d', *extra)
        adapter = importlib.import_module(backend + '_backend').Backend()
        result['setup'] = adapter.setup()
        result['stack_start_and_setup_seconds'] = time.perf_counter() - start
        images = json.loads(compose(backend, 'images', '--format', 'json'))
        result['images'] = images
        image_names = sorted({row['Repository'] + ':' + row['Tag'] for row in images})
        result['image_digests'] = [json.loads(docker('image', 'inspect', image,
                                      '--format', '{{json .RepoDigests}}')) for image in image_names]
        print(f'{backend}: warming full batch', flush=True)
        result['warmup'] = adapter.batch(20, 0.005)
        if result['warmup']['verified'] != 20:
            raise RuntimeError('Warmup did not verify all 20 results')
        result['memory_before'] = memory_snapshot(backend)
        for delay in delays:
            workload = {'sleep_seconds': delay, 'samples': []}
            result['workloads'].append(workload)
            for iteration in range(repeats):
                sample = adapter.batch(20, delay)
                if sample['verified'] != 20:
                    raise RuntimeError('Batch did not verify all 20 results')
                workload['samples'].append(sample)
                print(f'{backend}: sleep={delay} repeat={iteration+1} elapsed={sample["elapsed_seconds"]:.4f}s verified=20', flush=True)
                (output / (backend + '.json')).write_text(json.dumps(result, indent=2) + '\n')
            seconds = [s['elapsed_seconds'] for s in workload['samples']]
            workload['median_batch_seconds'] = statistics.median(seconds)
            workload['jobs_per_second'] = 20 / workload['median_batch_seconds']
        result['memory_after'] = memory_snapshot(backend)
        result['status'] = 'complete'
    except BaseException as exc:
        result['status'] = 'failed'
        result['error'] = f'{type(exc).__name__}: {exc}'
        try:
            (output / (backend + '.log')).write_text(compose(backend, 'logs', '--no-color', '--tail', '150'))
        except Exception:
            pass
        raise
    finally:
        (output / (backend + '.json')).write_text(json.dumps(result, indent=2) + '\n')
        # Only this benchmark's namespaced containers; preserve volumes/caches.
        compose(backend, 'down')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', choices=['all', *PROJECTS], default='all')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--delays', type=float, nargs='+', default=[0, .005])
    args = parser.parse_args()
    if args.repeats < 1 or any(d < 0 for d in args.delays):
        parser.error('positive repeat count and nonnegative delays required')
    for backend in PROJECTS if args.backend == 'all' else [args.backend]:
        run(backend, args.repeats, args.delays)


if __name__ == '__main__':
    main()
