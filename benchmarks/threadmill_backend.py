import concurrent.futures
import json
import time
import urllib.request


class Backend:
    def __init__(self, base_url='http://127.0.0.1:18080'):
        self.base = base_url

    def request(self, path, data=None):
        encoded = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request(self.base + path, data=encoded,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    def setup(self):
        deadline = time.monotonic() + 120
        while True:
            try:
                self.request('/health')
                return
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def run_one(self, delay):
        identifier = self.request('/jobs', {'value': 41, 'delay': delay})['id']
        deadline = time.monotonic() + 180
        while True:
            result = self.request('/jobs/' + identifier)
            if result['completed']:
                if not result['success'] or result['result'] != 42:
                    raise RuntimeError(f'Threadmill job failed: {result}')
                return 42
            if time.monotonic() >= deadline:
                raise TimeoutError('Threadmill job timed out')
            time.sleep(.01)

    def batch(self, jobs, delay):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            start = time.perf_counter()
            results = list(pool.map(self.run_one, [delay] * jobs))
            elapsed = time.perf_counter() - start
        return {'elapsed_seconds': elapsed, 'verified': len(results)}
