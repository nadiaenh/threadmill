"""Benchmark-only loopback HTTP adapter; not a production service."""
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threadmill import Lane, Scheduler

scheduler = Scheduler({'default': Lane(4, 256)})
jobs = {}
lock = threading.Lock()


def work(ctx, args):
    time.sleep(args['delay']) if args['delay'] else None
    return args['value'] + 1


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            job = scheduler.submit(work, payload)
            identifier = uuid.uuid4().hex
            with lock:
                jobs[identifier] = job
            self.reply({'id': identifier})
        except Exception as exc:
            self.reply({'error': str(exc)}, 400)

    def do_GET(self):
        if self.path == '/health':
            self.reply({'ok': True})
            return
        identifier = self.path.rsplit('/', 1)[-1]
        with lock:
            job = jobs.get(identifier)
        if job is None:
            self.reply({'error': 'unknown job'}, 404)
        elif not job.done():
            self.reply({'completed': False})
        else:
            try:
                self.reply({'completed': True, 'success': True, 'result': job.result()})
            finally:
                with lock:
                    jobs.pop(identifier, None)


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', 8000), Handler).serve_forever()
