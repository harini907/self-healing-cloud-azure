"""The microservice under test (one OS process per replica).

GET  /api                 -> worker-pool-bound request: CPU work + I/O wait + Redis INCR
GET  /health              -> {"ok","dep_ok","cpu","mem_mb","isolated","uptime"}
POST /admin/isolate?on=1  -> dependency isolation (circuit breaker): skip Redis and
                             serve a degraded-but-successful response
Fault hooks used by the injector:
POST /fault/crash                      -> exit(1)               (process crash)
POST /fault/cpu?threads=4&seconds=S    -> runaway busy threads  (CPU hog)
POST /fault/leak?mb_per_s=2            -> memory leak until OOM (exit 137 at limit)
Stdlib only.
"""
import hashlib
import json
import os
import resource
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import config as C

NAME = sys.argv[1]
PORT = C.APPS[NAME][0]
START = time.time()
state = {"isolated": os.environ.get("ISOLATED") == "1", "cpu": 0.0, "dep_ok": True}
workers = threading.BoundedSemaphore(C.APP_WORKERS)
leak = []
PAGE = os.sysconf("SC_PAGE_SIZE")


def rss_mb():
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * PAGE / 1048576
    except OSError:  # macOS fallback: ru_maxrss is in BYTES on macOS (KB on Linux)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak / 1048576 if sys.platform == "darwin" else peak / 1024


def redis_incr(key="hits"):
    """Minimal RESP client: INCR key via the fault proxy. Raises on failure."""
    with socket.create_connection((C.HOST, C.REDIS_PROXY_PORT), timeout=C.REDIS_TIMEOUT_S) as s:
        s.settimeout(C.REDIS_TIMEOUT_S)
        s.sendall(f"*2\r\n$4\r\nINCR\r\n${len(key)}\r\n{key}\r\n".encode())
        data = s.recv(64)
        if not data.startswith(b":"):
            raise IOError("bad redis reply")
        return int(data[1:].split(b"\r\n")[0])


def meter():
    last_cpu, last_t = time.process_time(), time.time()
    while True:
        time.sleep(0.5)
        c, t = time.process_time(), time.time()
        state["cpu"] = round((c - last_cpu) / (t - last_t), 3)
        last_cpu, last_t = c, t
        if rss_mb() > C.APP_MEM_LIMIT_MB:
            os._exit(137)  # behaves like the kernel / container OOM killer


def hog(until):
    x = 0
    while time.time() < until:
        x = (x * 31 + 7) % 1000003


def leaker(mb_per_s):
    while True:
        leak.append(bytearray(os.urandom(1024)) * 1024)  # 1 MB, touched pages
        time.sleep(1.0 / mb_per_s)


def cpu_work():
    h = b"x"
    end = time.perf_counter() + C.APP_CPU_MS / 1000
    while time.perf_counter() < end:
        h = hashlib.sha256(h).digest()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *a):
        pass

    def send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api":
            with workers:  # queue here when all workers are busy
                cpu_work()
                time.sleep(C.APP_IO_MS / 1000)
                if state["isolated"]:
                    return self.send(200, {"app": NAME, "degraded": True})
                try:
                    n = redis_incr()
                    state["dep_ok"] = True
                    return self.send(200, {"app": NAME, "n": n})
                except Exception as e:
                    state["dep_ok"] = False
                    return self.send(500, {"app": NAME, "error": str(e)})
        if path == "/health":
            try:
                redis_incr("health")
                state["dep_ok"] = True
            except Exception:
                state["dep_ok"] = False
            return self.send(200, {"app": NAME, "ok": True, "dep_ok": state["dep_ok"],
                                   "cpu": state["cpu"], "mem_mb": round(rss_mb(), 1),
                                   "isolated": state["isolated"],
                                   "uptime": round(time.time() - START, 1)})
        self.send(404, {})

    def do_POST(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/admin/isolate":
            state["isolated"] = q.get("on", "1") == "1"
            return self.send(200, {"isolated": state["isolated"]})
        if u.path == "/fault/crash":
            self.send(200, {"crashing": True})
            self.wfile.flush()
            threading.Timer(0.05, lambda: os._exit(1)).start()
            return
        if u.path == "/fault/cpu":
            until = time.time() + float(q.get("seconds", 60))
            for _ in range(int(q.get("threads", 4))):
                threading.Thread(target=hog, args=(until,), daemon=True).start()
            return self.send(200, {"hog_until": until})
        if u.path == "/fault/leak":
            threading.Thread(target=leaker, args=(float(q.get("mb_per_s", 2)),),
                             daemon=True).start()
            return self.send(200, {"leaking": True})
        self.send(404, {})


if __name__ == "__main__":
    time.sleep(C.APP_WARMUP_S)  # models container start-up / cache warm-up cost
    threading.Thread(target=meter, daemon=True).start()
    srv = ThreadingHTTPServer((C.HOST, PORT), H)
    srv.daemon_threads = True
    srv.request_queue_size = 256
    srv.serve_forever()
