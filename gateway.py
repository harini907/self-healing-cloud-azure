"""Load-balancing gateway (plays the role of nginx / a cloud load balancer).

Round-robin over the active backends, reaching each through the fault proxy.
Like nginx's default `proxy_next_upstream error`, a request whose connection
fails is retried once on the next backend (passive failover, all conditions).

GET  /api                         -> forwarded
GET  /stats                       -> per-backend count / error rate / p95 (last 5 s)
POST /admin/backends?set=app1,app2 -> change rotation (used by the healer)
"""
import http.client
import json
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import config as C

lock = threading.Lock()
active = [f"app{i}" for i in range(1, C.MIN_REPLICAS + 1)]
allreq = deque(maxlen=20000)   # (ts, ok) for every /api request
rr = [0]
samples = {n: deque(maxlen=5000) for n in C.APPS}   # (ts, latency_ms, ok)


def pick():
    with lock:
        if not active:
            return []
        i = rr[0] % len(active)
        rr[0] += 1
        return active[i:] + active[:i]


def forward(name):
    port = C.APPS[name][1]
    t0 = time.perf_counter()
    conn = http.client.HTTPConnection(C.HOST, port, timeout=C.CLIENT_TIMEOUT_S)
    try:
        conn.request("GET", "/api")
        r = conn.getresponse()
        body = r.read()
        return r.status, body, (time.perf_counter() - t0) * 1000
    finally:
        conn.close()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api":
            order = pick()
            for attempt, name in enumerate(order[:2]):
                try:
                    status, body, ms = forward(name)
                    samples[name].append((time.time(), ms, status == 200))
                    allreq.append((time.time(), status == 200))
                    return self.send(status, body)
                except (ConnectionError, OSError) as e:
                    is_timeout = "timed out" in str(e)
                    samples[name].append((time.time(), C.CLIENT_TIMEOUT_S * 1000, False))
                    if is_timeout:
                        break           # do not retry timeouts
            allreq.append((time.time(), False))
            return self.send(502, {"error": "no backend"})
        if path == "/stats":
            now = time.time()
            recent = [r for r in list(allreq) if now - r[0] <= 5]
            out = {"active": list(active), "backends": {},
                   "rps": round(len(recent) / 5.0, 2),
                   "err": round(sum(1 for r in recent if not r[1]) / len(recent), 3) if recent else 0.0}
            for n, dq in samples.items():
                win = [s for s in list(dq) if now - s[0] <= 5]
                if not win:
                    continue
                lats = sorted(s[1] for s in win)
                out["backends"][n] = {
                    "n": len(win),
                    "err": round(sum(1 for s in win if not s[2]) / len(win), 3),
                    "p95": round(lats[int(0.95 * (len(lats) - 1))], 1),
                }
            return self.send(200, out)
        self.send(404, {})

    def do_POST(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/admin/backends" and "set" in q:
            new = [b for b in q["set"].split(",") if b in C.APPS]
            with lock:
                active[:] = new
            return self.send(200, {"active": new})
        self.send(404, {})


if __name__ == "__main__":
    srv = ThreadingHTTPServer((C.HOST, C.GATEWAY_PORT), H)
    srv.daemon_threads = True
    srv.request_queue_size = 256
    srv.serve_forever()
