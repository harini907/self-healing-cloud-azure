"""Open-loop load generator (Poisson arrivals, diurnal-style wave + flash bursts).

usage: python3 loadgen.py <duration_s> <seed> <out_csv> [base_rps] [t0_epoch]
Writes one row per request: t (s since start), latency_ms, status
(status 0 = client timeout / connection error).
"""
import csv
import http.client
import math
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import config as C

duration, seed, out = float(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
base = float(sys.argv[4]) if len(sys.argv) > 4 else float(C.BASE_RPS)
rng = random.Random(seed)
bursts = sorted(rng.uniform(0.1, 0.85) * duration for _ in range(2))


def rate(t):
    r = base * (1 + 0.3 * math.sin(2 * math.pi * t / 300.0))
    if any(b <= t < b + 30 for b in bursts):
        r *= 2.0
    return r


rows, lock = [], threading.Lock()
T0 = float(sys.argv[5]) if len(sys.argv) > 5 else time.time()


def hit(t_sched):
    t0 = time.perf_counter()
    status = 0
    try:
        c = http.client.HTTPConnection(C.HOST, C.GATEWAY_PORT, timeout=C.CLIENT_TIMEOUT_S)
        c.request("GET", "/api")
        r = c.getresponse()
        r.read()
        status = r.status
        c.close()
    except Exception:
        status = 0
    ms = (time.perf_counter() - t0) * 1000
    with lock:
        rows.append((round(t_sched, 3), round(ms, 2), status))


t = max(0.0, time.time() - T0)


with ThreadPoolExecutor(max_workers=256) as pool:
    while t < duration:
        t += rng.expovariate(rate(t))
        delay = T0 + t - time.time()
        if delay > 0:
            time.sleep(delay)
        pool.submit(hit, t)

with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["t", "latency_ms", "status"])
    w.writerows(sorted(rows))
print(f"loadgen: {len(rows)} requests, bursts at {[round(b) for b in bursts]}")
