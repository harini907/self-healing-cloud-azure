"""Run ONE experiment episode.

usage: python3 run.py --condition integrated --seed 1 --duration 720 --out results

Conditions (same four as the simulation paper, plus ablations):
  reactive              threshold autoscaling only                         (baseline)
  predictive            forecast-driven autoscaling, no healer
  selfheal              reactive autoscaling + diagnosis-based healer
  integrated            forecast autoscaling + healer + failure prediction + arbiter
  no_arbiter            integrated without the coordination arbiter        (ablation)
  no_diagnosis          integrated, healer restarts on any anomaly         (ablation)
  no_failure_pred       integrated without memory-exhaustion prediction    (ablation)
  no_uncertainty        integrated, forecast without the uncertainty buffer(ablation)

Platform defaults present in EVERY condition: a process supervisor restarts a
crashed replica after 1 s (= Docker restart policy) and the load balancer drops a
replica after 2 failed health checks (= cloud LB health checks).
"""
import argparse
import csv
import http.client
import json
import math
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import config as C

KIT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

CONDITIONS = {
    #                 scaler       healer  diag   failpred arbiter buffer
    "reactive":        ("reactive",   False, True,  False, True,  True),
    "predictive":      ("predictive", False, True,  False, True,  True),
    "selfheal":        ("reactive",   True,  True,  False, True,  True),
    "integrated":      ("predictive", True,  True,  True,  True,  True),
    "no_arbiter":      ("predictive", True,  True,  True,  False, True),
    "no_diagnosis":    ("predictive", True,  False, True,  True,  True),
    "no_failure_pred": ("predictive", True,  True,  False, True,  True),
    "no_uncertainty":  ("predictive", True,  True,  True,  True,  False),
}


# ----------------------------------------------------------------------------- utils
def http_req(port, method, path, timeout=1.0):
    t0 = time.perf_counter()
    c = http.client.HTTPConnection(C.HOST, port, timeout=timeout)
    try:
        c.request(method, path)
        r = c.getresponse()
        body = r.read()
        return r.status, (json.loads(body) if body else {}), (time.perf_counter() - t0) * 1000
    finally:
        c.close()


def try_req(port, method, path, timeout=1.0):
    try:
        return http_req(port, method, path, timeout)
    except Exception:
        return None


class Log:
    def __init__(self, path, header):
        self.f = open(path, "w", newline="")
        self.w = csv.writer(self.f)
        self.w.writerow(header)
        self.lock = threading.Lock()

    def row(self, *vals):
        with self.lock:
            self.w.writerow(vals)
            self.f.flush()


# ------------------------------------------------------------------------ supervisor
class Supervisor:
    """Platform default: restart crashed replicas (all conditions)."""

    def __init__(self, T0, actions, logdir):
        self.T0, self.actions, self.logdir = T0, actions, logdir
        self.procs, self.wanted, self.pending = {}, set(), {}
        self.spawned_at = {}
        self.isolated = False
        self.restarts = {n: 0 for n in C.APPS}
        self.lock = threading.Lock()
        self.exit_events = []          # (t, name, code) for ground truth
        threading.Thread(target=self.loop, daemon=True).start()

    def now(self):
        return round(time.time() - self.T0, 2)

    def _spawn(self, name):
        env = dict(os.environ, ISOLATED="1" if self.isolated else "0")
        self.spawned_at[name] = time.time()
        err = open(os.path.join(self.logdir, f"{name}.stderr"), "a")
        self.procs[name] = subprocess.Popen([PY, os.path.join(KIT, "app.py"), name], cwd=KIT,
                                            env=env, stdout=subprocess.DEVNULL, stderr=err)

    def start(self, name):
        with self.lock:
            self.wanted.add(name)
            self._spawn(name)

    def stop(self, name):
        with self.lock:
            self.wanted.discard(name)
            p = self.procs.pop(name, None)
        if p:
            p.kill()
            p.wait()

    def restart(self, name):
        """Controller-initiated restart (kill + start)."""
        with self.lock:
            p = self.procs.get(name)
            if p:
                p.kill()
                p.wait()
            self.restarts[name] += 1
            self._spawn(name)

    def is_restarting(self, name):
        """True while a replica is waiting to be respawned or still warming up."""
        return name in self.pending or \
            time.time() - self.spawned_at.get(name, 0) < C.APP_WARMUP_S + 2

    def loop(self):
        while True:
            time.sleep(0.2)
            with self.lock:
                for name in list(self.wanted):
                    p = self.procs.get(name)
                    if name in self.pending:
                        if time.time() >= self.pending[name]:
                            del self.pending[name]
                            self.restarts[name] += 1
                            self._spawn(name)
                            self.actions.row(self.now(), "supervisor", name, "process_exit", "restart", "")
                    elif p and p.poll() is not None:
                        self.exit_events.append((self.now(), name, p.returncode))
                        self.actions.row(self.now(), "supervisor", name, "process_exit",
                                         "detected", f"exit={p.returncode}")
                        self.pending[name] = time.time() + C.SUPERVISOR_RESTART_DELAY_S

    def shutdown(self):
        with self.lock:
            self.wanted.clear()
            for p in self.procs.values():
                p.kill()


# ------------------------------------------------------------------------ forecaster
class Holt:
    """Damped-trend Holt exponential smoothing with an empirical uncertainty band."""

    WARMUP_TICKS = 20  # ignore the start-up ramp of the 5-s rate window

    def __init__(self):
        self.level = self.trend = None
        self.pending = []      # (due_t, forecast)
        self.errors = []
        self.n = 0

    def ready(self):
        return self.n > self.WARMUP_TICKS

    def update(self, t, y):
        self.n += 1
        if self.n <= self.WARMUP_TICKS // 2:
            self.level, self.trend = y, 0.0
            return y
        if self.level is None:
            self.level, self.trend = y, 0.0
        else:
            prev, phi = self.level, C.FORECAST_PHI
            self.level = C.FORECAST_ALPHA * y + (1 - C.FORECAST_ALPHA) * (self.level + phi * self.trend)
            self.trend = C.FORECAST_BETA * (self.level - prev) + (1 - C.FORECAST_BETA) * phi * self.trend
        while self.pending and self.pending[0][0] <= t:
            due, f = self.pending.pop(0)
            if self.ready():
                self.errors = (self.errors + [y - f])[-120:]
        f = self.forecast()
        self.pending.append((t + C.FORECAST_HORIZON_S, f))
        return f

    def forecast(self):
        damp = sum(C.FORECAST_PHI ** k for k in range(1, C.FORECAST_HORIZON_S + 1))
        return max(0.0, self.level + damp * self.trend)

    def sigma(self):
        return statistics.pstdev(self.errors) if len(self.errors) >= 10 else 0.0


# ------------------------------------------------------------------------ controller
class Controller:
    def __init__(self, cond, T0, logdir, sup, actions, ticks):
        (self.scaler, self.healer_on, self.diag_on, self.failpred_on,
         self.arbiter_on, self.buffer_on) = CONDITIONS[cond]
        self.T0, self.sup, self.actions, self.ticks = T0, sup, actions, ticks
        self.replicas = [f"app{i}" for i in range(1, C.MIN_REPLICAS + 1)]
        self.lb_fails = {n: 0 for n in C.APPS}
        self.warm = set()
        self.excluded = {}          # name -> reason (reroute | drain | restart)
        self.streak = {n: 0 for n in C.APPS}
        self.pending = {}           # name -> healing episode dict
        self.isolated = False
        self.dep_ok_ticks = 0
        self.good_ticks = {n: 0 for n in C.APPS}
        self.mem = {n: [] for n in C.APPS}
        self.last_up = self.last_down = -1e9
        self.low_since = None
        self.rotation_pushed = None
        self.holt = Holt()
        self.contradictions = 0
        self.pool = ThreadPoolExecutor(max_workers=8)
        self.stop = False

    def now(self):
        return round(time.time() - self.T0, 2)

    def act(self, actor, target, diagnosis, action, note=""):
        self.actions.row(self.now(), actor, target, diagnosis, action, note)

    # --------------------------------------------------------------- observation
    def probe(self, name):
        r = try_req(C.APPS[name][1], "GET", "/health", timeout=C.PROBE_TIMEOUT_S)
        if r and r[0] == 200:
            d = r[1]
            d["lat"] = r[2]
            return d
        return None

    def redis_ok(self):
        import socket
        try:
            with socket.create_connection((C.HOST, C.REDIS_PROXY_PORT), timeout=0.5) as s:
                s.settimeout(0.5)
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                return s.recv(16).startswith(b"+PONG")
        except Exception:
            return False

    # --------------------------------------------------------------- capacity
    def effective(self):
        return [r for r in self.replicas if r not in self.excluded]

    def rotation(self):
        rot = [r for r in self.replicas if r in self.warm and self.lb_fails[r] < C.LB_HEALTH_FAILS
               and r not in self.excluded]
        if not rot:  # never leave the LB empty
            rot = [r for r in self.replicas if r in self.warm] or list(self.replicas)
        return rot

    def scale_up(self, n=1, why=""):
        added = []
        for _ in range(n):
            free = [a for a in C.APPS if a not in self.replicas]
            if not free or len(self.replicas) >= C.MAX_REPLICAS:
                break
            a = free[0]
            self.replicas.append(a)
            self.warm.discard(a)
            self.mem[a] = []
            self.sup.start(a)
            added.append(a)
        if added:
            self.last_up = time.time()
            self.act("scaler", ",".join(added), "capacity", "scale_up", why)
        return added

    def scale_down(self, why=""):
        if len(self.replicas) <= C.MIN_REPLICAS:
            return
        healing = bool(self.pending) or bool(self.excluded)
        if healing and self.arbiter_on:
            self.act("arbiter", "-", "conflict", "block_scale_down", "healing in progress")
            self.last_down = time.time()
            return
        if healing:
            self.contradictions += 1
            self.act("scaler", "-", "conflict", "contradiction", "scale-down during healing")
        victim = self.replicas[-1]
        self.replicas.remove(victim)
        self.excluded.pop(victim, None)
        self.pending.pop(victim, None)
        self.warm.discard(victim)
        self.sup.stop(victim)
        self.last_down = time.time()
        self.act("scaler", victim, "capacity", "scale_down", why)

    def autoscale(self, rps, util, fc):
        t = time.time()
        n_eff = len(self.effective())
        if self.scaler == "predictive" and self.holt.ready():
            target_load = fc + (C.FORECAST_Z * self.holt.sigma() if self.buffer_on else 0.0)
            need = max(C.MIN_REPLICAS, math.ceil(target_load / (C.CAP_PER_REPLICA * C.SCALE_UP_UTIL)))
            if util > C.SCALE_UP_UTIL:
                need = max(need, n_eff + 1)
            if need > n_eff and t - self.last_up > 3:
                self.scale_up(need - n_eff, f"forecast={fc:.1f} need={need}")
                return
            want_down = need < n_eff and util < 0.5
        else:
            if util > C.SCALE_UP_UTIL and t - self.last_up > C.SCALE_UP_COOLDOWN_S:
                self.scale_up(1, f"util={util:.2f}")
                return
            want_down = util < C.SCALE_DOWN_UTIL
        if want_down:
            self.low_since = self.low_since or t
            if t - self.low_since > C.SCALE_DOWN_COOLDOWN_S and t - self.last_down > C.SCALE_DOWN_COOLDOWN_S \
                    and t - self.last_up > C.SCALE_DOWN_COOLDOWN_S:
                self.scale_down(f"util={util:.2f}")
                self.low_since = None
        else:
            self.low_since = None

    # --------------------------------------------------------------- healing
    def symptoms(self, name, pr, stats):
        s = set()
        if pr is None:
            if not self.sup.is_restarting(name):
                s.add("unreachable")
            return s
        if not pr.get("dep_ok", True) and not self.isolated:
            s.add("dep")
        if pr["lat"] > C.LAT_ANOMALY_MS:
            s.add("slow")
        if pr.get("cpu", 0) > C.CPU_ANOMALY:
            s.add("hot")
        b = stats.get("backends", {}).get(name)
        # gateway errors only count once the replica has been up longer than the
        # 5-s stats window (otherwise we re-diagnose errors from before a restart)
        if b and b["n"] >= 5 and b["err"] > 0.2 and not self.isolated and pr.get("uptime", 0) > 8:
            s.add("errors")
        return s

    def diagnose(self, sym, util):
        """Rule-based diagnosis -> (cause, first action, confidence)."""
        if not self.diag_on:
            return "unknown", "restart", 0.25
        if "dep" in sym:
            return "dependency_fault", "isolate", 0.9
        if "unreachable" in sym:
            return "component_fault", "restart", 0.8
        if "hot" in sym:
            return "component_fault", "restart", 0.8
        if "slow" in sym and util > 0.9:
            return "capacity_shortage", "scale_up", 0.6
        if "slow" in sym:
            return "network_fault", "reroute", 0.7
        if "errors" in sym:
            return "component_fault", "restart", 0.5
        return "unknown", "restart", 0.3

    LADDER = ["restart", "reroute", "isolate", "scale_up"]

    def do_action(self, name, action, fc, rps):
        if action == "restart":
            return self.drain_restart(name, fc, rps, why="healer")
        if action == "reroute":
            self.excluded[name] = "reroute"
            self.good_ticks[name] = 0
            return True
        if action == "isolate":
            self.set_isolation(True)
            return True
        if action == "scale_up":
            self.scale_up(1, "healer escalation")
            return True
        return False

    def drain_restart(self, name, fc, rps, why):
        """Make-before-break restart. The arbiter first checks that the remaining
        replicas can carry the (forecast) load; if not it scales up and defers."""
        if self.arbiter_on:
            others = [r for r in self.rotation() if r != name]
            load = max(fc, rps)
            if load > C.SCALE_UP_UTIL * C.CAP_PER_REPLICA * max(1, len(others)):
                if not [r for r in self.replicas if r not in self.warm]:
                    self.scale_up(1, f"arbiter: capacity before restarting {name}")
                self.act("arbiter", name, "conflict", "defer_restart", f"load={load:.1f}")
                return False
        self.excluded[name] = "restart"
        self.warm.discard(name)
        self.mem[name] = []
        self.sup.restart(name)
        return True

    def set_isolation(self, on):
        if self.isolated != on:
            self.isolated = on
            self.sup.isolated = on
            self.act("healer", "all", "dependency_fault", "isolate" if on else "unisolate", "")
        for r in self.replicas:
            self.pool.submit(try_req, C.APPS[r][1], "POST", f"/admin/isolate?on={int(on)}")

    def heal(self, probes, stats, util, fc, rps):
        t = time.time()
        # 1. undo temporary measures once the cause is gone
        for r, why in list(self.excluded.items()):
            pr = probes.get(r)
            if why == "restart" and pr is not None and r in self.warm:
                del self.excluded[r]
                self.act("healer", r, "-", "readmit", "restart finished")
            elif why == "reroute":
                self.good_ticks[r] = self.good_ticks[r] + 1 if pr and pr["lat"] < C.LAT_ANOMALY_MS else 0
                if self.good_ticks[r] >= C.HEALTHY_TICKS_TO_UNDO:
                    del self.excluded[r]
                    self.act("healer", r, "-", "readmit", "path healthy again")
        if self.isolated:
            self.dep_ok_ticks = self.dep_ok_ticks + 1 if self.redis_ok() else 0
            if self.dep_ok_ticks >= C.HEALTHY_TICKS_TO_UNDO:
                self.set_isolation(False)
        # 2. verification of pending episodes
        for r, ep in list(self.pending.items()):
            if t < ep["verify_at"]:
                continue
            sym = self.symptoms(r, probes.get(r), stats) if ep["last"] == "restart" else set()
            if ep["last"] == "restart":
                ok = r in self.warm and not sym
            elif ep["last"] == "isolate":
                ok = stats.get("err", 0) < 0.05
            elif ep["last"] == "reroute":
                ok = stats.get("err", 0) < 0.05 and all(
                    b["p95"] < C.SLO_LATENCY_MS for n, b in stats.get("backends", {}).items()
                    if n in self.rotation())
            else:
                ok = util < C.SCALE_UP_UTIL
            if ok:
                self.act("healer", r, ep["cause"], "verified", ep["last"])
                del self.pending[r]
                continue
            if len(ep["tried"]) >= C.MAX_ESCALATIONS:
                self.act("healer", r, ep["cause"], "give_up", ",".join(ep["tried"]))
                del self.pending[r]
                continue
            ladder = ["restart"] if not self.diag_on else self.LADDER
            nxt = next((a for a in ladder if a not in ep["tried"]), None) or "restart"
            if self.do_action(r, nxt, fc, rps):
                ep["tried"].append(nxt)
                ep["last"] = nxt
                ep["verify_at"] = t + C.VERIFY_WAIT_S + (C.APP_WARMUP_S if nxt == "restart" else 0)
                self.act("healer", r, ep["cause"], "escalate:" + nxt, f"step={len(ep['tried'])}")
        # 3. detection + diagnosis for replicas without an open episode
        for r in self.effective():
            if r in self.pending or r not in self.warm:
                continue
            sym = self.symptoms(r, probes.get(r), stats)
            self.streak[r] = self.streak[r] + 1 if sym else 0
            if self.streak[r] < C.DETECT_STREAK:
                continue
            cause, action, conf = self.diagnose(sym, util)
            if cause == "capacity_shortage":
                continue  # capacity is the scaler's job; healer stays out
            self.act("healer", r, cause, "detected", f"sym={'+'.join(sorted(sym))} conf={conf}")
            if self.do_action(r, action, fc, rps):
                self.pending[r] = {"cause": cause, "tried": [action], "last": action,
                                   "verify_at": t + C.VERIFY_WAIT_S +
                                   (C.APP_WARMUP_S if action == "restart" else 0)}
                self.act("healer", r, cause, action, f"conf={conf}")
                self.streak[r] = 0

    def predict_failures(self, probes, fc, rps):
        """Prediction-driven healing: extrapolate memory growth, restart before OOM."""
        t = time.time()
        for r in self.effective():
            pr = probes.get(r)
            if pr is None or r in self.pending:
                continue
            h = self.mem[r]
            h.append((t, pr["mem_mb"]))
            while h and t - h[0][0] > C.MEM_WINDOW_S:
                h.pop(0)
            if len(h) < 8:
                continue
            xs, ys = [p[0] - h[0][0] for p in h], [p[1] for p in h]
            mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
            den = sum((x - mx) ** 2 for x in xs) or 1e-9
            slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
            if slope < C.MEM_MIN_SLOPE_MB_S:
                continue
            eta = (C.APP_MEM_LIMIT_MB - ys[-1]) / slope
            if eta < C.MEM_ETA_ACT_S:
                self.act("predictor", r, "predicted_oom", "detected",
                         f"slope={slope:.2f}MB/s eta={eta:.1f}s mem={ys[-1]:.0f}MB")
                if self.drain_restart(r, fc, rps, why="predicted OOM"):
                    self.pending[r] = {"cause": "predicted_oom", "tried": ["restart"], "last": "restart",
                                       "verify_at": t + C.VERIFY_WAIT_S + C.APP_WARMUP_S}
                    self.act("predictor", r, "predicted_oom", "proactive_restart", f"eta={eta:.1f}s")

    # --------------------------------------------------------------- main loop
    def tick(self):
        names = list(self.replicas)
        futs = {n: self.pool.submit(self.probe, n) for n in names}
        st = try_req(C.GATEWAY_PORT, "GET", "/stats", timeout=1.0)
        stats = st[1] if st else {}
        probes = {n: f.result() for n, f in futs.items()}
        for n in names:
            if probes[n] is None:
                self.lb_fails[n] += 1
            else:
                self.lb_fails[n] = 0
                if n not in self.warm:
                    self.warm.add(n)
                    if pr_isolated_mismatch(probes[n], self.isolated):
                        self.pool.submit(try_req, C.APPS[n][1], "POST",
                                         f"/admin/isolate?on={int(self.isolated)}")
        rps = stats.get("rps", 0.0)
        fc = self.holt.update(time.time(), rps)
        n_rot = max(1, len(self.rotation()))
        util = rps / (n_rot * C.CAP_PER_REPLICA)
        if self.healer_on:
            self.heal(probes, stats, util, fc, rps)
        if self.failpred_on:
            self.predict_failures(probes, fc, rps)
        self.autoscale(rps, util, fc if self.scaler == "predictive" else rps)
        rot = self.rotation()
        if rot != self.rotation_pushed:
            if try_req(C.GATEWAY_PORT, "POST", "/admin/backends?set=" + ",".join(rot)):
                self.rotation_pushed = rot
        self.ticks.row(self.now(), rps, round(fc, 2), round(self.holt.sigma(), 2), len(self.replicas),
                       len(rot), round(util, 3), "|".join(rot), int(self.isolated),
                       stats.get("err", ""))

    def run(self, duration):
        next_t = time.time()
        while not self.stop and time.time() - self.T0 < duration:
            try:
                self.tick()
            except Exception as e:  # never let the controller die mid-run
                self.act("controller", "-", "error", "exception", repr(e)[:200])
            next_t += C.TICK_S
            time.sleep(max(0.0, next_t - time.time()))


def pr_isolated_mismatch(pr, isolated):
    return pr is not None and bool(pr.get("isolated")) != bool(isolated)


# ------------------------------------------------------------------------ injector
FAULT_TYPES = ["crash", "cpu_hog", "network_delay", "dependency_outage", "memory_leak"]


def fault_plan(seed, duration):
    rng = random.Random(seed * 7919)
    types = FAULT_TYPES[:]
    rng.shuffle(types)
    gap = (duration - 150) / len(types)
    plan = []
    for i, ft in enumerate(types):
        start = 60 + i * gap + rng.uniform(0, 10)
        dur = {"crash": 0, "cpu_hog": 75, "network_delay": 75, "dependency_outage": 45,
               "memory_leak": 0}[ft]
        plan.append({"type": ft, "target": rng.choice(["app1", "app2"]), "start": round(start, 1),
                     "dur": dur})
    return plan


def injector(plan, T0, faults_log, stop_evt):
    for f in plan:
        while time.time() - T0 < f["start"]:
            if stop_evt.is_set():
                return
            time.sleep(0.1)
        tgt, port = f["target"], C.APPS[f["target"]][0]
        t = round(time.time() - T0, 2)
        ok = None
        if f["type"] == "crash":
            ok = try_req(port, "POST", "/fault/crash")
        elif f["type"] == "cpu_hog":
            ok = try_req(port, "POST", f"/fault/cpu?threads=4&seconds={f['dur']}")
        elif f["type"] == "memory_leak":
            ok = try_req(port, "POST", "/fault/leak?mb_per_s=2.5")
        elif f["type"] == "network_delay":
            ok = try_req(C.PROXY_ADMIN_PORT, "POST", f"/set?name={tgt}&delay_ms=220")
        elif f["type"] == "dependency_outage":
            ok = try_req(C.PROXY_ADMIN_PORT, "POST", "/set?name=redis&down=1")
        faults_log.row(f["type"], tgt, t, f["dur"], "injected" if ok else "FAILED")
        if f["type"] in ("network_delay", "dependency_outage"):
            def clear(ft=f["type"], tgt=tgt):
                if ft == "network_delay":
                    try_req(C.PROXY_ADMIN_PORT, "POST", f"/set?name={tgt}&delay_ms=0")
                else:
                    try_req(C.PROXY_ADMIN_PORT, "POST", "/set?name=redis&down=0")
            threading.Timer(f["dur"], clear).start()


# ------------------------------------------------------------------------ main
def wait_port(port, timeout=15):
    import socket
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection((C.HOST, port), timeout=0.3).close()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--condition", required=True, choices=list(CONDITIONS))
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--duration", type=float, default=720)
    ap.add_argument("--out", default="results")
    ap.add_argument("--rps", type=float, default=C.BASE_RPS)
    a = ap.parse_args()

    logdir = os.path.join(a.out, a.condition, f"seed{a.seed}")
    os.makedirs(logdir, exist_ok=True)
    bg = []

    def spawn(cmd, name):
        p = subprocess.Popen(cmd, cwd=KIT, stdout=open(os.path.join(logdir, name + ".out"), "w"),
                             stderr=subprocess.STDOUT)
        bg.append(p)
        return p

    if shutil.which("redis-server"):
        spawn(["redis-server", "--port", str(C.REDIS_PORT), "--save", "", "--appendonly", "no",
               "--bind", C.HOST], "redis")
    else:
        spawn([PY, "minikv.py", str(C.REDIS_PORT)], "redis")
    spawn([PY, "faultproxy.py"], "faultproxy")
    spawn([PY, "gateway.py"], "gateway")
    for port in (C.REDIS_PORT, C.PROXY_ADMIN_PORT, C.GATEWAY_PORT):
        if not wait_port(port):
            sys.exit(f"service on port {port} did not start - is something else using it?")

    actions = Log(os.path.join(logdir, "actions.csv"), ["t", "actor", "target", "diagnosis", "action", "note"])
    ticks = Log(os.path.join(logdir, "ticks.csv"),
                ["t", "rps", "forecast", "sigma", "replicas", "rotation_n", "util", "rotation",
                 "isolated", "gw_err"])
    faults = Log(os.path.join(logdir, "faults.csv"), ["type", "target", "t", "dur", "status"])

    T0 = time.time() + C.APP_WARMUP_S + 2  # t=0 once the first replicas are warm
    sup = Supervisor(T0, actions, logdir)
    ctl = Controller(a.condition, T0, logdir, sup, actions, ticks)
    for r in ctl.replicas:
        sup.start(r)
    while time.time() < T0:
        time.sleep(0.05)
    lg = spawn([PY, "loadgen.py", str(a.duration), str(a.seed), os.path.join(logdir, "requests.csv"),
                str(a.rps), str(T0)], "loadgen")
    stop_evt = threading.Event()
    plan = fault_plan(a.seed, a.duration)
    threading.Thread(target=injector, args=(plan, T0, faults, stop_evt), daemon=True).start()

    def bye(*_):
        stop_evt.set()
        ctl.stop = True
    signal.signal(signal.SIGINT, bye)
    signal.signal(signal.SIGTERM, bye)

    print(f"[{a.condition} seed={a.seed}] running {a.duration:.0f}s ... faults: "
          + ", ".join(f"{f['type']}@{f['start']:.0f}s->{f['target']}" for f in plan), flush=True)
    ctl.run(a.duration)
    stop_evt.set()
    lg.wait(timeout=30)
    with open(os.path.join(logdir, "meta.json"), "w") as f:
        json.dump({"condition": a.condition, "seed": a.seed, "duration": a.duration, "rps": a.rps,
                   "plan": plan, "contradictions": ctl.contradictions,
                   "restarts": sup.restarts, "exits": sup.exit_events,
                   "config": {k: getattr(C, k) for k in dir(C) if k.isupper()}}, f, indent=1)
    sup.shutdown()
    for p in bg:
        p.kill()
    try_req(C.PROXY_ADMIN_PORT, "POST", "/set?name=redis&down=0")
    print(f"[{a.condition} seed={a.seed}] done -> {logdir}", flush=True)


if __name__ == "__main__":
    main()
