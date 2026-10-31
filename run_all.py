"""Run the whole study: every condition x every seed, one after another.

usage:
  python3 run_all.py                       # 4 main conditions + 4 ablations, 5 seeds, 12 min each
  python3 run_all.py --main-only           # only the 4 main conditions
  python3 run_all.py --seeds 3 --duration 600
Finished runs are skipped, so you can stop (Ctrl-C) and resume at any time.
Runs are interleaved by seed (seed1: all conditions, seed2: all conditions, ...)
so that if you stop early you still have a balanced, comparable data set.
"""
import argparse
import os
import subprocess
import sys
import time

MAIN = ["reactive", "predictive", "selfheal", "integrated"]
ABL = ["no_arbiter", "no_diagnosis", "no_failure_pred", "no_uncertainty"]

ap = argparse.ArgumentParser()
ap.add_argument("--seeds", type=int, default=5)
ap.add_argument("--duration", type=float, default=720)
ap.add_argument("--out", default="results")
ap.add_argument("--main-only", action="store_true")
ap.add_argument("--rps", type=float, default=None)
a = ap.parse_args()

conds = MAIN if a.main_only else MAIN + ABL
todo = [(s, c) for s in range(1, a.seeds + 1) for c in conds
        if not os.path.exists(os.path.join(a.out, c, f"seed{s}", "meta.json"))]
est = len(todo) * (a.duration + 25) / 3600
print(f"{len(todo)} runs to do, about {est:.1f} hours. Results -> {a.out}/", flush=True)
here = os.path.dirname(os.path.abspath(__file__))
for i, (s, c) in enumerate(todo, 1):
    print(f"\n=== run {i}/{len(todo)}: {c} seed {s}  ({time.strftime('%H:%M:%S')})", flush=True)
    cmd = [sys.executable, os.path.join(here, "run.py"), "--condition", c, "--seed", str(s),
           "--duration", str(a.duration), "--out", a.out]
    if a.rps:
        cmd += ["--rps", str(a.rps)]
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"run failed (exit {r.returncode}); continuing", flush=True)
    time.sleep(5)  # let ports close before the next run
print("\nAll runs finished. Now run:  python3 analyze.py", a.out)
