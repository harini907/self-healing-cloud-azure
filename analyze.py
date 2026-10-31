"""Analyse all runs under a results folder and produce tables, tests and figures.

usage: python3 analyze.py results            (writes into results/analysis/)
Needs: pandas numpy scipy matplotlib  (pip install -r requirements.txt)
"""
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C

ROOT = sys.argv[1] if len(sys.argv) > 1 else "results"
OUT = os.path.join(ROOT, "analysis")
os.makedirs(OUT, exist_ok=True)
ORDER = ["reactive", "predictive", "selfheal", "integrated",
         "no_arbiter", "no_diagnosis", "no_failure_pred", "no_uncertainty"]
EXPECTED = {"crash": {"component_fault"}, "cpu_hog": {"component_fault"},
            "network_delay": {"network_fault"}, "dependency_outage": {"dependency_fault"},
            "memory_leak": {"predicted_oom", "component_fault"}}
PALETTE = {"reactive": "#9aa0a6", "predictive": "#7baaf7", "selfheal": "#f6ae2d",
           "integrated": "#2a9d8f"}


def per_second(req, duration):
    req = req[req.t < duration].copy()
    req["s"] = req.t.astype(int)
    req["good"] = (req.status == 200) & (req.latency_ms <= C.SLO_LATENCY_MS)
    g = req.groupby("s").agg(n=("good", "size"), good=("good", "sum"),
                             p95=("latency_ms", lambda x: np.percentile(x, 95)))
    g = g.reindex(range(int(duration)), fill_value=0)
    g["ratio"] = np.where(g.n > 0, g.good / g.n.replace(0, 1), 1.0)
    return req, g


def analyse_run(d):
    meta = json.load(open(os.path.join(d, "meta.json")))
    dur = meta["duration"]
    req = pd.read_csv(os.path.join(d, "requests.csv"))
    ticks = pd.read_csv(os.path.join(d, "ticks.csv"))
    acts = pd.read_csv(os.path.join(d, "actions.csv"))
    faults = pd.read_csv(os.path.join(d, "faults.csv"))
    req, sec = per_second(req, dur)
    run = {"condition": meta["condition"], "seed": meta["seed"],
           "availability": 100 * req.good.mean(),
           "error_rate": 100 * (req.status != 200).mean(),
           "sla_viol_seconds": 100 * (sec.ratio < C.SLO_SECOND_GOOD_RATIO).mean(),
           "p95_ms": np.percentile(req.latency_ms, 95),
           "p99_ms": np.percentile(req.latency_ms, 99),
           "avg_replicas": ticks.replicas.mean(),
           "under_prov": 100 * (ticks.util > 0.9).mean(),
           "over_prov": 100 * (ticks.util < 0.3).mean(),
           "contradictions": meta.get("contradictions", 0),
           "restarts": sum(meta["restarts"].values()),
           "proactive_restarts": int((acts.action == "proactive_restart").sum()),
           "healer_actions": int(acts.actor.isin(["healer", "predictor"]).sum())}
    # forecast error at the horizon (only meaningful where a forecast drives scaling)
    tt = ticks.set_index(ticks.t.round().astype(int))
    tt = tt[~tt.index.duplicated()]
    fut = tt.rps.reindex(tt.index + C.FORECAST_HORIZON_S).values
    m = ~np.isnan(fut)
    run["forecast_mae"] = float(np.mean(np.abs(tt.forecast.values[m] - fut[m])))
    now_v = tt.rps.values
    run["naive_mae"] = float(np.mean(np.abs(now_v[m] - fut[m])))  # "next = current" baseline
    run["forecast_smape"] = float(100 * np.mean(2 * np.abs(tt.forecast.values[m] - fut[m]) /
                                                (np.abs(tt.forecast.values[m]) + np.abs(fut[m]) + 1e-9)))
    # per-fault metrics
    rows = []
    fl = faults.sort_values("t").reset_index(drop=True)
    for i, f in fl.iterrows():
        start = f.t
        nxt = fl.t.iloc[i + 1] if i + 1 < len(fl) else dur
        end = min(nxt, dur - 10)
        w = sec.iloc[int(start):int(end)]
        bad = int((w.n - w.good).sum())
        # time-to-recover: first second after which 10 s are continuously healthy
        healthy = (w.ratio >= C.SLO_SECOND_GOOD_RATIO).values
        ttr = None
        for k in range(len(healthy) - 9):
            if healthy[k:k + 10].all() and (k > 0 or bad == 0):
                ttr = k
                break
        if bad == 0:
            ttr = 0
        if ttr is None:
            ttr = len(healthy)
        # detection may legitimately come late for slow faults (memory leak), so look
        # up to 120 s ahead even if the next fault has already started
        det_end = max(end, start + 120)
        a = acts[(acts.t >= start) & (acts.t < det_end) & (acts.action == "detected") &
                 (acts.target.isin([f.target, "all"]) | (f.type == "dependency_outage"))]
        a_heal = a[a.actor.isin(["healer", "predictor"])]
        # the platform supervisor only "detects" process exits (crash, or OOM after a leak)
        a_sup = a[(a.actor == "supervisor") & (f.type in ("crash", "memory_leak"))]
        det = (a_heal.t.iloc[0] - start) if len(a_heal) else \
            (a_sup.t.iloc[0] - start if len(a_sup) else np.nan)
        diag = a_heal.diagnosis.iloc[0] if len(a_heal) else ""
        rows.append({"condition": meta["condition"], "seed": meta["seed"], "fault": f.type,
                     "target": f.target, "start": start, "bad_requests": bad,
                     "ttr_s": ttr, "detect_s": det, "first_diagnosis": diag,
                     "diag_correct": (diag in EXPECTED[f.type]) if diag else np.nan,
                     "injected": f.status})
    return run, rows, (sec, ticks, acts, faults, dur)


def ci95(x):
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    if len(x) < 2:
        return np.nan
    return stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / np.sqrt(len(x))


def main():
    runs, frows, timelines = [], [], {}
    for d in sorted(glob.glob(os.path.join(ROOT, "*", "seed*"))):
        if not os.path.exists(os.path.join(d, "meta.json")):
            print("skip (incomplete):", d)
            continue
        r, fr, tl = analyse_run(d)
        runs.append(r)
        frows += fr
        timelines[(r["condition"], r["seed"])] = tl
    if not runs:
        sys.exit("no completed runs found under " + ROOT)
    R, F = pd.DataFrame(runs), pd.DataFrame(frows)
    R.to_csv(os.path.join(OUT, "runs.csv"), index=False)
    F.to_csv(os.path.join(OUT, "faults.csv"), index=False)
    conds = [c for c in ORDER if c in set(R.condition)]

    metrics = ["availability", "sla_viol_seconds", "error_rate", "p95_ms", "avg_replicas",
               "under_prov", "over_prov", "forecast_mae", "naive_mae", "contradictions", "restarts",
               "proactive_restarts"]
    lines = []
    for c in conds:
        sub = R[R.condition == c]
        row = {"condition": c, "n_seeds": len(sub)}
        for m in metrics:
            row[m] = f"{sub[m].mean():.2f} ± {ci95(sub[m]):.2f}"
        lines.append(row)
    T = pd.DataFrame(lines)
    T.to_csv(os.path.join(OUT, "table_main.csv"), index=False)

    ft = F.groupby(["condition", "fault"]).agg(ttr_mean=("ttr_s", "mean"), bad_mean=("bad_requests", "mean"),
                                               detect_mean=("detect_s", "mean"),
                                               diag_acc=("diag_correct", "mean")).reset_index()
    ft.to_csv(os.path.join(OUT, "table_faults.csv"), index=False)

    # significance tests: integrated vs every other condition (Welch + paired by seed)
    out = ["Significance tests (integrated vs others)", "=" * 60]
    if "integrated" in conds:
        I = R[R.condition == "integrated"].set_index("seed")
        for c in conds:
            if c == "integrated":
                continue
            O = R[R.condition == c].set_index("seed")
            for m in ["availability", "sla_viol_seconds", "p95_ms", "under_prov", "avg_replicas"]:
                if len(I) > 1 and len(O) > 1:
                    w = stats.ttest_ind(I[m], O[m], equal_var=False)
                    common = I.index.intersection(O.index)
                    pt = stats.ttest_rel(I.loc[common, m], O.loc[common, m]) if len(common) > 1 else None
                    out.append(f"{m:18s} integrated={I[m].mean():8.2f}  {c:16s}={O[m].mean():8.2f}  "
                               f"Welch t={w.statistic:6.2f} p={w.pvalue:.4g}" +
                               (f"   paired t={pt.statistic:6.2f} p={pt.pvalue:.4g}" if pt else ""))
            out.append("-" * 60)
    open(os.path.join(OUT, "stats_tests.txt"), "w").write("\n".join(out))

    # ---- figures
    def bar(metric, ylabel, fname, subset):
        cs = [c for c in subset if c in conds]
        if not cs:
            return
        fig, ax = plt.subplots(figsize=(6.4, 3.6))
        means = [R[R.condition == c][metric].mean() for c in cs]
        errs = [ci95(R[R.condition == c][metric]) for c in cs]
        ax.bar(cs, means, yerr=errs, capsize=4, color=[PALETTE.get(c, "#bbbbbb") for c in cs])
        ax.set_ylabel(ylabel)
        ax.spines[["top", "right"]].set_visible(False)
        if metric == "availability":
            lo = min(m - (e if e == e else 0) for m, e in zip(means, errs))
            ax.set_ylim(max(0, lo - 3), 100)
        plt.xticks(rotation=20)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, fname), dpi=200)
        plt.close(fig)

    main4 = ["reactive", "predictive", "selfheal", "integrated"]
    abl = ["integrated", "no_arbiter", "no_diagnosis", "no_failure_pred", "no_uncertainty"]
    bar("availability", "Availability (% good requests)", "fig1_availability.png", main4)
    bar("sla_viol_seconds", "SLA-violating seconds (%)", "fig2_sla_violations.png", main4)
    bar("under_prov", "Under-provisioned time (%)", "fig3_under_provisioned.png", main4)
    bar("availability", "Availability (% good requests)", "fig5_ablation_availability.png", abl)

    if len(F):
        piv = F.groupby(["fault", "condition"]).ttr_s.mean().unstack()
        piv = piv[[c for c in conds if c in piv.columns and c in main4]]
        if len(piv.columns):
            ax = piv.plot.bar(figsize=(7.5, 3.8), color=[PALETTE.get(c) for c in piv.columns])
            ax.set_ylabel("Time to recover (s)")
            ax.set_xlabel("")
            ax.spines[["top", "right"]].set_visible(False)
            plt.xticks(rotation=15)
            plt.tight_layout()
            plt.savefig(os.path.join(OUT, "fig4_ttr_by_fault.png"), dpi=200)
            plt.close()

    # timeline of one run per main condition (seed with smallest id)
    for c in [c for c in main4 if c in conds]:
        seed = min(s for (cc, s) in timelines if cc == c)
        sec, ticks, acts, faults, dur = timelines[(c, seed)]
        fig, axs = plt.subplots(3, 1, figsize=(10, 6.5), sharex=True)
        axs[0].plot(ticks.t, ticks.rps, lw=1, label="observed req/s", color="#444")
        axs[0].plot(ticks.t, ticks.forecast, lw=1, ls="--", label=f"forecast (+{C.FORECAST_HORIZON_S}s)",
                    color="#2a9d8f")
        axs[0].set_ylabel("req/s")
        axs[0].legend(loc="upper left", fontsize=8, frameon=False)
        axs[1].step(ticks.t, ticks.replicas, where="post", label="replicas", color="#264653")
        axs[1].step(ticks.t, ticks.rotation_n, where="post", label="in rotation", color="#f6ae2d")
        axs[1].set_ylabel("replicas")
        axs[1].legend(loc="upper left", fontsize=8, frameon=False)
        axs[2].plot(sec.index, 100 * sec.ratio, lw=1, color="#e76f51")
        axs[2].set_ylabel("good req (%)")
        axs[2].set_ylim(-2, 102)
        axs[2].set_xlabel("time (s)")
        for _, f in faults.iterrows():
            for ax in axs:
                ax.axvspan(f.t, f.t + max(f.dur, 3), color="#e63946", alpha=0.08)
            axs[0].text(f.t, axs[0].get_ylim()[1] * 0.95, f.type.replace("_", " "), fontsize=7,
                        color="#e63946", va="top")
        for _, a in acts[acts.actor.isin(["healer", "predictor"]) &
                         ~acts.action.isin(["detected", "verified", "readmit"])].iterrows():
            axs[2].axvline(a.t, color="#2a9d8f", lw=0.6, alpha=0.6)
        for ax in axs:
            ax.spines[["top", "right"]].set_visible(False)
        fig.suptitle(f"{c} (seed {seed}) — red bands: injected faults, green lines: healing actions",
                     fontsize=10)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, f"fig6_timeline_{c}.png"), dpi=200)
        plt.close(fig)

    pd.set_option("display.width", 200)
    print(T.to_string(index=False))
    print()
    print(ft.to_string(index=False))
    print()
    print("\n".join(out))
    print("\nwrote tables + figures to", OUT)


if __name__ == "__main__":
    main()
