# Prediction-Driven Self-Healing on a Real Cloud VM

Source code, raw data and analysis for the study "Prediction-Driven Self-Healing on a Real Cloud VM: A Measured Study of Coordinated Diagnosis, Failure Prediction and Forecast-Based Autoscaling".

## What this is
A small testbed with replicas, a gateway, a fault-injecting proxy and Redis, controlled by a layered controller (forecasting, failure prediction, rule-based diagnosis, severity ladder, arbiter). Eight controller configurations were run 5 times each (40 runs, 12 minutes each, 8 hours of measured operation) with five injected faults per run: crash, CPU hog, network delay, dependency outage and memory leak.

## Environment
Azure VM Standard_B2as_v2 (2 vCPU, 8 GiB), East Asia, Ubuntu 24.04, Python 3.12.3, Redis 7.0.15. See `results/environment.txt`.

## Files
- `config.py`, `app.py`, `faultproxy.py`, `gateway.py`, `loadgen.py`, `minikv.py`, `run.py`: testbed and controller
- `run_all.py`: runs all configurations and seeds
- `analyze.py`: produces tables, statistical tests and figures
- `results/`: driver logs (`run_all.log`, `run_all2.log`), environment record, analysis output in `results/analysis/`

## How to run
```
python3 run_all.py
python3 analyze.py results
```

## Note
Seeds 1-3 were run twice because of an overlapping launch; the first run was discarded and the clean re-run (`run_all2.log`) is used in the paper.
