"""Shared configuration for the prediction-driven self-healing testbed.

Everything runs on ONE machine (your free cloud VM) and talks over localhost:
    loadgen -> gateway -> faultproxy -> app_i -> faultproxy -> redis
The fault proxy sits in front of every replica and in front of Redis, so we can
inject network delay and dependency outages without touching the apps.
"""

HOST = "127.0.0.1"

# --- ports -----------------------------------------------------------------
GATEWAY_PORT = 8080
PROXY_ADMIN_PORT = 9000
REDIS_PORT = 6380            # private redis-server started by the runner
REDIS_PROXY_PORT = 9379      # apps reach redis through the fault proxy

# replica name -> (real app port, proxied port used by gateway + controller)
APPS = {f"app{i}": (8100 + i, 9100 + i) for i in range(1, 6)}
MIN_REPLICAS = 2             # app1, app2 always exist; app3..app5 are scale-out
MAX_REPLICAS = 5

# --- application model --------------------------------------------------------
# Each replica is worker-pool bound (like gunicorn with N workers), so adding a
# replica adds real capacity even on a 1-2 core VM.
APP_WORKERS = 2
APP_CPU_MS = 2               # CPU work per request
APP_IO_MS = 45               # simulated I/O wait per request (DB/disk/remote call)
APP_WARMUP_S = 3.0           # start-up / warm-up time after every (re)start
APP_MEM_LIMIT_MB = 200       # replica is "OOM-killed" (exit 137) above this RSS
REDIS_TIMEOUT_S = 0.5
# approx. max req/s one replica serves = workers / service time
CAP_PER_REPLICA = APP_WORKERS / ((APP_CPU_MS + APP_IO_MS + 3) / 1000.0)  # ~40 rps

# --- load ----------------------------------------------------------------------
BASE_RPS = 45                # mean offered load; wave +-30 %, two 2x bursts
CLIENT_TIMEOUT_S = 2.0

# --- platform defaults (ALL conditions) ----------------------------------------
SUPERVISOR_RESTART_DELAY_S = 1.0   # = Docker restart:on-failure / systemd Restart=
LB_HEALTH_FAILS = 2                # LB drops a replica after N failed health checks

# --- SLO -----------------------------------------------------------------------
SLO_LATENCY_MS = 250         # request is "good" if HTTP 200 and latency <= this (as in the simulation paper)
SLO_SECOND_GOOD_RATIO = 0.95 # a second violates the SLA if its good ratio < this

# --- scaler ---------------------------------------------------------------------
SCALE_UP_UTIL = 0.75
SCALE_DOWN_UTIL = 0.30
SCALE_UP_COOLDOWN_S = 8
SCALE_DOWN_COOLDOWN_S = 30
FORECAST_HORIZON_S = 15      # >= warm-up + detection, so capacity is ready in time
FORECAST_ALPHA = 0.5         # Holt level smoothing
FORECAST_BETA = 0.1          # Holt trend smoothing
FORECAST_PHI = 0.85          # trend damping (damped-trend Holt; limits overshoot on bursts)
FORECAST_Z = 1.28            # uncertainty buffer: one-sided 90 % band

# --- healer -----------------------------------------------------------------------
TICK_S = 1.0
PROBE_TIMEOUT_S = 1.0
LAT_ANOMALY_MS = 150         # health-probe latency above this = slow path
CPU_ANOMALY = 0.60           # replica CPU (cores) above this = hot
DETECT_STREAK = 2            # consecutive anomalous ticks before acting
VERIFY_WAIT_S = 5.0          # wait after an action, then verify it worked
MAX_ESCALATIONS = 4
HEALTHY_TICKS_TO_UNDO = 5    # re-admit rerouted replica / lift isolation

# --- failure predictor (memory exhaustion) ---------------------------------------
MEM_WINDOW_S = 15            # regression window for memory slope
MEM_MIN_SLOPE_MB_S = 0.3
MEM_ETA_ACT_S = 30           # act when predicted time-to-OOM below this
