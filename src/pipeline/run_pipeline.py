import subprocess
import sys
import time
import csv
import os
from datetime import datetime




STEPS = [
    "pipeline.dim_events",                   
    "pipeline.fact_tasks_staging",                   
    "pipeline.dim_labels",                   
    "pipeline.dim_buckets",                  
    "pipeline.fact_sub_tasks",
    "pipeline.fact_tasks_prod",    
]


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "pipeline_log.csv")
FIELDNAMES = ["run_id", "step", "start", "end", "duration_s", "status", "error"]


def ensure_log_header() -> None:
    file_exists = os.path.isfile(LOG_FILE)
    if not file_exists:
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def log_step(run_id: str, step: str, start: datetime, end: datetime, status: str, error: str = "") -> None:
    ensure_log_header()
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow({
            "run_id": run_id,
            "step": step,
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_s": round((end - start).total_seconds(), 1),
            "status": status,
            "error": (error or "").strip()
        })


def run_step(module: str) -> None:
    print(f"\n=== Running: {module} ===")
    cmd = [sys.executable, "-m", module]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        print(result.stdout, end="")

    if result.returncode != 0:
        # Include stderr in the raised error so it gets logged
        err = (result.stderr or "").strip()
        if result.stderr:
            print(result.stderr, end="")
        raise RuntimeError(f"Exit code {result.returncode}. {err}".strip())


def main():
    # Unique id per pipeline run
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Pipeline started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run ID: {run_id}")
    print(f"Logging to: {LOG_FILE}")

    pipeline_start = time.time()

    for step in STEPS:
        step_start_dt = datetime.now()
        try:
            run_step(step)
            step_end_dt = datetime.now()
            log_step(run_id, step, step_start_dt, step_end_dt, "success", "")
        except Exception as e:
            step_end_dt = datetime.now()
            log_step(run_id, step, step_start_dt, step_end_dt, "failed", str(e))
            # Stop the pipeline on first failure
            raise SystemExit(f"FAILED: {step} | {e}")

    total_elapsed = time.time() - pipeline_start
    print(f"\nPipeline complete. Total time: {total_elapsed:.1f}s")


if __name__ == "__main__":
    main()