"""
pig_pipeline.py

Python wrapper for Apache Pig pipeline
"""

import time
import subprocess
from datetime import datetime

import db


PIG_DIR = "pig_queries"


# ─────────────────────────────────────────────
# RUN PIG SCRIPT
# ─────────────────────────────────────────────

def run_pig_script(script_name, log_file):

    script_path = f"{PIG_DIR}/{script_name}"

    result = subprocess.run(
        [
            "pig",
            "-x",
            "local",
            script_path,
            "-param",
            f"INPUT={log_file}"
        ],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"\n[Pig ERROR]\n{result.stderr}")

    return result


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run(log_file: str, batch_size: int = 10000, verbose: bool = True):

    print("\n" + "=" * 60)
    print(" PIPELINE: Apache Pig")
    print("=" * 60)

    start_time = time.time()

    # Execute Pig scripts
    run_pig_script("query1.pig", log_file)
    run_pig_script("query2.pig", log_file)
    run_pig_script("query3.pig", log_file)

    runtime = time.time() - start_time

    # Store run metadata
    run_id = db.insert_run(
        pipeline="Pig",
        run_timestamp=datetime.utcnow(),
        batch_size=batch_size,
        total_batches=0,
        total_records=0,
        malformed_count=0,
        avg_batch_size=0,
        runtime_seconds=runtime
    )

    print(f"\n[Pig] Runtime : {runtime:.3f}s")
    print(f"[Pig] Run ID : {run_id}")

    return run_id