"""
hive_pipeline.py

Python wrapper for Apache Hive pipeline
Executes HiveQL scripts and stores results into PostgreSQL
"""
import time
import subprocess
from datetime import datetime

import db


HIVE_DIR = "hive_queries"


# ─────────────────────────────────────────────
# RUN HIVE SCRIPT
# ─────────────────────────────────────────────

def run_hive_script(script_name):

    script_path = f"{HIVE_DIR}/{script_name}"

    result = subprocess.run(
        ["hive", "-f", script_path],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"\n[Hive ERROR]\n{result.stderr}")

    return result


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def run(log_file: str, batch_size: int = 10000, verbose: bool = True):

    print("\n" + "=" * 60)
    print(" PIPELINE: Apache Hive")
    print("=" * 60)

    start_time = time.time()

    # Create external table
    run_hive_script("create_table.hql")

    # Execute analytical queries
    run_hive_script("query1.hql")
    run_hive_script("query2.hql")
    run_hive_script("query3.hql")

    runtime = time.time() - start_time

    # Store run metadata
    run_id = db.insert_run(
        pipeline="Hive",
        run_timestamp=datetime.utcnow(),
        batch_size=batch_size,
        total_batches=0,
        total_records=0,
        malformed_count=0,
        avg_batch_size=0,
        runtime_seconds=runtime
    )

    print(f"\n[Hive] Runtime : {runtime:.3f}s")
    print(f"[Hive] Run ID : {run_id}")

    return run_id