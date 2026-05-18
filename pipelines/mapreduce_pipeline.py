"""
mapreduce_pipeline.py – Python MapReduce pipeline for NASA HTTP log analytics.

Implements the classic MapReduce paradigm using Python generators:
  - Map phase: parse each record and emit (key, value) pairs
  - Shuffle phase: group values by key (in-memory sort)
  - Reduce phase: aggregate grouped values

This mirrors how Hadoop MapReduce works, just run locally.
"""
import sys
import os
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from parser import parse_file_in_batches
import db


# ─────────────────────────────────────────────
#  MAP FUNCTIONS
# ─────────────────────────────────────────────

def map_q1(record):
    """
    Mapper for Query 1 – Daily Traffic Summary.
    Emits: ((log_date, status_code), (1, bytes_transferred))
    """
    yield (record.log_date, record.status_code), (1, record.bytes_transferred)


def map_q2(record):
    """
    Mapper for Query 2 – Top Requested Resources.
    Emits: (resource_path, (1, bytes_transferred, host))
    """
    yield record.resource_path, (1, record.bytes_transferred, record.host)


def map_q3(record):
    """
    Mapper for Query 3 – Hourly Error Analysis.
    Emits: ((log_date, log_hour), (is_error, 1, error_host_or_None))
    """
    is_error = 1 if 400 <= record.status_code <= 599 else 0
    error_host = record.host if is_error else None
    yield (record.log_date, record.log_hour), (is_error, 1, error_host)


# ─────────────────────────────────────────────
#  REDUCE FUNCTIONS
# ─────────────────────────────────────────────

def reduce_q1(key, values):
    """
    Reducer for Query 1.
    Returns dict with log_date, status_code, request_count, total_bytes.
    """
    total_requests = 0
    total_bytes = 0
    for (count, byt) in values:
        total_requests += count
        total_bytes += byt
    log_date, status_code = key
    return {
        "log_date": log_date,
        "status_code": status_code,
        "request_count": total_requests,
        "total_bytes": total_bytes,
    }


def reduce_q2(key, values):
    """
    Reducer for Query 2.
    Returns dict with resource_path, request_count, total_bytes, distinct_host_count.
    """
    total_requests = 0
    total_bytes = 0
    hosts = set()
    for (count, byt, host) in values:
        total_requests += count
        total_bytes += byt
        if host:
            hosts.add(host)
    return {
        "resource_path": key,
        "request_count": total_requests,
        "total_bytes": total_bytes,
        "distinct_host_count": len(hosts),
    }


def reduce_q3(key, values):
    """
    Reducer for Query 3.
    Returns dict with log_date, log_hour, error counts, error_rate, distinct_error_hosts.
    """
    error_count = 0
    total_count = 0
    error_hosts = set()
    for (is_error, count, error_host) in values:
        error_count += is_error
        total_count += count
        if error_host:
            error_hosts.add(error_host)
    log_date, log_hour = key
    error_rate = (error_count / total_count) if total_count > 0 else 0.0
    return {
        "log_date": log_date,
        "log_hour": log_hour,
        "error_request_count": error_count,
        "total_request_count": total_count,
        "error_rate": round(error_rate, 6),
        "distinct_error_hosts": len(error_hosts),
    }


# ─────────────────────────────────────────────
#  SHUFFLE (group by key)
# ─────────────────────────────────────────────

def shuffle(mapped_pairs):
    """Group emitted (key, value) pairs by key."""
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─────────────────────────────────────────────
#  PIPELINE ENTRY POINT
# ─────────────────────────────────────────────

def run(log_file: str, batch_size: int = 10000, verbose: bool = True):
    """
    Execute the full MapReduce ETL pipeline.
    Returns run_id on success.
    """
    print(f"\n{'='*60}")
    print("  PIPELINE: MapReduce (Python)")
    print(f"{'='*60}")
    print(f"  Log file  : {log_file}")
    print(f"  Batch size: {batch_size:,}")

    db.init_schema()

    # Accumulators for in-memory MapReduce
    # (For large datasets these would be spilled to disk)
    emit_q1 = []
    emit_q2 = []
    emit_q3 = []

    total_records = 0
    total_malformed = 0
    total_batches = 0

    start_time = time.time()

    for batch_id, records, malformed_in_batch in parse_file_in_batches(log_file, batch_size):
        total_batches = batch_id
        total_records += len(records)
        total_malformed += malformed_in_batch

        if verbose:
            print(f"  [MR] Batch {batch_id:4d}: {len(records):,} records "
                  f"({malformed_in_batch} malformed)")

        # MAP phase
        for rec in records:
            emit_q1.extend(map_q1(rec))
            emit_q2.extend(map_q2(rec))
            emit_q3.extend(map_q3(rec))

    # SHUFFLE phase
    print("\n  [MR] Shuffle phase ...")
    grouped_q1 = shuffle(emit_q1)
    grouped_q2 = shuffle(emit_q2)
    grouped_q3 = shuffle(emit_q3)

    # REDUCE phase
    print("  [MR] Reduce phase ...")
    results_q1 = [reduce_q1(k, v) for k, v in grouped_q1.items()]
    results_q2_all = [reduce_q2(k, v) for k, v in grouped_q2.items()]
    results_q2 = sorted(results_q2_all, key=lambda x: -x["request_count"])[:20]
    results_q3 = [reduce_q3(k, v) for k, v in grouped_q3.items()]

    runtime = time.time() - start_time
    avg_batch = total_records / total_batches if total_batches > 0 else 0

    print(f"\n  [MR] Records processed : {total_records:,}")
    print(f"  [MR] Malformed records  : {total_malformed:,}")
    print(f"  [MR] Batches            : {total_batches:,}")
    print(f"  [MR] Avg batch size     : {avg_batch:,.1f}")
    print(f"  [MR] Runtime            : {runtime:.3f}s")

    # LOAD into relational DB
    print("\n  [MR] Loading results into SQLite ...")
    run_timestamp = datetime.utcnow().isoformat()
    run_id = db.insert_run(
        pipeline="MapReduce",
        run_timestamp=run_timestamp,
        batch_size=batch_size,
        total_batches=total_batches,
        total_records=total_records,
        malformed_count=total_malformed,
        avg_batch_size=avg_batch,
        runtime_seconds=runtime,
    )
    db.insert_q1(run_id, "MapReduce", results_q1)
    db.insert_q2(run_id, "MapReduce", results_q2)
    db.insert_q3(run_id, "MapReduce", results_q3)

    print(f"  [MR] Run ID: {run_id} – results stored successfully.")
    return run_id
