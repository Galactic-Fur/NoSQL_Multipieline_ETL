"""
mongodb_pipeline.py – MongoDB aggregation pipeline for NASA HTTP log analytics.

ETL flow:
  1. Parse raw log lines using the shared parser.
  2. Insert parsed records into a MongoDB collection in batches.
  3. Run MongoDB aggregation pipelines for each query.
  4. Retrieve results and load them into SQLite (relational store).

Requires: pymongo + a running MongoDB instance.
Falls back to a pure-Python in-memory simulation if MongoDB is unavailable,
so the prototype always produces output for the demo.
"""
import sys
import os
import time
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from parser import parse_file_in_batches
import db as reldb

# ── Try to import pymongo ──────────────────────────────────────────────────────
try:
    from pymongo import MongoClient, DESCENDING
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False


MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "nasa_logs"
COLLECTION = "http_logs"


def _get_mongo_collection():
    """Return MongoDB collection, or raise RuntimeError if unavailable."""
    if not PYMONGO_AVAILABLE:
        raise RuntimeError("pymongo not installed")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    client.admin.command("ping")  # will raise if no server
    db_obj = client[MONGO_DB]
    return db_obj[COLLECTION], client


# ── MongoDB Aggregation Pipelines ─────────────────────────────────────────────

def agg_q1():
    """Query 1 – Daily Traffic Summary."""
    return [
        {"$group": {
            "_id": {"log_date": "$log_date", "status_code": "$status_code"},
            "request_count": {"$sum": 1},
            "total_bytes":   {"$sum": "$bytes_transferred"},
        }},
        {"$project": {
            "_id": 0,
            "log_date":      "$_id.log_date",
            "status_code":   "$_id.status_code",
            "request_count": 1,
            "total_bytes":   1,
        }},
        {"$sort": {"log_date": 1, "status_code": 1}},
    ]


def agg_q2():
    """Query 2 – Top 20 Requested Resources."""
    return [
        {"$group": {
            "_id": "$resource_path",
            "request_count":       {"$sum": 1},
            "total_bytes":         {"$sum": "$bytes_transferred"},
            "distinct_host_count": {"$addToSet": "$host"},
        }},
        {"$project": {
            "_id": 0,
            "resource_path":       "$_id",
            "request_count":       1,
            "total_bytes":         1,
            "distinct_host_count": {"$size": "$distinct_host_count"},
        }},
        {"$sort":  {"request_count": -1}},
        {"$limit": 20},
    ]


def agg_q3():
    """Query 3 – Hourly Error Analysis."""
    return [
        {"$facet": {
            "errors": [
                {"$match": {"status_code": {"$gte": 400, "$lte": 599}}},
                {"$group": {
                    "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
                    "error_request_count": {"$sum": 1},
                    "distinct_error_hosts": {"$addToSet": "$host"},
                }},
            ],
            "totals": [
                {"$group": {
                    "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
                    "total_request_count": {"$sum": 1},
                }},
            ],
        }},
        # Unwind and merge
        {"$project": {"all": {"$concatArrays": ["$errors", "$totals"]}}},
        {"$unwind": "$all"},
        {"$replaceRoot": {"newRoot": "$all"}},
        {"$group": {
            "_id": "$_id",
            "error_request_count": {"$sum": {"$ifNull": ["$error_request_count", 0]}},
            "total_request_count": {"$sum": {"$ifNull": ["$total_request_count", 0]}},
            "distinct_error_hosts": {"$first": "$distinct_error_hosts"},
        }},
        {"$project": {
            "_id": 0,
            "log_date":   "$_id.log_date",
            "log_hour":   "$_id.log_hour",
            "error_request_count": 1,
            "total_request_count": 1,
            "distinct_error_hosts": {"$size": {"$ifNull": ["$distinct_error_hosts", []]}},
            "error_rate": {
                "$cond": [
                    {"$gt": ["$total_request_count", 0]},
                    {"$divide": ["$error_request_count", "$total_request_count"]},
                    0.0
                ]
            },
        }},
        {"$sort": {"log_date": 1, "log_hour": 1}},
    ]


# ── Fallback: pure-Python simulation (same logic, no MongoDB) ─────────────────

def _simulate_mongo(all_records, verbose):
    """Simulate MongoDB aggregation entirely in Python when no server is available."""
    print("  [MongoDB] ⚠ No MongoDB server found – running in-memory simulation.")

    # Q1
    q1_acc = defaultdict(lambda: {"request_count": 0, "total_bytes": 0})
    for r in all_records:
        key = (r.log_date, r.status_code)
        q1_acc[key]["request_count"] += 1
        q1_acc[key]["total_bytes"] += r.bytes_transferred
    results_q1 = [
        {"log_date": k[0], "status_code": k[1],
         "request_count": v["request_count"], "total_bytes": v["total_bytes"]}
        for k, v in q1_acc.items()
    ]

    # Q2
    q2_acc = defaultdict(lambda: {"request_count": 0, "total_bytes": 0, "hosts": set()})
    for r in all_records:
        q2_acc[r.resource_path]["request_count"] += 1
        q2_acc[r.resource_path]["total_bytes"] += r.bytes_transferred
        q2_acc[r.resource_path]["hosts"].add(r.host)
    results_q2 = sorted([
        {"resource_path": k, "request_count": v["request_count"],
         "total_bytes": v["total_bytes"], "distinct_host_count": len(v["hosts"])}
        for k, v in q2_acc.items()
    ], key=lambda x: -x["request_count"])[:20]

    # Q3
    q3_total = defaultdict(int)
    q3_error = defaultdict(int)
    q3_ehosts = defaultdict(set)
    for r in all_records:
        key = (r.log_date, r.log_hour)
        q3_total[key] += 1
        if 400 <= r.status_code <= 599:
            q3_error[key] += 1
            q3_ehosts[key].add(r.host)
    all_keys = set(q3_total) | set(q3_error)
    results_q3 = []
    for k in all_keys:
        ec = q3_error.get(k, 0)
        tc = q3_total.get(k, 0)
        results_q3.append({
            "log_date": k[0], "log_hour": k[1],
            "error_request_count": ec,
            "total_request_count": tc,
            "error_rate": round(ec / tc, 6) if tc else 0.0,
            "distinct_error_hosts": len(q3_ehosts.get(k, set())),
        })

    return results_q1, results_q2, results_q3


# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────

def run(log_file: str, batch_size: int = 10000, verbose: bool = True):
    """
    Execute the full MongoDB ETL pipeline.
    Returns run_id on success.
    """
    print(f"\n{'='*60}")
    print("  PIPELINE: MongoDB Aggregation")
    print(f"{'='*60}")
    print(f"  Log file  : {log_file}")
    print(f"  Batch size: {batch_size:,}")

    reldb.init_schema()

    total_records = 0
    total_malformed = 0
    total_batches = 0
    collection = None
    mongo_client = None
    use_real_mongo = False

    # Try connecting to MongoDB
    try:
        collection, mongo_client = _get_mongo_collection()
        collection.drop()  # fresh run
        use_real_mongo = True
        print("  [MongoDB] Connected to mongodb://localhost:27017")
    except Exception as e:
        print(f"  [MongoDB] Cannot connect ({e}); will use in-memory fallback.")

    start_time = time.time()
    all_records_fallback = []  # for fallback mode

    for batch_id, records, malformed_in_batch in parse_file_in_batches(log_file, batch_size):
        total_batches = batch_id
        total_records += len(records)
        total_malformed += malformed_in_batch

        if verbose:
            print(f"  [MongoDB] Batch {batch_id:4d}: {len(records):,} records "
                  f"({malformed_in_batch} malformed)")

        if use_real_mongo:
            docs = [
                {
                    "host":             r.host,
                    "timestamp":        r.timestamp,
                    "log_date":         r.log_date,
                    "log_hour":         r.log_hour,
                    "http_method":      r.http_method,
                    "resource_path":    r.resource_path,
                    "protocol_version": r.protocol_version,
                    "status_code":      r.status_code,
                    "bytes_transferred": r.bytes_transferred,
                }
                for r in records
            ]
            collection.insert_many(docs, ordered=False)
        else:
            all_records_fallback.extend(records)

    # ── Run aggregations ────────────────────────────────────────────────────
    if use_real_mongo:
        print("\n  [MongoDB] Running aggregation pipelines ...")
        raw_q1 = list(collection.aggregate(agg_q1(), allowDiskUse=True))
        results_q1 = [dict(r) for r in raw_q1]

        raw_q2 = list(collection.aggregate(agg_q2(), allowDiskUse=True))
        results_q2 = [dict(r) for r in raw_q2]

        raw_q3 = list(collection.aggregate(agg_q3(), allowDiskUse=True))
        results_q3 = []
        for r in raw_q3:
            r["error_rate"] = round(r.get("error_rate", 0.0), 6)
            results_q3.append(dict(r))

        mongo_client.close()
    else:
        print("\n  [MongoDB] Running in-memory aggregation (fallback) ...")
        results_q1, results_q2, results_q3 = _simulate_mongo(all_records_fallback, verbose)

    runtime = time.time() - start_time
    avg_batch = total_records / total_batches if total_batches > 0 else 0

    print(f"\n  [MongoDB] Records processed : {total_records:,}")
    print(f"  [MongoDB] Malformed records  : {total_malformed:,}")
    print(f"  [MongoDB] Batches            : {total_batches:,}")
    print(f"  [MongoDB] Avg batch size     : {avg_batch:,.1f}")
    print(f"  [MongoDB] Runtime            : {runtime:.3f}s")

    # ── Load results into relational DB ────────────────────────────────────
    print("\n  [MongoDB] Loading results into SQLite ...")
    run_timestamp = datetime.utcnow().isoformat()
    run_id = reldb.insert_run(
        pipeline="MongoDB",
        run_timestamp=run_timestamp,
        batch_size=batch_size,
        total_batches=total_batches,
        total_records=total_records,
        malformed_count=total_malformed,
        avg_batch_size=avg_batch,
        runtime_seconds=runtime,
    )
    reldb.insert_q1(run_id, "MongoDB", results_q1)
    reldb.insert_q2(run_id, "MongoDB", results_q2)
    reldb.insert_q3(run_id, "MongoDB", results_q3)

    print(f"  [MongoDB] Run ID: {run_id} – results stored successfully.")
    return run_id
