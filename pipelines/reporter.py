"""
reporter.py – Reads ETL results from the relational DB and renders a formatted report.

Shows:
  - Run metadata (pipeline, runtime, batch stats)
  - Query 1: Daily Traffic Summary
  - Query 2: Top 20 Requested Resources
  - Query 3: Hourly Error Analysis
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
import db

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def _table(headers, rows, fmt="grid"):
    if HAS_TABULATE:
        return tabulate(rows, headers=headers, tablefmt=fmt)
    # Fallback: simple fixed-width
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                  for i, h in enumerate(headers)]
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    lines = [sep]
    lines.append("| " + " | ".join(str(h).ljust(w) for h, w in zip(headers, col_widths)) + " |")
    lines.append(sep)
    for row in rows:
        lines.append("| " + " | ".join(str(c).ljust(w) for c, w in zip(row, col_widths)) + " |")
    lines.append(sep)
    return "\n".join(lines)


def print_run_report(run_id: int):
    """Print a full report for the given run_id."""
    run = db.fetch_run(run_id)
    q1  = db.fetch_q1(run_id)
    q2  = db.fetch_q2(run_id)
    q3  = db.fetch_q3(run_id)

    width = 70
    print("\n" + "=" * width)
    print(f"  ETL RUN REPORT – Run #{run_id}")
    print("=" * width)
    print(f"  Pipeline         : {run['pipeline']}")
    print(f"  Run Timestamp    : {run['run_timestamp']} UTC")
    print(f"  Batch Size       : {run['batch_size']:,}")
    print(f"  Total Batches    : {run['total_batches']:,}")
    print(f"  Total Records    : {run['total_records']:,}")
    print(f"  Malformed Records: {run['malformed_count']:,}")
    print(f"  Avg Batch Size   : {run['avg_batch_size']:,.1f}")
    print(f"  Runtime          : {run['runtime_seconds']:.3f} seconds")
    print("=" * width)

    # ── Query 1 ───────────────────────────────────────────────────────────
    print(f"\n  QUERY 1: Daily Traffic Summary  ({len(q1)} rows)")
    print("-" * width)
    if q1:
        # Show first 30 rows to keep output manageable
        sample = q1[:30]
        rows = [(r["log_date"], r["status_code"],
                 f"{r['request_count']:,}", f"{r['total_bytes']:,}")
                for r in sample]
        print(_table(
            ["log_date", "status_code", "request_count", "total_bytes"],
            rows
        ))
        if len(q1) > 30:
            print(f"  ... ({len(q1) - 30} more rows omitted)")
    else:
        print("  (no results)")

    # ── Query 2 ───────────────────────────────────────────────────────────
    print(f"\n  QUERY 2: Top 20 Requested Resources  ({len(q2)} rows)")
    print("-" * width)
    if q2:
        rows = [(r["resource_path"][:45],
                 f"{r['request_count']:,}",
                 f"{r['total_bytes']:,}",
                 r["distinct_host_count"])
                for r in q2]
        print(_table(
            ["resource_path", "request_count", "total_bytes", "distinct_hosts"],
            rows
        ))
    else:
        print("  (no results)")

    # ── Query 3 ───────────────────────────────────────────────────────────
    print(f"\n  QUERY 3: Hourly Error Analysis  ({len(q3)} rows)")
    print("-" * width)
    if q3:
        sample = q3[:30]
        rows = [(r["log_date"], r["log_hour"],
                 f"{r['error_request_count']:,}",
                 f"{r['total_request_count']:,}",
                 f"{r['error_rate']*100:.2f}%",
                 r["distinct_error_hosts"])
                for r in sample]
        print(_table(
            ["log_date", "hour", "error_reqs", "total_reqs", "error_rate", "error_hosts"],
            rows
        ))
        if len(q3) > 30:
            print(f"  ... ({len(q3) - 30} more rows omitted)")
    else:
        print("  (no results)")

    print("\n" + "=" * width)
    print("  END OF REPORT")
    print("=" * width + "\n")


def print_all_runs():
    """Print a summary table of all ETL runs."""
    runs = db.list_runs()
    if not runs:
        print("No ETL runs found in the database.")
        return
    print("\n=== ALL ETL RUNS ===")
    rows = [(r["run_id"], r["pipeline"], r["run_timestamp"][:19],
             r["batch_size"], r["total_records"],
             r["malformed_count"], f"{r['runtime_seconds']:.2f}s")
            for r in runs]
    print(_table(
        ["run_id", "pipeline", "run_timestamp", "batch_size",
         "total_records", "malformed", "runtime"],
        rows
    ))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print_run_report(int(sys.argv[1]))
    else:
        print_all_runs()
