#!/usr/bin/env python3
"""
main.py - Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics
           DAS 839 - NoSQL Systems End Semester Project

Usage:
  python main.py run   --pipeline <mapreduce|mongodb> --log <file> [--batch-size N]
  python main.py report --run-id <id>
  python main.py report --all
  python main.py demo               # generate sample data and run both pipelines

Supported pipelines: mapreduce, mongodb
"""
import argparse
import sys
import os

# Make sure local imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "pipelines"))

PIPELINE_REGISTRY = {
    "mapreduce": "mapreduce_pipeline",
    "mongodb":   "mongodb_pipeline",
}


def cmd_run(args):
    pipeline_name = args.pipeline.lower()
    if pipeline_name not in PIPELINE_REGISTRY:
        print(f"ERROR: Unknown pipeline '{pipeline_name}'. "
              f"Choose from: {list(PIPELINE_REGISTRY)}")
        sys.exit(1)

    log_file = args.log
    if not os.path.isfile(log_file):
        print(f"ERROR: Log file not found: {log_file}")
        sys.exit(1)

    mod_name = PIPELINE_REGISTRY[pipeline_name]
    mod = __import__(mod_name)
    run_id = mod.run(log_file=log_file, batch_size=args.batch_size, verbose=not args.quiet)

    print(f"\n✓ Pipeline complete. Run ID = {run_id}")
    print(f"  View report: python main.py report --run-id {run_id}\n")
    return run_id


def cmd_report(args):
    import reporter
    if args.all:
        reporter.print_all_runs()
    elif args.run_id is not None:
        reporter.print_run_report(args.run_id)
    else:
        print("Specify --run-id <id> or --all")
        sys.exit(1)


def cmd_demo(args):
    """Generate sample data and run both pipelines."""
    print("\n" + "★" * 60)
    print("  DEMO MODE – Generating sample NASA log data and running pipelines")
    print("★" * 60)

    # Generate sample data
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    log_file = os.path.join(data_dir, "NASA_sample.log")

    sys.path.insert(0, data_dir)
    from generate_sample_logs import generate_logs
    generate_logs(n=args.records, output_path=log_file)

    batch_size = args.batch_size
    run_ids = []

    # Run MapReduce
    import mapreduce_pipeline
    rid = mapreduce_pipeline.run(log_file=log_file, batch_size=batch_size, verbose=True)
    run_ids.append(("MapReduce", rid))

    # Run MongoDB
    import mongodb_pipeline
    rid = mongodb_pipeline.run(log_file=log_file, batch_size=batch_size, verbose=True)
    run_ids.append(("MongoDB", rid))

    # Report both
    import reporter
    print("\n" + "★" * 60)
    print("  REPORTS")
    print("★" * 60)
    for pipeline_name, run_id in run_ids:
        reporter.print_run_report(run_id)

    reporter.print_all_runs()


def main():
    parser = argparse.ArgumentParser(
        description="Multi-Pipeline ETL Framework for NASA HTTP Log Analytics"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── run ──────────────────────────────────────────────────────────────
    p_run = subparsers.add_parser("run", help="Execute an ETL pipeline")
    p_run.add_argument(
        "--pipeline", required=True,
        choices=list(PIPELINE_REGISTRY),
        help="Pipeline to use: mapreduce | mongodb"
    )
    p_run.add_argument("--log", required=True, help="Path to NASA log file")
    p_run.add_argument("--batch-size", type=int, default=10000, metavar="N",
                       help="Number of records per batch (default: 10000)")
    p_run.add_argument("--quiet", action="store_true", help="Suppress per-batch output")

    # ── report ───────────────────────────────────────────────────────────
    p_rep = subparsers.add_parser("report", help="Display ETL results from DB")
    p_rep.add_argument("--run-id", type=int, metavar="ID",
                       help="Show report for a specific run ID")
    p_rep.add_argument("--all", action="store_true", help="List all runs")

    # ── demo ─────────────────────────────────────────────────────────────
    p_demo = subparsers.add_parser("demo", help="Generate sample data and run both pipelines")
    p_demo.add_argument("--records", type=int, default=50000,
                        help="Number of log records to generate (default: 50000)")
    p_demo.add_argument("--batch-size", type=int, default=10000, metavar="N",
                        help="Batch size (default: 10000)")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "report":
        cmd_report(args)
    elif args.command == "demo":
        cmd_demo(args)


if __name__ == "__main__":
    main()
