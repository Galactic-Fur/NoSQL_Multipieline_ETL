# """
# db.py – Relational database layer.

# Uses SQLite for portability (swap to MySQL/PostgreSQL by changing the
# connect() call and adapting the DDL slightly).

# Schema:
#   etl_runs         – one row per pipeline run
#   q1_daily_traffic – Query 1 results
#   q2_top_resources – Query 2 results
#   q3_hourly_errors – Query 3 results
# """
# import sqlite3
# import os

# DB_PATH = os.path.join(os.path.dirname(__file__), "..", "etl_results.db")


# def get_connection():
#     conn = sqlite3.connect(DB_PATH)
#     conn.row_factory = sqlite3.Row
#     return conn


# def init_schema():
#     conn = get_connection()
#     c = conn.cursor()

#     c.executescript("""
#     CREATE TABLE IF NOT EXISTS etl_runs (
#         run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
#         pipeline        TEXT    NOT NULL,
#         run_timestamp   TEXT    NOT NULL,
#         batch_size      INTEGER NOT NULL,
#         total_batches   INTEGER,
#         total_records   INTEGER,
#         malformed_count INTEGER,
#         avg_batch_size  REAL,
#         runtime_seconds REAL
#     );

#     CREATE TABLE IF NOT EXISTS q1_daily_traffic (
#         id              INTEGER PRIMARY KEY AUTOINCREMENT,
#         run_id          INTEGER REFERENCES etl_runs(run_id),
#         pipeline        TEXT,
#         log_date        TEXT,
#         status_code     INTEGER,
#         request_count   INTEGER,
#         total_bytes     INTEGER
#     );

#     CREATE TABLE IF NOT EXISTS q2_top_resources (
#         id                  INTEGER PRIMARY KEY AUTOINCREMENT,
#         run_id              INTEGER REFERENCES etl_runs(run_id),
#         pipeline            TEXT,
#         resource_path       TEXT,
#         request_count       INTEGER,
#         total_bytes         INTEGER,
#         distinct_host_count INTEGER
#     );

#     CREATE TABLE IF NOT EXISTS q3_hourly_errors (
#         id                  INTEGER PRIMARY KEY AUTOINCREMENT,
#         run_id              INTEGER REFERENCES etl_runs(run_id),
#         pipeline            TEXT,
#         log_date            TEXT,
#         log_hour            INTEGER,
#         error_request_count INTEGER,
#         total_request_count INTEGER,
#         error_rate          REAL,
#         distinct_error_hosts INTEGER
#     );
#     """)

#     conn.commit()
#     conn.close()


# def insert_run(pipeline, run_timestamp, batch_size, total_batches,
#                total_records, malformed_count, avg_batch_size, runtime_seconds):
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("""
#         INSERT INTO etl_runs
#             (pipeline, run_timestamp, batch_size, total_batches,
#              total_records, malformed_count, avg_batch_size, runtime_seconds)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#     """, (pipeline, run_timestamp, batch_size, total_batches,
#           total_records, malformed_count, avg_batch_size, runtime_seconds))
#     run_id = c.lastrowid
#     conn.commit()
#     conn.close()
#     return run_id


# def insert_q1(run_id, pipeline, rows):
#     conn = get_connection()
#     c = conn.cursor()
#     c.executemany("""
#         INSERT INTO q1_daily_traffic
#             (run_id, pipeline, log_date, status_code, request_count, total_bytes)
#         VALUES (?, ?, ?, ?, ?, ?)
#     """, [(run_id, pipeline, r["log_date"], r["status_code"],
#            r["request_count"], r["total_bytes"]) for r in rows])
#     conn.commit()
#     conn.close()


# def insert_q2(run_id, pipeline, rows):
#     conn = get_connection()
#     c = conn.cursor()
#     c.executemany("""
#         INSERT INTO q2_top_resources
#             (run_id, pipeline, resource_path, request_count, total_bytes, distinct_host_count)
#         VALUES (?, ?, ?, ?, ?, ?)
#     """, [(run_id, pipeline, r["resource_path"], r["request_count"],
#            r["total_bytes"], r["distinct_host_count"]) for r in rows])
#     conn.commit()
#     conn.close()


# def insert_q3(run_id, pipeline, rows):
#     conn = get_connection()
#     c = conn.cursor()
#     c.executemany("""
#         INSERT INTO q3_hourly_errors
#             (run_id, pipeline, log_date, log_hour, error_request_count,
#              total_request_count, error_rate, distinct_error_hosts)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#     """, [(run_id, pipeline, r["log_date"], r["log_hour"],
#            r["error_request_count"], r["total_request_count"],
#            r["error_rate"], r["distinct_error_hosts"]) for r in rows])
#     conn.commit()
#     conn.close()


# def fetch_run(run_id):
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("SELECT * FROM etl_runs WHERE run_id = ?", (run_id,))
#     row = dict(c.fetchone())
#     conn.close()
#     return row


# def fetch_q1(run_id):
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("""SELECT log_date, status_code, request_count, total_bytes
#                  FROM q1_daily_traffic WHERE run_id=?
#                  ORDER BY log_date, status_code""", (run_id,))
#     rows = [dict(r) for r in c.fetchall()]
#     conn.close()
#     return rows


# def fetch_q2(run_id):
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("""SELECT resource_path, request_count, total_bytes, distinct_host_count
#                  FROM q2_top_resources WHERE run_id=?
#                  ORDER BY request_count DESC""", (run_id,))
#     rows = [dict(r) for r in c.fetchall()]
#     conn.close()
#     return rows


# def fetch_q3(run_id):
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("""SELECT log_date, log_hour, error_request_count, total_request_count,
#                         error_rate, distinct_error_hosts
#                  FROM q3_hourly_errors WHERE run_id=?
#                  ORDER BY log_date, log_hour""", (run_id,))
#     rows = [dict(r) for r in c.fetchall()]
#     conn.close()
#     return rows


# def list_runs():
#     conn = get_connection()
#     c = conn.cursor()
#     c.execute("SELECT * FROM etl_runs ORDER BY run_id DESC")
#     rows = [dict(r) for r in c.fetchall()]
#     conn.close()
#     return rows


import psycopg2
from psycopg2.extras import execute_batch

DB_CONFIG = {
    "host": "localhost",
    "database": "nosql_project",
    "user": "postgres",
    "password": "postgre"
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────

def init_schema():
    conn = get_connection()
    cur = conn.cursor()

    # Runs table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id SERIAL PRIMARY KEY,
        pipeline VARCHAR(50),
        run_timestamp TIMESTAMP,
        batch_size INT,
        total_batches INT,
        total_records INT,
        malformed_count INT,
        avg_batch_size FLOAT,
        runtime_seconds FLOAT
    );
    """)

    # Query 1 results
    cur.execute("""
    CREATE TABLE IF NOT EXISTS query1_results (
        id SERIAL PRIMARY KEY,
        run_id INT,
        pipeline VARCHAR(50),
        log_date VARCHAR(20),
        status_code INT,
        request_count INT,
        total_bytes BIGINT
    );
    """)

    # Query 2 results
    cur.execute("""
    CREATE TABLE IF NOT EXISTS query2_results (
        id SERIAL PRIMARY KEY,
        run_id INT,
        pipeline VARCHAR(50),
        resource_path TEXT,
        request_count INT,
        total_bytes BIGINT,
        distinct_host_count INT
    );
    """)

    # Query 3 results
    cur.execute("""
    CREATE TABLE IF NOT EXISTS query3_results (
        id SERIAL PRIMARY KEY,
        run_id INT,
        pipeline VARCHAR(50),
        log_date VARCHAR(20),
        log_hour INT,
        error_request_count INT,
        total_request_count INT,
        error_rate FLOAT,
        distinct_error_hosts INT
    );
    """)

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# RUN METADATA
# ─────────────────────────────────────────────

def insert_run(
    pipeline,
    run_timestamp,
    batch_size,
    total_batches,
    total_records,
    malformed_count,
    avg_batch_size,
    runtime_seconds
):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO runs (
        pipeline,
        run_timestamp,
        batch_size,
        total_batches,
        total_records,
        malformed_count,
        avg_batch_size,
        runtime_seconds
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING run_id;
    """, (
        pipeline,
        run_timestamp,
        batch_size,
        total_batches,
        total_records,
        malformed_count,
        avg_batch_size,
        runtime_seconds
    ))

    run_id = cur.fetchone()[0]

    conn.commit()
    conn.close()

    return run_id


# ─────────────────────────────────────────────
# QUERY 1
# ─────────────────────────────────────────────

def insert_q1(run_id, pipeline, results):
    conn = get_connection()
    cur = conn.cursor()

    rows = [
        (
            run_id,
            pipeline,
            r["log_date"],
            r["status_code"],
            r["request_count"],
            r["total_bytes"]
        )
        for r in results
    ]

    execute_batch(cur, """
    INSERT INTO query1_results (
        run_id,
        pipeline,
        log_date,
        status_code,
        request_count,
        total_bytes
    )
    VALUES (%s,%s,%s,%s,%s,%s);
    """, rows)

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# QUERY 2
# ─────────────────────────────────────────────

def insert_q2(run_id, pipeline, results):
    conn = get_connection()
    cur = conn.cursor()

    rows = [
        (
            run_id,
            pipeline,
            r["resource_path"],
            r["request_count"],
            r["total_bytes"],
            r["distinct_host_count"]
        )
        for r in results
    ]

    execute_batch(cur, """
    INSERT INTO query2_results (
        run_id,
        pipeline,
        resource_path,
        request_count,
        total_bytes,
        distinct_host_count
    )
    VALUES (%s,%s,%s,%s,%s,%s);
    """, rows)

    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# QUERY 3
# ─────────────────────────────────────────────

def insert_q3(run_id, pipeline, results):
    conn = get_connection()
    cur = conn.cursor()

    rows = [
        (
            run_id,
            pipeline,
            r["log_date"],
            r["log_hour"],
            r["error_request_count"],
            r["total_request_count"],
            r["error_rate"],
            r["distinct_error_hosts"]
        )
        for r in results
    ]

    execute_batch(cur, """
    INSERT INTO query3_results (
        run_id,
        pipeline,
        log_date,
        log_hour,
        error_request_count,
        total_request_count,
        error_rate,
        distinct_error_hosts
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
    """, rows)

    conn.commit()
    conn.close()