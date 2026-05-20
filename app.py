import streamlit as st
import pandas as pd
import psycopg2

from pipelines.mongodb_pipeline import run as run_mongo
from pipelines.mapreduce_pipeline import run as run_mr
from pipelines.hive_pipeline import run as run_hive
from pipelines.pig_pipeline import run as run_pig


# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="NASA Log Analytics",
    layout="wide"
)

st.title("NASA Log Analytics Tool")

st.markdown("""
Compare ETL and analytics execution using:
- MongoDB
- MapReduce
- Hive
- Pig
""")


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

pipeline = st.sidebar.selectbox(
    "Select Pipeline",
    [
        "MongoDB",
        "MapReduce",
        "Hive",
        "Pig"
    ]
)

query_choice = st.sidebar.selectbox(
    "Select Query",
    [
        "Query 1 - Daily Traffic Summary",
        "Query 2 - Top Requested Resources",
        "Query 3 - Hourly Error Analysis"
    ]
)

batch_size = st.sidebar.number_input(
    "Batch Size",
    min_value=1000,
    max_value=50000,
    value=10000,
    step=1000
)

run_button = st.sidebar.button("Run Pipeline")


# ─────────────────────────────────────────────
# RUN PIPELINE
# ─────────────────────────────────────────────

LOG_FILE = "NASA_access_log_Jul95"

if run_button:

    st.info(f"Running {pipeline} pipeline...")

    if pipeline == "MongoDB":
        run_id = run_mongo(LOG_FILE, batch_size)

    elif pipeline == "MapReduce":
        run_id = run_mr(LOG_FILE, batch_size)

    elif pipeline == "Hive":
        run_id = run_hive(LOG_FILE, batch_size)

    elif pipeline == "Pig":
        run_id = run_pig(LOG_FILE, batch_size)

    st.success(f"Pipeline completed! Run ID: {run_id}")


# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="nosql_project",
        user="postgres",
        password="postgre"
    )


# ─────────────────────────────────────────────
# SHOW RUN METADATA
# ─────────────────────────────────────────────

st.header("📊 Recent Pipeline Runs")

try:
    conn = get_connection()

    query = """
    SELECT *
    FROM runs
    ORDER BY run_id DESC
    LIMIT 10;
    """

    df_runs = pd.read_sql(query, conn)

    st.dataframe(df_runs, use_container_width=True)

    conn.close()

except Exception as e:
    st.error(f"Database error: {e}")


# ─────────────────────────────────────────────
# QUERY 1
# ─────────────────────────────────────────────

if query_choice == "Query 1 - Daily Traffic Summary":

    st.header("Query 1 Results")

    st.markdown("""
    **Daily Traffic Summary**

    For each date and status code:
    - request count
    - total bytes transferred
    """)

    try:
        conn = get_connection()

        query = """
        SELECT
            pipeline,
            log_date,
            status_code,
            request_count,
            total_bytes
        FROM query1_results
        ORDER BY id DESC
        LIMIT 100;
        """

        df_q1 = pd.read_sql(query, conn)

        st.dataframe(df_q1, use_container_width=True)

        # Optional chart
        st.subheader("Request Count by Status Code")

        chart_df = (
            df_q1.groupby("status_code")["request_count"]
            .sum()
            .reset_index()
        )

        st.bar_chart(chart_df.set_index("status_code"))

        conn.close()

    except Exception as e:
        st.error(f"Database error: {e}")

# ─────────────────────────────────────────────
# QUERY 2
# ─────────────────────────────────────────────

elif query_choice == "Query 2 - Top Requested Resources":

    st.header("Query 2 Results")

    st.markdown("""
    **Top Requested Resources**

    Top 20 requested resource paths with:
    - request count
    - total bytes
    - distinct host count
    """)

    try:
        conn = get_connection()

        query = """
        SELECT
            pipeline,
            resource_path,
            request_count,
            total_bytes,
            distinct_host_count
        FROM query2_results
        ORDER BY request_count DESC
        LIMIT 20;
        """

        df_q2 = pd.read_sql(query, conn)

        st.dataframe(df_q2, use_container_width=True)

        st.subheader("Top Requested Resources")

        chart_df = df_q2[["resource_path", "request_count"]]
        chart_df = chart_df.set_index("resource_path")

        st.bar_chart(chart_df)

        conn.close()

    except Exception as e:
        st.error(f"Database error: {e}")

# ─────────────────────────────────────────────
# QUERY 3
# ─────────────────────────────────────────────

elif query_choice == "Query 3 - Hourly Error Analysis":

    st.header("Query 3 Results")

    st.markdown("""
    **Hourly Error Analysis**

    For each date and hour:
    - error request count
    - total request count
    - error rate
    - distinct error hosts
    """)

    try:
        conn = get_connection()

        query = """
        SELECT
            pipeline,
            log_date,
            log_hour,
            error_request_count,
            total_request_count,
            ROUND(error_rate::numeric, 4) AS error_rate,
            distinct_error_hosts
        FROM query3_results
        ORDER BY id DESC
        LIMIT 100;
        """

        df_q3 = pd.read_sql(query, conn)

        st.dataframe(df_q3, use_container_width=True)

        st.subheader("Error Rate by Hour")

        chart_df = (
            df_q3.groupby("log_hour")["error_rate"]
            .mean()
            .reset_index()
        )

        chart_df = chart_df.set_index("log_hour")

        st.line_chart(chart_df)

        conn.close()

    except Exception as e:
        st.error(f"Database error: {e}")


# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────

st.markdown("---")
st.caption("NoSQL Systems End Semester Project – Multi-Pipeline ETL and Reporting Framework")