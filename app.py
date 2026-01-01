import os
import time
import re
import sqlite3
import pandas as pd
import streamlit as st

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit


# =============================
# Spark helpers (internal only)
# =============================
@st.cache_resource
def create_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Filemetry")
        .getOrCreate()
    )


def prepare_spark_view(df: pd.DataFrame):
    spark = create_spark()

    sdf = spark.createDataFrame(df)
    if "processed" in sdf.columns:
        sdf = sdf.withColumn(
            "processed_flag",
            when(col("processed") == "y", lit("processed"))
            .otherwise(lit("unprocessed"))
        )

    sdf.createOrReplaceTempView("records")
    return spark


# =============================
# DB helpers
# =============================
def execute_sqlite_query(db_path, query):
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(query, conn)


def execute_sqlalchemy_query(conn_str, query):
    import sqlalchemy
    engine = sqlalchemy.create_engine(conn_str)
    try:
        return pd.read_sql_query(query, engine)
    finally:
        engine.dispose()


# =============================
# Utilities
# =============================
def load_data(uploaded_file):
    if uploaded_file is not None:
        return pd.read_csv(uploaded_file)

    sample_path = os.path.join(
        os.path.dirname(__file__), "sample_data", "sample.csv"
    )
    return pd.read_csv(sample_path)


def apply_date_filter(query, column, date_from, date_to):
    def _fmt(d):
        try:
            return d.isoformat()
        except Exception:
            return str(d)

    conditions = []

    # If both dates are provided and equal, use equality on the DATE cast
    if date_from and date_to and date_from == date_to:
        ds = _fmt(date_from)
        conditions.append(f"CAST({column} AS DATE) = DATE('{ds}')")
    else:
        if date_from:
            conditions.append(f"CAST({column} AS DATE) >= DATE('{_fmt(date_from)}')")
        if date_to:
            conditions.append(f"CAST({column} AS DATE) <= DATE('{_fmt(date_to)}')")

    if not conditions:
        return query

    clause = " AND ".join(conditions)

    # Remove trailing semicolon for manipulation, we'll re-add it later
    trailing_semicolon = query.rstrip().endswith(";")
    q_work = query.rstrip().rstrip(";")

    # Find clause keywords using regex word boundaries
    pattern = re.compile(r"\b(order by|group by|having|limit|offset)\b", flags=re.IGNORECASE)
    matches = list(pattern.finditer(q_work))

    # If query already has a WHERE, insert the new conditions into that WHERE
    where_match = re.search(r"\bwhere\b", q_work, flags=re.IGNORECASE)
    if where_match:
        where_start = where_match.start()
        # find next clause appearing after WHERE
        next_after_where = [m for m in matches if m.start() > where_start]
        if next_after_where:
            first_next = min(next_after_where, key=lambda m: m.start())
            pos = first_next.start()
            before = q_work[:pos]
            after = q_work[pos:]
            # Insert AND {clause} before the next clause
            new_q = f"{before} AND {clause} {after}"
        else:
            # No following clause, append at end
            new_q = f"{q_work} AND {clause}"

        if trailing_semicolon:
            new_q = new_q.rstrip() + ";"
        return new_q

    # No existing WHERE: insert WHERE before the earliest clause or append
    if matches:
        first = min(matches, key=lambda m: m.start())
        pos = first.start()
        before = q_work[:pos]
        after = q_work[pos:]
        new_q = f"{before} WHERE {clause} {after}"
    else:
        new_q = f"{q_work} WHERE {clause}"

    if trailing_semicolon:
        new_q = new_q.rstrip() + ";"

    return new_q


def detect_date_column(columns):
    for c in ["createdat", "created_at", "timestamp"]:
        if c in columns:
            return c
    return None


def strip_limit_offset(query: str) -> str:
    """Return query without LIMIT/OFFSET clauses and trailing semicolon."""
    q = query.rstrip().rstrip(";")
    # remove limit ... offset ... (case-insensitive)
    q = re.sub(r"(?i)\s+limit\s+\d+(\s+offset\s+\d+)?", "", q)
    q = re.sub(r"(?i)\s+offset\s+\d+", "", q)
    return q


def get_total_count_for_query(query: str, data_source, spark, pg_conn, custom_conn):
    q_no_limit = strip_limit_offset(query)
    count_q = f"SELECT COUNT(*) AS __total_count__ FROM ({q_no_limit}) AS _q"
    try:
        if data_source in ["File (CSV)", "SQLite (local)"]:
            # Spark path
            df_count = spark.sql(count_q).toPandas()
            return int(df_count['__total_count__'].iloc[0])
        else:
            # SQLAlchemy path (Postgres / custom)
            if data_source == "Postgres":
                df_count = execute_sqlalchemy_query(pg_conn, count_q)
            else:
                df_count = execute_sqlalchemy_query(custom_conn, count_q)
            return int(df_count['__total_count__'].iloc[0])
    except Exception:
        return None


# =============================
# Main App
# =============================
def main():
    st.set_page_config(page_title="Streamlit Filemetry", layout="wide")

    # ---------- Theme ----------
    st.markdown(
        """
        <style>
        .app-header {
            background-color:#0b6b3a;
            padding:12px;
            border-radius:6px;
        }
        .app-header h1 {
            color:white;
            margin:0;
        }
        .stButton>button {
            background-color:#0b6b3a;
            color:white;
            border-radius:6px;
        }
        /* Green loader */
        .green-loader { display:inline-block; width:20px; height:20px; border:3px solid #e6f0ea; border-top-color:#0b6b3a; border-radius:50%; animation:spin 1s linear infinite; margin-right:8px; }
        .green-loader-text { color:#0b6b3a; display:inline-block; vertical-align:middle; font-weight:600; }
        @keyframes spin { to { transform: rotate(360deg); } }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="app-header"><h1>Streamlit Filemetry</h1></div>', unsafe_allow_html=True)
    st.caption("File processing telemetry & query observability")

    # ---------- Sidebar ----------
    st.sidebar.header("Data Source")

    data_source = st.sidebar.selectbox(
        "Source",
        ["File (CSV)", "SQLite (local)", "Postgres", "Custom DB"]
    )

    csv_file = None
    sqlite_path = None
    pg_conn = None
    custom_conn = None

    if data_source == "File (CSV)":
        csv_file = st.sidebar.file_uploader("Upload CSV", type=["csv"])

    elif data_source == "SQLite (local)":
        sqlite_path = st.sidebar.text_input(
            "SQLite DB path",
            value=os.path.join(os.getcwd(), "demo_records.db")
        )

    elif data_source == "Postgres":
        pg_conn = st.sidebar.text_input(
            "Postgres SQLAlchemy URL",
            value="postgresql+psycopg2://user:pass@host:5432/db"
        )

    elif data_source == "Custom DB":
        custom_conn = st.sidebar.text_input(
            "SQLAlchemy URL",
            placeholder="dialect+driver://user:pass@host/db"
        )

    # ---------- Query Templates ----------
    st.sidebar.header("Query")

    TEMPLATES = {
        "Processed summary":
            "SELECT processed_flag, COUNT(*) AS cnt FROM records GROUP BY processed_flag",
        "Unprocessed sample":
            "SELECT * FROM records WHERE processed_flag = 'unprocessed' LIMIT 20",
        "Raw preview":
            "SELECT * FROM records LIMIT 50",
    }

    template = st.sidebar.selectbox("Template", list(TEMPLATES.keys()))
    query = st.sidebar.text_area("SQL", TEMPLATES[template], height=160)

    # ---------- Date Filters ----------
    st.sidebar.header("Date Filter")
    use_date = st.sidebar.checkbox("Apply date filter")

    date_from = st.sidebar.date_input("From") if use_date else None
    date_to = st.sidebar.date_input("To") if use_date else None

    run_query = st.sidebar.button("Run query")

    # ---------- Load Data ----------
    df = pd.DataFrame()
    spark = None
    date_column = None

    if data_source == "File (CSV)":
        df = load_data(csv_file)
        spark = prepare_spark_view(df)
        date_column = detect_date_column(df.columns)

    elif data_source == "SQLite (local)":
        if sqlite_path and os.path.exists(sqlite_path):
            df = execute_sqlite_query(sqlite_path, "SELECT * FROM records")
        else:
            df = load_data(None)
        spark = prepare_spark_view(df)
        date_column = detect_date_column(df.columns)

    # ---------- Layout ----------
    left, right = st.columns([2, 1])

    with left:
        st.subheader("Input sample")
        st.dataframe(df.head(10))

    with right:
        st.subheader("Query result")

        if run_query:
            start = time.perf_counter()
            final_query = query
            if use_date and date_column:
                final_query = apply_date_filter(
                    query, date_column, date_from, date_to
                )

            try:
                # Show custom green loader while executing query and counting
                spinner_ph = st.empty()
                spinner_ph.markdown(
                    "<div><span class=\"green-loader\"></span><span class=\"green-loader-text\">Running query...</span></div>",
                    unsafe_allow_html=True,
                )

                if data_source in ["File (CSV)", "SQLite (local)"]:
                    result = spark.sql(final_query).toPandas()
                elif data_source == "Postgres":
                    result = execute_sqlalchemy_query(pg_conn, final_query)
                else:
                    result = execute_sqlalchemy_query(custom_conn, final_query)

                # Try to compute total matching rows (strip LIMIT/OFFSET)
                total_count = get_total_count_for_query(final_query, data_source, spark, pg_conn, custom_conn)

                # Compute elapsed time
                elapsed = (time.perf_counter() - start) * 1000

            except Exception as e:
                # ensure spinner is removed on error
                try:
                    spinner_ph.empty()
                except Exception:
                    pass
                st.error(f"Query failed: {e}")
            else:
                try:
                    spinner_ph.empty()
                except Exception:
                    pass

                st.success(f"Query executed in {elapsed:.2f} ms")
                if total_count is not None:
                    st.caption(f"Total matching rows: {total_count}")

                # Show a preview (max 100 rows) for performance in the frontend
                try:
                    st.dataframe(result.head(100))
                except Exception:
                    st.dataframe(result)
        else:
            st.info("Click **Run query** to execute")

    st.info(
        "Date filters are applied automatically if a supported timestamp column exists "
        "(createdat, created_at, timestamp). Spark is internal only."
    )


if __name__ == "__main__":
    main()
