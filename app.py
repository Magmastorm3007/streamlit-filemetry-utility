import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
import pandas as pd
import os
import sqlite3


def create_spark():
    return SparkSession.builder.master("local[*]").appName("StreamlitFilemetry").getOrCreate()


def load_data(uploaded_file):
    if uploaded_file is not None:
        return pd.read_csv(uploaded_file)
    sample_path = os.path.join(os.path.dirname(__file__), "sample_data", "sample.csv")
    return pd.read_csv(sample_path)


def spark_process(spark, pdf):
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn(
        "processed_flag", when(col("processed") == "y", lit("processed")).otherwise(lit("unprocessed"))
    )
    counts = sdf.groupBy("processed_flag").count().toPandas()
    sdf.createOrReplaceTempView("records")
    return sdf, counts


def run_template_queries(spark):
    # Template SQL queries against the Spark temp view `records`
    q1 = "SELECT processed, COUNT(*) as cnt FROM records GROUP BY processed"
    q2 = "SELECT * FROM records WHERE processed IS NULL OR processed != 'y' LIMIT 20"
    r1 = spark.sql(q1).toPandas()
    r2 = spark.sql(q2).toPandas()
    return {"processed_counts": r1, "unprocessed_sample": r2}


# Backend helpers
def init_sqlite_db(db_path, df, table_name="records"):
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()


def execute_sqlite_query(db_path, query):
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def execute_sqlalchemy_query(conn_str, query):
    try:
        import sqlalchemy
    except Exception as e:
        raise RuntimeError("SQLAlchemy is required for this backend (install 'sqlalchemy' and DB driver).") from e
    engine = sqlalchemy.create_engine(conn_str)
    try:
        return pd.read_sql_query(query, engine)
    finally:
        engine.dispose()


def execute_postgres_query(conn_str, query):
    return execute_sqlalchemy_query(conn_str, query)


def execute_oracle_query(conn_str, query):
    return execute_sqlalchemy_query(conn_str, query)


def main():
    st.title("Streamlit Filemetry")
    st.markdown("Demo processing of records where `processed` is 'y', null, or other values (e.g. 'p').")

    # Sidebar: backend selection / config and template-query editor
        st.set_page_config(page_title="Streamlit Filemetry", layout="wide")

        # Green theme header styles (inline CSS)
        st.markdown(
            """
            <style>
            .app-header {background-color:#0b6b3a;padding:10px;border-radius:6px}
            .app-header h1{color:#fff;margin:0;padding:0}
            .stButton>button{background-color:#0b6b3a;color:white}
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="app-header"><h1>Streamlit Filemetry</h1></div>', unsafe_allow_html=True)
        st.markdown("Demo processing of records where `processed` is 'y', null, or other values (e.g. 'p').")

        # Sidebar: choose data source type (file or DB)
        st.sidebar.header("Data Source & Backend")
        data_source = st.sidebar.selectbox("Data Source Type", ["File (CSV)", "SQLite (local)", "Postgres", "Oracle", "Spark TempView"])

        # File options
        csv_option = None
        if data_source == "File (CSV)":
            csv_mode = st.sidebar.radio("CSV source", ["Use sample file", "Upload CSV"])
            if csv_mode == "Use sample file":
                csv_option = os.path.join(os.path.dirname(__file__), "sample_data", "sample.csv")
            else:
                uploaded_file = st.sidebar.file_uploader("Upload CSV file", type=["csv"])  
                csv_option = uploaded_file

        # SQLite options
        sqlite_path = None
        if data_source == "SQLite (local)":
            sqlite_path = st.sidebar.text_input("SQLite DB path", value=os.path.join(os.getcwd(), "demo_records.db"))
            if st.sidebar.button("Initialize SQLite DB from sample CSV"):
                try:
                    df_init = pd.read_csv(os.path.join(os.path.dirname(__file__), "sample_data", "sample.csv"))
                    init_sqlite_db(sqlite_path, df_init)
                    st.sidebar.success(f"Initialized {sqlite_path}")
                except Exception as e:
                    st.sidebar.error(f"Failed to init SQLite DB: {e}")

        # Postgres / Oracle options
        pg_conn = None
        oracle_conn = None
        if data_source == "Postgres":
            pg_conn = st.sidebar.text_input("Postgres SQLAlchemy URL", value="postgresql+psycopg2://user:pass@host:5432/dbname")
            st.sidebar.caption("Requires SQLAlchemy + psycopg2-binary installed in the runtime")
        if data_source == "Oracle":
            oracle_conn = st.sidebar.text_input("Oracle SQLAlchemy URL", value="oracle+oracledb://user:pass@host:1521/?service_name=ORCLPDB1")
            st.sidebar.caption("Requires SQLAlchemy + oracledb (or cx_Oracle) installed")

        # Template query editor
        st.sidebar.header("Template Query")
        default_query = "SELECT processed, COUNT(*) as cnt FROM records GROUP BY processed"
        query = st.sidebar.text_area("SQL template", value=default_query, height=160)

        # Date filter (optional)
        st.sidebar.header("Date Filter")
        apply_date = st.sidebar.checkbox("Apply date filter on `timestamp` column", value=False)
        date_from = None
        date_to = None
        if apply_date:
            date_from = st.sidebar.date_input("From", value=None)
            date_to = st.sidebar.date_input("To", value=None)

        # Load dataframe based on data source
        if data_source == "File (CSV)":
            df = load_data(csv_option if not hasattr(csv_option, 'read') else csv_option)
        elif data_source == "SQLite (local)":
            # try to read table `records` if exists
            try:
                if os.path.exists(sqlite_path):
                    df = execute_sqlite_query(sqlite_path, "SELECT * FROM records LIMIT 100000")
                else:
                    st.warning("SQLite DB not found; falling back to sample CSV")
                    df = load_data(None)
            except Exception as e:
                st.error(f"SQLite read failed: {e}")
                df = load_data(None)
        elif data_source == "Postgres":
            df = pd.DataFrame()
            st.info("Postgres selected — run template queries via the editor once connected.")
        elif data_source == "Oracle":
            df = pd.DataFrame()
            st.info("Oracle selected — run template queries via the editor once connected.")
        else:
            # Spark TempView or fallback
            df = load_data(None)

        # Apply date filter if requested
        if not df.empty and apply_date and "timestamp" in df.columns:
            try:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                if date_from:
                    df = df[df["timestamp"].dt.date >= date_from]
                if date_to:
                    df = df[df["timestamp"].dt.date <= date_to]
            except Exception:
                st.warning("Could not parse `timestamp` column for date filtering.")

        # Main layout: metrics and charts
        left, right = st.columns([2, 1])

        with left:
            st.subheader("Input sample")
            st.dataframe(df.head(10))

            # Spark processing always available for analytics and temp view
            spark = create_spark()
            sdf, counts = spark_process(spark, df)

            st.subheader("Processed / Unprocessed counts (Spark)")
            st.table(counts)

            # Additional analytics: amounts distribution and transaction_type counts
            if "amount" in df.columns:
                st.subheader("Amount distribution")
                try:
                    amt_series = pd.to_numeric(df["amount"], errors="coerce").dropna()
                    st.bar_chart(amt_series.value_counts(bins=20).sort_index())
                except Exception:
                    st.write("Unable to render amount distribution")

            if "transaction_type" in df.columns:
                st.subheader("Transaction Type Counts")
                tt = df["transaction_type"].fillna("(null)").value_counts()
                st.bar_chart(tt)

        with right:
            st.subheader("Template Query Runner")
            st.write("Selected backend: ", data_source)

            if st.button("Run template query on selected backend"):
                try:
                    if data_source == "Spark TempView":
                        res = spark.sql(query).toPandas()
                    elif data_source == "File (CSV)":
                        # run query on Spark temp view
                        res = spark.sql(query).toPandas()
                    elif data_source == "SQLite (local)":
                        res = execute_sqlite_query(sqlite_path, query)
                    elif data_source == "Postgres":
                        res = execute_postgres_query(pg_conn, query)
                    elif data_source == "Oracle":
                        res = execute_oracle_query(oracle_conn, query)
                    else:
                        res = pd.DataFrame()
                    st.write(res)
                except Exception as e:
                    st.error(f"Query failed: {e}")

            st.markdown("---")
            st.subheader("Quick actions")
            if st.button("Download sample CSV"):
                st.download_button("Download", data=open(os.path.join(os.path.dirname(__file__), "sample_data", "sample.csv"), "rb"), file_name="sample.csv")

        # Keep Spark template outputs for convenience
        queries = run_template_queries(spark)
        st.subheader("Template query: processed counts (Spark)")
        st.table(queries["processed_counts"])

        st.subheader("Template query: unprocessed sample (Spark)")
        st.table(queries["unprocessed_sample"])

        st.info("In this demo, records with `processed=='y'` are considered processed; all others are unprocessed.")
