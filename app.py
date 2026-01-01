import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
import pandas as pd
import os


def create_spark():
    return SparkSession.builder.master("local[*]").appName("StreamlitPySparkDemo").getOrCreate()


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


def main():
    st.title("Streamlit + PySpark Demo")
    st.markdown("Demo processing of records where `processed` is 'y', null, or other values (e.g. 'p').")

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
    df = load_data(uploaded_file)

    st.subheader("Input sample")
    st.dataframe(df.head(10))

    spark = create_spark()
    sdf, counts = spark_process(spark, df)

    st.subheader("Processed / Unprocessed counts")
    st.table(counts)

    queries = run_template_queries(spark)
    st.subheader("Template query: processed counts")
    st.table(queries["processed_counts"])

    st.subheader("Template query: unprocessed sample (non-'y' or NULL)")
    st.table(queries["unprocessed_sample"])

    st.info("In this demo, records with `processed=='y'` are considered processed; all others are unprocessed.")


if __name__ == "__main__":
    main()
