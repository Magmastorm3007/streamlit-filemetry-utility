# Streamlit + PySpark Demo

Simple demo showing file processing with PySpark inside a Dockerized Streamlit app.

Quick start

1. Build and run with Docker Compose:

```bash
cd streamlit_pyspark_demo
docker-compose up --build
```

2. Open http://localhost:8501 to view the Streamlit UI.

What it does

- Loads `sample_data/sample.csv` (or upload your own CSV).
- Uses PySpark to count rows where `processed=='y'` (treated as processed) and others as unprocessed.
- Runs example/template SQL queries against a Spark temp view to showcase query outputs.

Git

Initialize a git repo in the folder and make the first commit:

```bash
cd streamlit_pyspark_demo
git init
git add .
git commit -m "Initial scaffold: Streamlit + PySpark demo"
```
