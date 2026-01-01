# streamlit_filemetry

Streamlit Filemetry — demo app for file processing and simple analytics using PySpark.

Quick start

1. Build and run with Docker Compose:

```bash
cd streamlit_pyspark_demo
docker compose up --build
```

2. Open http://localhost:8501 to view the Streamlit UI.

Backend support

- Spark TempView (in-memory)
- SQLite (local file) — initialize from the UI
- Postgres (via SQLAlchemy) — provide a SQLAlchemy URL (requires `psycopg2-binary`)
- Oracle (via SQLAlchemy) — provide a SQLAlchemy URL (requires `oracledb` and optionally Oracle client for thick mode)

What it does

- Loads `sample_data/sample.csv` (or upload your own CSV).
- Uses PySpark to create a temp view `records` and run template queries.
- Lets users configure a backend, edit SQL template queries, initialize a local SQLite DB, and run queries against SQLite/Postgres/Oracle.

Dependencies

Install runtime dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Notes

- For Postgres use a SQLAlchemy URL like `postgresql+psycopg2://user:pass@host:5432/dbname` and ensure `psycopg2-binary` is installed.
- For Oracle, prefer `oracledb` (pure Python) or `cx_Oracle` with proper Oracle client libraries. Example SQLAlchemy URL:
	`oracle+oracledb://user:pass@host:1521/?service_name=ORCLPDB1`.
- The Docker image installs Java for PySpark; if you add DB client libs that require system packages, update the `Dockerfile` accordingly.

Git

Initialize a git repo in the folder and make the first commit (already done in this workspace):

```bash
cd streamlit_pyspark_demo
git status
```
