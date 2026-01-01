# streamlit_filemetry

![Streamlit Filemetry](./images/Streamlit_filemetry.png)

Streamlit Filemetry — demo app for file processing and simple analytics using PySpark. It can be used as a starter template for file processing analytics in ETL systems.

Project path (example): `~/Projects/Python/streamlit_filemetry`

Overview

- Loads `sample_data/sample.csv` (or upload your own CSV).
- Uses PySpark to create an in-memory temp view `records` and run template SQL queries.
- Provides backends: File (in-memory), SQLite (local file), Postgres and Oracle via SQLAlchemy.

This README documents full Docker build/run steps and local setup for Ubuntu-like systems.

Prerequisites

1. Docker Engine installed and running. See: https://docs.docker.com/engine/install/ubuntu/
2. (Optional) Docker Compose plugin (v2) or legacy `docker-compose` if you prefer.
3. If developing locally: Python 3.8+ and `venv`.

Quick start — Docker Compose

From the project root (`~/Projects/Python/streamlit_filemetry`):

```bash
cd ~/Projects/Python/streamlit_filemetry

# Build and run (foreground)

# Attach user group permissions like u1, can use sudo
docker compose up --build

# Or detached
docker compose up --build -d
docker compose logs -f

# Stop
docker compose down
```

Force full rebuild (no Docker layer cache):

```bash
docker compose build --no-cache
# or
docker build --no-cache -t streamlit_filemetry -f Dockerfile .
```

If your user cannot access the Docker socket, prefix commands with `sudo` or add your user to the `docker` group:

```bash
sudo usermod -aG docker $USER
newgrp docker
```

Local (non-Docker) development

```bash
cd ~/Projects/Python/streamlit_filemetry
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Run Streamlit locally
streamlit run app.py
```

Notes for pip installs:

- To avoid pip cache files: `pip install --no-cache-dir -r requirements.txt`
- To force reinstall: `pip install --force-reinstall -r requirements.txt`

Java / PySpark notes

- PySpark requires a Java runtime. The Docker image installs OpenJDK for you.
- Locally, install a JRE (example):

```bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless
```

Set `JAVA_HOME` if needed:

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

Configuration and environment variables

- You can pass DB connection strings via environment variables or the UI.
- Example SQLAlchemy URLs:
	- Postgres: `postgresql+psycopg2://user:pass@host:5432/dbname`
	- Oracle: `oracle+oracledb://user:pass@host:1521/?service_name=ORCLPDB1`

If you want a quick demo Postgres alongside the app, add a `docker-compose.override.yml` or extend `docker-compose.yml` with a Postgres service and set the `POSTGRES_URL` env var.

Example `.env` for Compose (optional):

```
# .env
POSTGRES_URL=postgresql+psycopg2://postgres:password@postgres:5432/demo_db
```

Supported backends & requirements

- Spark TempView (in-memory): requires PySpark (Java runtime in container or local host).
- SQLite: builtin; the UI can initialize a local SQLite DB from the sample CSV.
- Postgres: requires `sqlalchemy` + `psycopg2-binary`.
- Oracle: requires `sqlalchemy` + `oracledb` (or `cx_Oracle` plus client libs).

If installing DB client system libs (e.g., Oracle Instant Client) inside the container, update the `Dockerfile` and rebuild with `--no-cache`.

Troubleshooting

- Blank Streamlit page: check container logs:

```bash
docker compose logs -f
# or
docker logs <container_id>
```

- PySpark Java errors: ensure Java is available in the runtime (see Java notes above).
- Docker permission errors: use `sudo` or fix `docker` group membership.
- Want to force package reinstall inside image: add pip `--no-cache-dir` or `--force-reinstall` to your Dockerfile `pip install` line and rebuild with `--no-cache`.

Git and development flow

```bash
cd ~/Projects/Python/streamlit_filemetry
git status
git checkout development
git add .
git commit -m "your message"
```

Optional: create a remote GitHub repo and push (if you choose):

```bash
gh repo create streamlit_filemetry --private --source=. --remote=origin
git push -u origin main
git push -u origin development
```

Useful commands

```bash
# Check Streamlit is listening
ss -ltnp | grep 8501

# Quick HTTP health check
curl -I http://localhost:8501

# Rebuild and restart
docker compose down
docker compose build --no-cache
docker compose up -d
docker compose logs -f
```

If you'd like, I can also:
- Add a sample `docker-compose.yml` snippet that includes a Postgres service for local demos.
- Add a `.env.example` and a small script to initialize the SQLite DB from `sample_data/sample.csv`.

Enjoy — let me know if you want the Compose file extended with a demo Postgres or Oracle setup.

Linux distro notes

The commands above assume a Debian/Ubuntu-like system. Docker and Java installation steps vary across Linux distributions — below are quick notes for other common distros.

- Fedora / RHEL (dnf):
```bash
# Fedora / RHEL
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io
sudo systemctl enable --now docker
# Compose plugin availability varies by distro
sudo dnf install -y docker-compose-plugin || true
```

- CentOS / Alma / Rocky (yum/dnf):
```bash
sudo dnf install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io
sudo systemctl enable --now docker
```

- openSUSE (zypper):
```bash
sudo zypper install -y docker
sudo systemctl enable --now docker
```

- Arch Linux (pacman):
```bash
sudo pacman -Syu docker
sudo systemctl enable --now docker
```

Notes and caveats

- Java package names differ: `openjdk-11-jre-headless`, `openjdk-17-jre`, or `openjdk-21-jre` depending on distro and release. If `apt`/`dnf` can’t find a specific package, check your distro’s package list or install a JDK via SDKMAN, AdoptOpenJDK, or OpenJDK binaries.
- Docker Compose packaging differs: prefer the `docker compose` (v2) plugin if available; otherwise install the legacy `docker-compose` binary.
- If Docker requires `sudo`, either run with `sudo` or add your user to the `docker` group and re-login:
```bash
sudo usermod -aG docker $USER
newgrp docker
```
- For DB clients (Postgres, Oracle) you may need distro-specific dev packages (e.g., `libpq-dev`, `oracle-instantclient-basic`); update the `Dockerfile` and rebuild when adding those.

If you want, I can patch the `Dockerfile` to include commented examples for installing Postgres client headers, Oracle Instant Client, and multiple OpenJDK options — shall I proceed with those Dockerfile comments and optional installs?*** End Patch

