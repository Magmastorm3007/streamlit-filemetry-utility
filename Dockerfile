FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install Java (OpenJDK) required by PySpark and basic tools.
# Using OpenJDK 21 on recent Debian-based images; adjust package name per distro if needed.
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	   openjdk-21-jre \
	   ca-certificates \
	   curl \
	   gnupg \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# If you need to build binary Python packages (psycopg2, oracledb thick mode, etc.)
# uncomment and adjust the following to install build deps for Debian/Ubuntu:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential libpq-dev libffi-dev default-libmysqlclient-dev \
#     && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client cannot be redistributed. To enable Oracle thick client support,
# download the RPMs/ZIPs from Oracle and copy them into the image here and install.
# Example (pseudo):
# COPY oracle-instantclient-basic.rpm /tmp/
# RUN apt-get update && apt-get install -y alien && alien -i /tmp/oracle-instantclient-basic.rpm
# ENV LD_LIBRARY_PATH=/usr/lib/oracle/<version>/client64/lib:$LD_LIBRARY_PATH

# Copy requirements and install Python dependencies.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Expose Streamlit and Spark UI ports
EXPOSE 8501 4040

# Start Streamlit
CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
