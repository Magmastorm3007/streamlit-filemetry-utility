FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install Java (OpenJDK 11) required by PySpark and basic tools
RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-21-jre ca-certificates curl \
	&& rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements.txt ./
RUN pip install  -r requirements.txt

COPY . .

EXPOSE 8501 4040

CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
