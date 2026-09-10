FROM apache/airflow:3.2.0-python3.10

ENV AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        unixodbc \
        unixodbc-dev \
        curl \
        ca-certificates \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# PySpark JDBC Drivers
# ============================================================

RUN mkdir -p /opt/airflow/spark-jars

COPY jars/*.jar /opt/airflow/spark-jars/

RUN chown -R airflow:root /opt/airflow/spark-jars

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    -r /requirements.txt
