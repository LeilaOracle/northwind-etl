from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

import psycopg2
import clickhouse_connect


def load_dimdate_to_clickhouse():

    pg_conn = Connection.get(conn_id="northwind_postgres")

    postgres_conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port or 5432,
        database=pg_conn.schema or "northwind",
        user=pg_conn.login,
        password=pg_conn.password,
    )

    pg_cursor = postgres_conn.cursor()


    pg_cursor.execute("""
        SELECT
            "DateKey",
            "FullDate",
            "Day",
            "DayName",
            "DayOfWeek",
            "Month",
            "MonthName",
            "Quarter",
            "Year",
            "IsWeekend"
        FROM "DimDate";
    """)

    rows = pg_cursor.fetchall()

    print(f"Extracted {len(rows)} rows from PostgreSQL")


    ch_conn = Connection.get(conn_id="clickhouse_northwind")

    clickhouse_client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port or 8123,
        username=ch_conn.login,
        password=ch_conn.password,
        database=ch_conn.schema or "northwind_dw"
    )


    clickhouse_client.command("""
        TRUNCATE TABLE DimDate
    """)


    clickhouse_client.insert(
        "DimDate",
        rows,
        column_names=[
            "DateKey",
            "FullDate",
            "Day",
            "DayName",
            "DayOfWeek",
            "Month",
            "MonthName",
            "Quarter",
            "Year",
            "IsWeekend"
        ]
    )


    print(
        f"Loaded {len(rows)} rows into ClickHouse DimDate"
    )


    pg_cursor.close()
    postgres_conn.close()



with DAG(
    dag_id="etl_dimdate_clickhouse",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=["northwind","clickhouse","dimension"],
) as dag:


    load = PythonOperator(
        task_id="load_dimdate",
        python_callable=load_dimdate_to_clickhouse
    )
