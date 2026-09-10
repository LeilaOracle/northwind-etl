from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

import psycopg2


def cleanup_fact_sales():

    pg_conn = Connection.get(conn_id="northwind_postgres")

    postgres_conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port or 5432,
        database=pg_conn.schema or "northwind",
        user=pg_conn.login,
        password=pg_conn.password,
    )

    pg_cursor = postgres_conn.cursor()

    try:

        print("Dropping FactSales table...")

        pg_cursor.execute("""
            DROP TABLE IF EXISTS "FactSales";
        """)

        postgres_conn.commit()

        print("FactSales dropped successfully")

    except Exception as e:

        postgres_conn.rollback()

        print(f"Cleanup Error: {str(e)}")

        raise

    finally:

        pg_cursor.close()
        postgres_conn.close()


with DAG(
    dag_id="etl_cleanup_fact_sales",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["northwind", "etl", "cleanup"]
) as dag:

    cleanup_task = PythonOperator(
        task_id="cleanup_fact_sales",
        python_callable=cleanup_fact_sales
    )
