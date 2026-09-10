from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

import pymssql
import psycopg2


def extract_and_load_geography():

    sqlserver_conn = None
    sqlserver_cursor = None
    postgres_conn = None
    postgres_cursor = None

    try:
        # ==========================================
        # 1. Connect to SQL Server (Source)
        # ==========================================

        sql_conn = Connection.get(conn_id="northwind_sqlserver")

        sqlserver_conn = pymssql.connect(
            server=sql_conn.host,
            user=sql_conn.login,
            password=sql_conn.password,
            database=sql_conn.schema or "Northwind",
            port=sql_conn.port or 1433,
        )

        sqlserver_cursor = sqlserver_conn.cursor()

        # ==========================================
        # 2. Extract Geography Data
        # ==========================================

        extract_query = """
            SELECT
                Country,
                Region,
                City,
                PostalCode,
                Address
            FROM dbo.Customers

            UNION

            SELECT
                Country,
                Region,
                City,
                PostalCode,
                Address
            FROM dbo.Suppliers

            UNION

            SELECT
                Country,
                Region,
                City,
                PostalCode,
                Address
            FROM dbo.Employees

            UNION

            SELECT
                ShipCountry AS Country,
                ShipRegion AS Region,
                ShipCity AS City,
                ShipPostalCode AS PostalCode,
                ShipAddress AS Address
            FROM dbo.Orders
            WHERE ShipCountry IS NOT NULL;
        """

        sqlserver_cursor.execute(extract_query)

        rows = sqlserver_cursor.fetchall()

        print(f"Extracted {len(rows)} geography records from SQL Server")

        # ==========================================
        # 3. Connect to PostgreSQL (Target)
        # ==========================================

        pg_conn = Connection.get(conn_id="northwind_postgres")

        postgres_conn = psycopg2.connect(
            host=pg_conn.host,
            port=pg_conn.port or 5432,
            database=pg_conn.schema or "northwind",
            user=pg_conn.login,
            password=pg_conn.password,
        )

        postgres_cursor = postgres_conn.cursor()

        # ==========================================
        # 4. Create table if it does not exist
        # ==========================================

        postgres_cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DimGeography" (
                "GeographyKey" SERIAL PRIMARY KEY,
                "Country" VARCHAR(100),
                "Region" VARCHAR(100),
                "City" VARCHAR(100),
                "PostalCode" VARCHAR(20),
                "Address" VARCHAR(255)
            );
        """)

        # ==========================================
        # 5. Full Load - Clear old data
        # ==========================================

        postgres_cursor.execute("""
        TRUNCATE TABLE "DimGeography" RESTART IDENTITY;
        """)

        # ==========================================
        # 6. Load Data
        # ==========================================

        insert_query = """
            INSERT INTO "DimGeography"
            (
                "Country",
                "Region",
                "City",
                "PostalCode",
                "Address"
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        postgres_cursor.executemany(insert_query, rows)

        postgres_conn.commit()

        print(
            f"Successfully loaded {len(rows)} records into DimGeography"
        )

    except Exception as e:

        if postgres_conn:
            postgres_conn.rollback()

        print(f"ETL Error: {str(e)}")

        raise

    finally:

        if sqlserver_cursor:
            sqlserver_cursor.close()

        if sqlserver_conn:
            sqlserver_conn.close()

        if postgres_cursor:
            postgres_cursor.close()

        if postgres_conn:
            postgres_conn.close()


with DAG(
    dag_id="etl_dim_geography",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["northwind", "etl", "dimension"],
) as dag:

    load_dim_geography = PythonOperator(
        task_id="extract_and_load_dim_geography",
        python_callable=extract_and_load_geography,
    )