from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

import pymssql
import psycopg2


def extract_and_load_customer():
    """
    Extract customer data from SQL Server
    and fully reload DimCustomer in PostgreSQL.
    """

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

        extract_query = """
            SELECT
                CustomerID,
                CompanyName,
                ContactName,
                ContactTitle,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                Phone,
                Fax
            FROM dbo.Customers;
        """

        sqlserver_cursor.execute(extract_query)

        rows = sqlserver_cursor.fetchall()

        print(f"Extracted {len(rows)} customer records from SQL Server")

        # ==========================================
        # 2. Connect to PostgreSQL (Target)
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
        # 3. Create table if not exists
        # ==========================================

        postgres_cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DimCustomer" (
                "CustomerKey" SERIAL PRIMARY KEY,
                "CustomerID" VARCHAR(10),
                "CompanyName" VARCHAR(100),
                "ContactName" VARCHAR(100),
                "ContactTitle" VARCHAR(100),
                "Address" VARCHAR(255),
                "City" VARCHAR(100),
                "Region" VARCHAR(100),
                "PostalCode" VARCHAR(20),
                "Country" VARCHAR(100),
                "Phone" VARCHAR(50),
                "Fax" VARCHAR(50)
            );
        """)

        # ==========================================
        # 4. Full Load - Clear old data
        # ==========================================

        postgres_cursor.execute("""
        TRUNCATE TABLE "DimCustomer" RESTART IDENTITY;
        """)

        # ==========================================
        # 5. Load Data
        # ==========================================

        insert_query = """
            INSERT INTO "DimCustomer"
            (
                "CustomerID",
                "CompanyName",
                "ContactName",
                "ContactTitle",
                "Address",
                "City",
                "Region",
                "PostalCode",
                "Country",
                "Phone",
                "Fax"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        postgres_cursor.executemany(insert_query, rows)

        postgres_conn.commit()

        print(f"Successfully loaded {len(rows)} records into DimCustomer")

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
    dag_id="etl_dim_customer",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["northwind", "etl", "dimension"],
) as dag:

    load_dim_customer = PythonOperator(
        task_id="extract_and_load_dim_customer",
        python_callable=extract_and_load_customer,
    )