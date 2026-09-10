from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

import pymssql
import psycopg2


def extract_and_load_supplier():
    """
    Extract supplier data from SQL Server
    and fully reload DimSupplier in PostgreSQL.
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
                SupplierID,
                CompanyName,
                ContactName,
                ContactTitle,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                Phone,
                Fax,
                HomePage
            FROM dbo.Suppliers;
        """

        sqlserver_cursor.execute(extract_query)

        rows = sqlserver_cursor.fetchall()

        print(
            f"Extracted {len(rows)} supplier records "
            "from SQL Server"
        )


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
            CREATE TABLE IF NOT EXISTS "DimSupplier" (
                "SupplierKey" SERIAL PRIMARY KEY,
                "SupplierID" INTEGER,
                "CompanyName" VARCHAR(100),
                "ContactName" VARCHAR(100),
                "ContactTitle" VARCHAR(100),
                "Address" VARCHAR(255),
                "City" VARCHAR(100),
                "Region" VARCHAR(100),
                "PostalCode" VARCHAR(20),
                "Country" VARCHAR(100),
                "Phone" VARCHAR(50),
                "Fax" VARCHAR(50),
                "HomePage" TEXT
            );
        """)


        # ==========================================
        # 4. Full Load - Clear old data
        # ==========================================

        postgres_cursor.execute("""
            TRUNCATE TABLE "DimSupplier"
            RESTART IDENTITY;
        """)


        # ==========================================
        # 5. Load Data
        # ==========================================

        insert_query = """
            INSERT INTO "DimSupplier"
            (
                "SupplierID",
                "CompanyName",
                "ContactName",
                "ContactTitle",
                "Address",
                "City",
                "Region",
                "PostalCode",
                "Country",
                "Phone",
                "Fax",
                "HomePage"
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """

        postgres_cursor.executemany(
            insert_query,
            rows
        )

        postgres_conn.commit()

        print(
            f"Successfully loaded {len(rows)} records "
            "into DimSupplier"
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


# ==========================================
# Airflow DAG
# ==========================================

with DAG(

    dag_id="etl_dimsupplier_postgres",

    start_date=datetime(2025, 1, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "northwind",
        "sqlserver",
        "postgres",
        "etl",
        "dimension"
    ],

) as dag:

    load_dimsupplier = PythonOperator(
        task_id="extract_and_load_dimsupplier",
        python_callable=extract_and_load_supplier,
    )
