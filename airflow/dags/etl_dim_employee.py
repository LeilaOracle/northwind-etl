from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import pyodbc
import psycopg2


def load_dim_employee():

    # =========================
    # SQL Server Connection
    # =========================
    sql_conn = Connection.get(conn_id="northwind_sqlserver")

    sqlserver_conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_conn.host},{sql_conn.port or 1433};"
        f"DATABASE={sql_conn.schema or 'Northwind'};"
        f"UID={sql_conn.login};"
        f"PWD={sql_conn.password};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )

    sql_cursor = sqlserver_conn.cursor()

    # =========================
    # PostgreSQL Connection
    # =========================
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

        # =========================
        # Extract
        # =========================
        sql_cursor.execute("""
            SELECT
                EmployeeID,
                LastName,
                FirstName,
                Title,
                TitleOfCourtesy,
                BirthDate,
                HireDate,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                HomePhone,
                Extension,
                ReportsTo
            FROM Employees
        """)

        rows = sql_cursor.fetchall()

        print(f"Extracted {len(rows)} employees from SQL Server")

        # =========================
        # Create table if not exists
        # =========================
        pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DimEmployee" (

                "EmployeeKey" SERIAL PRIMARY KEY,

                "EmployeeID" INTEGER,

                "LastName" VARCHAR(20),

                "FirstName" VARCHAR(20),

                "Title" VARCHAR(50),

                "TitleOfCourtesy" VARCHAR(30),

                "BirthDate" TIMESTAMP,

                "HireDate" TIMESTAMP,

                "Address" VARCHAR(100),

                "City" VARCHAR(50),

                "Region" VARCHAR(50),

                "PostalCode" VARCHAR(20),

                "Country" VARCHAR(50),

                "HomePhone" VARCHAR(30),

                "Extension" VARCHAR(10),

                "ReportsTo" INTEGER
            );
        """)

        # =========================
        # Full Load - Clear old data
        # =========================
        pg_cursor.execute(
            'TRUNCATE TABLE "DimEmployee" RESTART IDENTITY CASCADE;'
        )

        # =========================
        # Load
        # =========================
        insert_query = """
            INSERT INTO "DimEmployee" (
                "EmployeeID",
                "LastName",
                "FirstName",
                "Title",
                "TitleOfCourtesy",
                "BirthDate",
                "HireDate",
                "Address",
                "City",
                "Region",
                "PostalCode",
                "Country",
                "HomePhone",
                "Extension",
                "ReportsTo"
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """

        for row in rows:
            pg_cursor.execute(insert_query, tuple(row))

        postgres_conn.commit()

        print(
            f"Successfully loaded {len(rows)} records into DimEmployee"
        )

    except Exception as e:

        postgres_conn.rollback()

        print(f"ETL Error: {str(e)}")

        raise

    finally:

        sql_cursor.close()
        sqlserver_conn.close()

        pg_cursor.close()
        postgres_conn.close()


with DAG(
    dag_id="etl_dim_employee",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["northwind", "dimension", "employee"]
) as dag:

    load_employee_task = PythonOperator(
        task_id="load_dim_employee",
        python_callable=load_dim_employee
    )