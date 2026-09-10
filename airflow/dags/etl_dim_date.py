from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta
import psycopg2


def load_dim_date():

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
        # Ensure Dimension Table Exists
        # =========================
        pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS "DimDate" (

                "DateKey" INTEGER PRIMARY KEY,

                "FullDate" DATE NOT NULL,

                "Day" INTEGER NOT NULL,

                "DayName" VARCHAR(20),

                "DayOfWeek" INTEGER,

                "Month" INTEGER NOT NULL,

                "MonthName" VARCHAR(20),

                "Quarter" INTEGER,

                "Year" INTEGER NOT NULL,

                "IsWeekend" BOOLEAN
            );
        """)

        # =========================
        # Full Reload
        # =========================
        pg_cursor.execute("""
            TRUNCATE TABLE "DimDate" CASCADE;
        """)
        # =========================
        # Generate Dates
        # =========================

        start_date = datetime(1990, 1, 1)
        end_date = datetime(2030, 12, 31)

        current_date = start_date

        insert_query = """
            INSERT INTO "DimDate" (
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
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """

        records = []

        while current_date <= end_date:

            date_key = int(
                current_date.strftime("%Y%m%d")
            )

            day_of_week = current_date.isoweekday()

            is_weekend = (
                day_of_week == 6 or
                day_of_week == 7
            )

            records.append(
                (
                    date_key,
                    current_date.date(),
                    current_date.day,
                    current_date.strftime("%A"),
                    day_of_week,
                    current_date.month,
                    current_date.strftime("%B"),
                    (current_date.month - 1) // 3 + 1,
                    current_date.year,
                    is_weekend
                )
            )

            current_date += timedelta(days=1)

        # =========================
        # Load
        # =========================
        pg_cursor.executemany(
            insert_query,
            records
        )

        postgres_conn.commit()

        print(
            f"Successfully loaded {len(records)} records into DimDate"
        )

    except Exception as e:

        postgres_conn.rollback()

        print(f"ETL Error: {str(e)}")

        raise

    finally:

        pg_cursor.close()
        postgres_conn.close()


# =========================
# DAG Definition
# =========================

with DAG(
    dag_id="etl_dim_date",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=["northwind", "dimension", "date"]
) as dag:

    load_date_task = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )
