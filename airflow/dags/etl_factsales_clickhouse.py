from datetime import datetime

import psycopg2
import clickhouse_connect

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook

# ============================================================
# PostgreSQL → ClickHouse FactSales Full Load
# ============================================================

def load_factsales_to_clickhouse():

    # --------------------------------------------------------
    # 1. PostgreSQL connection
    # --------------------------------------------------------
    pg_conn = Connection.get(conn_id="northwind_postgres")

    postgres_conn = psycopg2.connect(
        host=pg_conn.host,
        port=pg_conn.port or 5432,
        database=pg_conn.schema or "northwind",
        user=pg_conn.login,
        password=pg_conn.password,
    )

    pg_cursor = postgres_conn.cursor()

    # --------------------------------------------------------
    # 2. ClickHouse connection from Airflow Connection
    # --------------------------------------------------------
    ch_conn = BaseHook.get_connection("clickhouse_northwind")

    clickhouse_client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port or 8123,
        username=ch_conn.login,
        password=ch_conn.password,
        database=ch_conn.schema or "northwind_dw",
    )

    try:

        print("=" * 70)
        print("Starting PostgreSQL FactSales → ClickHouse FactSales")
        print("=" * 70)

        # ----------------------------------------------------
        # 3. Read FactSales from PostgreSQL
        #
        # ProductID is not stored directly in PostgreSQL FactSales.
        # Therefore we join DimProduct using ProductKey.
        # ----------------------------------------------------
        pg_cursor.execute("""
            SELECT
                fs."SalesKey",
                fs."OrderID",
                dp."ProductID",
                fs."ProductKey",
                fs."CustomerKey",
                fs."EmployeeKey",
                fs."OrderDateKey",
                fs."RequiredDateKey",
                fs."ShippedDateKey",
                fs."GeographyKey",
                fs."UnitPrice",
                fs."Quantity",
                fs."Discount",
                fs."GrossSales",
                fs."DiscountAmount",
                fs."NetSales",
                fs."AllocatedFreight"
            FROM "FactSales" fs
            INNER JOIN "DimProduct" dp
                ON fs."ProductKey" = dp."ProductKey"
            ORDER BY
                fs."SalesKey"
        """)

        rows = pg_cursor.fetchall()

        print(f"Extracted {len(rows)} rows from PostgreSQL FactSales")

        if not rows:
            raise ValueError(
                "PostgreSQL FactSales is empty. "
                "ClickHouse FactSales was not modified."
            )

        # ----------------------------------------------------
        # 4. Full refresh of ClickHouse FactSales
        #
        # We intentionally use TRUNCATE rather than DROP.
        # The ClickHouse table structure must remain intact.
        # ----------------------------------------------------
        print("Truncating ClickHouse FactSales...")

        clickhouse_client.command("""
            TRUNCATE TABLE northwind_dw.FactSales
        """)

        print("ClickHouse FactSales truncated successfully")

        # ----------------------------------------------------
        # 5. Prepare ClickHouse rows
        # ----------------------------------------------------
        processed_at = datetime.now()

        clickhouse_rows = []

        for row in rows:

            (
                sales_key,
                order_id,
                product_id,
                product_key,
                customer_key,
                employee_key,
                order_date_key,
                required_date_key,
                shipped_date_key,
                geography_key,
                unit_price,
                quantity,
                discount,
                gross_sales,
                discount_amount,
                net_sales,
                allocated_freight,
            ) = row

            clickhouse_rows.append(
                (
                    int(sales_key),
                    int(order_id),
                    int(product_id),
                    int(product_key),
                    int(customer_key),
                    int(employee_key),
                    int(order_date_key),

                    int(required_date_key)
                    if required_date_key is not None
                    else None,

                    int(shipped_date_key)
                    if shipped_date_key is not None
                    else None,

                    int(geography_key)
                    if geography_key is not None
                    else None,

                    unit_price,
                    int(quantity),
                    discount,
                    gross_sales,
                    discount_amount,
                    net_sales,
                    allocated_freight,

                    "I",
                    1,
                    0,
                    processed_at,
                )
            )

        # ----------------------------------------------------
        # 6. Insert into ClickHouse
        # ----------------------------------------------------
        print(
            f"Inserting {len(clickhouse_rows)} rows "
            "into ClickHouse FactSales..."
        )

        clickhouse_client.insert(
            "northwind_dw.FactSales",
            clickhouse_rows,
            column_names=[
                "SalesKey",
                "OrderID",
                "ProductID",
                "ProductKey",
                "CustomerKey",
                "EmployeeKey",
                "OrderDateKey",
                "RequiredDateKey",
                "ShippedDateKey",
                "GeographyKey",
                "UnitPrice",
                "Quantity",
                "Discount",
                "GrossSales",
                "DiscountAmount",
                "NetSales",
                "AllocatedFreight",
                "CDCOperation",
                "CDCVersion",
                "IsDeleted",
                "CDCProcessedAt",
            ],
        )

        # ----------------------------------------------------
        # 7. Validation
        # ----------------------------------------------------
        result = clickhouse_client.query("""
            SELECT
                count() AS RowCount,
                uniqExact(OrderID) AS OrderCount,
                uniqExact(ProductID) AS ProductCount
            FROM northwind_dw.FactSales
        """)

        print("ClickHouse FactSales validation:")
        print(result.result_rows)

        clickhouse_count = result.result_rows[0][0]

        if clickhouse_count != len(rows):
            raise ValueError(
                f"Row count mismatch: "
                f"PostgreSQL={len(rows)}, "
                f"ClickHouse={clickhouse_count}"
            )

        # ----------------------------------------------------
        # 8. Validate financial totals
        # ----------------------------------------------------
        pg_cursor.execute("""
            SELECT
                COUNT(*),
                COALESCE(SUM("GrossSales"), 0),
                COALESCE(SUM("DiscountAmount"), 0),
                COALESCE(SUM("NetSales"), 0),
                COALESCE(SUM("AllocatedFreight"), 0)
            FROM "FactSales"
        """)

        pg_totals = pg_cursor.fetchone()

        ch_totals_result = clickhouse_client.query("""
            SELECT
                count(),
                sum(GrossSales),
                sum(DiscountAmount),
                sum(NetSales),
                sum(AllocatedFreight)
            FROM northwind_dw.FactSales
        """)

        ch_totals = ch_totals_result.result_rows[0]

        print("PostgreSQL totals:")
        print(pg_totals)

        print("ClickHouse totals:")
        print(ch_totals)

        # ----------------------------------------------------
        # 9. Compare totals
        # ----------------------------------------------------
        if pg_totals[0] != ch_totals[0]:
            raise ValueError(
                f"Count mismatch: "
                f"PostgreSQL={pg_totals[0]}, "
                f"ClickHouse={ch_totals[0]}"
            )

        for i, column_name in enumerate(
            [
                "GrossSales",
                "DiscountAmount",
                "NetSales",
                "AllocatedFreight",
            ],
            start=1,
        ):

            pg_value = pg_totals[i]
            ch_value = ch_totals[i]

            if pg_value != ch_value:
                raise ValueError(
                    f"{column_name} mismatch: "
                    f"PostgreSQL={pg_value}, "
                    f"ClickHouse={ch_value}"
                )

        print("=" * 70)
        print("SUCCESS")
        print(
            f"Loaded {len(rows)} rows "
            "from PostgreSQL FactSales to ClickHouse FactSales"
        )
        print("=" * 70)

    except Exception as e:

        print("=" * 70)
        print("ERROR loading FactSales to ClickHouse")
        print(str(e))
        print("=" * 70)

        raise

    finally:

        pg_cursor.close()
        postgres_conn.close()
        clickhouse_client.close()


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="etl_factsales_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "northwind",
        "fact",
        "sales",
        "clickhouse",
    ],
) as dag:

    load_fact_sales_clickhouse = PythonOperator(
        task_id="load_factsales_to_clickhouse",
        python_callable=load_factsales_to_clickhouse,
    )
