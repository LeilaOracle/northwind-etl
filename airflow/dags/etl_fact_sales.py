from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import pyodbc
import psycopg2


def load_fact_sales():

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

        # ============================================================
        # 1. Extract from SQL Server
        # ============================================================

        print("Starting FactSales full load...")

        sql_cursor.execute("""
            SELECT
                o.OrderID,
                o.CustomerID,
                o.EmployeeID,
                o.OrderDate,
                o.RequiredDate,
                o.ShippedDate,
                o.Freight,
                o.ShipCountry,
                o.ShipRegion,
                o.ShipCity,
                o.ShipPostalCode,
                o.ShipAddress,

                od.ProductID,
                od.UnitPrice,
                od.Quantity,
                od.Discount,

                COUNT(*) OVER (
                    PARTITION BY o.OrderID
                ) AS OrderLineCount

            FROM Orders o

            INNER JOIN [Order Details] od
                ON o.OrderID = od.OrderID
        """)

        rows = sql_cursor.fetchall()

        print(f"Extracted {len(rows)} sales records from SQL Server")

        # ============================================================
        # 2. Drop and recreate PostgreSQL FactSales
        # ============================================================

        print("Dropping existing PostgreSQL FactSales table...")

        pg_cursor.execute("""
            DROP TABLE IF EXISTS "FactSales";
        """)

        print("Creating PostgreSQL FactSales table...")

        pg_cursor.execute("""
            CREATE TABLE "FactSales" (

                "SalesKey" SERIAL PRIMARY KEY,

                "OrderID" INTEGER NOT NULL,

                "ProductKey" INTEGER,

                "CustomerKey" INTEGER,

                "EmployeeKey" INTEGER,

                "OrderDateKey" INTEGER,

                "RequiredDateKey" INTEGER,

                "ShippedDateKey" INTEGER,

                "GeographyKey" INTEGER,

                "UnitPrice" NUMERIC(12,2),

                "Quantity" INTEGER,

                "Discount" NUMERIC(10,6),

                "GrossSales" NUMERIC(14,2),

                "DiscountAmount" NUMERIC(14,2),

                "NetSales" NUMERIC(14,2),

                "AllocatedFreight" NUMERIC(14,2),

                CONSTRAINT fk_product
                    FOREIGN KEY ("ProductKey")
                    REFERENCES "DimProduct" ("ProductKey"),

                CONSTRAINT fk_customer
                    FOREIGN KEY ("CustomerKey")
                    REFERENCES "DimCustomer" ("CustomerKey"),

                CONSTRAINT fk_employee
                    FOREIGN KEY ("EmployeeKey")
                    REFERENCES "DimEmployee" ("EmployeeKey"),

                CONSTRAINT fk_order_date
                    FOREIGN KEY ("OrderDateKey")
                    REFERENCES "DimDate" ("DateKey"),

                CONSTRAINT fk_required_date
                    FOREIGN KEY ("RequiredDateKey")
                    REFERENCES "DimDate" ("DateKey"),

                CONSTRAINT fk_shipped_date
                    FOREIGN KEY ("ShippedDateKey")
                    REFERENCES "DimDate" ("DateKey"),

                CONSTRAINT fk_geography
                    FOREIGN KEY ("GeographyKey")
                    REFERENCES "DimGeography" ("GeographyKey")
            );
        """)

        # ============================================================
        # 3. Insert statement
        # ============================================================

        insert_query = """
            INSERT INTO "FactSales" (
                "OrderID",
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
                "AllocatedFreight"
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        loaded_count = 0
        geography_warning_count = 0

        # ============================================================
        # 4. Transform and load
        # ============================================================

        for row in rows:

            (
                order_id,
                customer_id,
                employee_id,
                order_date,
                required_date,
                shipped_date,
                freight,
                ship_country,
                ship_region,
                ship_city,
                ship_postal_code,
                ship_address,
                product_id,
                unit_price,
                quantity,
                discount,
                order_line_count
            ) = row

            # --------------------------------------------------------
            # Product lookup
            # --------------------------------------------------------

            pg_cursor.execute("""
                SELECT "ProductKey"
                FROM "DimProduct"
                WHERE "ProductID" = %s
            """, (product_id,))

            result = pg_cursor.fetchone()

            if not result:
                raise ValueError(
                    f"ProductID {product_id} not found in DimProduct"
                )

            product_key = result[0]

            # --------------------------------------------------------
            # Customer lookup
            # --------------------------------------------------------

            customer_id_clean = (
                customer_id.strip()
                if customer_id
                else None
            )

            pg_cursor.execute("""
                SELECT "CustomerKey"
                FROM "DimCustomer"
                WHERE "CustomerID" = %s
            """, (customer_id_clean,))

            result = pg_cursor.fetchone()

            if not result:
                raise ValueError(
                    f"CustomerID {customer_id_clean} not found in DimCustomer"
                )

            customer_key = result[0]

            # --------------------------------------------------------
            # Employee lookup
            # --------------------------------------------------------

            pg_cursor.execute("""
                SELECT "EmployeeKey"
                FROM "DimEmployee"
                WHERE "EmployeeID" = %s
            """, (employee_id,))

            result = pg_cursor.fetchone()

            if not result:
                raise ValueError(
                    f"EmployeeID {employee_id} not found in DimEmployee"
                )

            employee_key = result[0]

            # --------------------------------------------------------
            # Date lookups
            # --------------------------------------------------------

            order_date_key = None
            required_date_key = None
            shipped_date_key = None

            if order_date:

                pg_cursor.execute("""
                    SELECT "DateKey"
                    FROM "DimDate"
                    WHERE "FullDate" = %s
                """, (order_date.date(),))

                result = pg_cursor.fetchone()

                if result:
                    order_date_key = result[0]

            if required_date:

                pg_cursor.execute("""
                    SELECT "DateKey"
                    FROM "DimDate"
                    WHERE "FullDate" = %s
                """, (required_date.date(),))

                result = pg_cursor.fetchone()

                if result:
                    required_date_key = result[0]

            if shipped_date:

                pg_cursor.execute("""
                    SELECT "DateKey"
                    FROM "DimDate"
                    WHERE "FullDate" = %s
                """, (shipped_date.date(),))

                result = pg_cursor.fetchone()

                if result:
                    shipped_date_key = result[0]

            # --------------------------------------------------------
            # Geography lookup
            # --------------------------------------------------------

            ship_country = (
                ship_country.strip()
                if ship_country
                else None
            )

            ship_region = (
                ship_region.strip()
                if ship_region
                else None
            )

            ship_city = (
                ship_city.strip()
                if ship_city
                else None
            )

            ship_postal_code = (
                ship_postal_code.strip()
                if ship_postal_code
                else None
            )

            ship_address = (
                ship_address.strip()
                if ship_address
                else None
            )

            pg_cursor.execute("""
                SELECT "GeographyKey"
                FROM "DimGeography"
                WHERE TRIM("Country") = %s
                  AND COALESCE(TRIM("Region"), '') =
                      COALESCE(%s, '')
                  AND TRIM("City") = %s
                  AND COALESCE(TRIM("PostalCode"), '') =
                      COALESCE(%s, '')
                  AND COALESCE(TRIM("Address"), '') =
                      COALESCE(%s, '')
            """, (
                ship_country,
                ship_region,
                ship_city,
                ship_postal_code,
                ship_address
            ))

            result = pg_cursor.fetchone()

            if not result:

                geography_warning_count += 1

                print(
                    "WARNING: Geography not found | "
                    f"OrderID={order_id} | "
                    f"Country={ship_country} | "
                    f"Region={ship_region} | "
                    f"City={ship_city}"
                )

            geography_key = result[0] if result else None

            # ========================================================
            # Financial calculations
            #
            # IMPORTANT:
            # Do NOT use float.
            # Use Decimal and explicit ROUND_HALF_UP.
            # ========================================================

            unit_price_decimal = Decimal(str(unit_price))

            quantity_decimal = Decimal(str(int(quantity)))

            discount_decimal = Decimal(str(discount))

            # --------------------------------------------------------
            # Gross Sales
            # --------------------------------------------------------

            gross_sales = (
                unit_price_decimal * quantity_decimal
            ).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            # --------------------------------------------------------
            # Discount Amount
            # --------------------------------------------------------

            discount_amount = (
                gross_sales * discount_decimal
            ).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            # --------------------------------------------------------
            # Net Sales
            #
            # Calculate from already-rounded GrossSales and
            # already-rounded DiscountAmount.
            # This guarantees:
            #
            # NetSales = GrossSales - DiscountAmount
            # --------------------------------------------------------

            net_sales = (
                gross_sales - discount_amount
            ).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP
            )

            # --------------------------------------------------------
            # Allocated Freight
            # --------------------------------------------------------

            if freight is not None and order_line_count:

                freight_decimal = Decimal(str(freight))

                allocated_freight = (
                    freight_decimal /
                    Decimal(str(int(order_line_count)))
                ).quantize(
                    Decimal("0.01"),
                    rounding=ROUND_HALF_UP
                )

            else:

                allocated_freight = Decimal("0.00")

            # ========================================================
            # Insert FactSales row
            # ========================================================

            pg_cursor.execute(
                insert_query,
                (
                    order_id,
                    product_key,
                    customer_key,
                    employee_key,
                    order_date_key,
                    required_date_key,
                    shipped_date_key,
                    geography_key,
                    unit_price_decimal,
                    int(quantity),
                    discount_decimal,
                    gross_sales,
                    discount_amount,
                    net_sales,
                    allocated_freight
                )
            )

            loaded_count += 1

        # ============================================================
        # 5. Commit
        # ============================================================

        postgres_conn.commit()

        print(
            f"Successfully loaded {loaded_count} records "
            "into PostgreSQL FactSales"
        )

        print(
            f"Geography lookup warnings: "
            f"{geography_warning_count}"
        )

        # ============================================================
        # 6. Validation
        # ============================================================

        pg_cursor.execute("""
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT "OrderID") AS order_count,
                COUNT(DISTINCT "ProductKey") AS product_count
            FROM "FactSales";
        """)

        validation_result = pg_cursor.fetchone()

        print(
            "FactSales validation: "
            f"Rows={validation_result[0]}, "
            f"Orders={validation_result[1]}, "
            f"Products={validation_result[2]}"
        )

        # ------------------------------------------------------------
        # Validate financial calculations
        # ------------------------------------------------------------

        pg_cursor.execute("""
            SELECT
                COUNT(*) AS calculation_errors
            FROM "FactSales"
            WHERE
                ROUND("NetSales", 2) <>
                ROUND("GrossSales" - "DiscountAmount", 2);
        """)

        calculation_errors = pg_cursor.fetchone()[0]

        print(
            f"Financial calculation validation errors: "
            f"{calculation_errors}"
        )

        if calculation_errors != 0:
            raise ValueError(
                "Financial calculation validation failed: "
                f"{calculation_errors} rows have inconsistent NetSales"
            )

        # ------------------------------------------------------------
        # Financial totals
        # ------------------------------------------------------------

        pg_cursor.execute("""
            SELECT
                SUM("GrossSales") AS GrossSales,
                SUM("DiscountAmount") AS DiscountAmount,
                SUM("NetSales") AS NetSales,
                SUM("AllocatedFreight") AS AllocatedFreight
            FROM "FactSales";
        """)

        totals = pg_cursor.fetchone()

        print(
            "FactSales financial totals:"
        )

        print(
            f"GrossSales       = {totals[0]}"
        )

        print(
            f"DiscountAmount   = {totals[1]}"
        )

        print(
            f"NetSales         = {totals[2]}"
        )

        print(
            f"AllocatedFreight = {totals[3]}"
        )

        print("FactSales validation completed successfully")

    except Exception as e:

        postgres_conn.rollback()

        print(
            f"FactSales ETL Error: {str(e)}"
        )

        raise

    finally:

        sql_cursor.close()
        sqlserver_conn.close()

        pg_cursor.close()
        postgres_conn.close()


# ================================================================
# DAG definition
# ================================================================

with DAG(
    dag_id="etl_fact_sales",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "northwind",
        "fact",
        "sales"
    ],
) as dag:

    load_fact_sales_task = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
    )