from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimProduct - SCD Type 2 using PySpark
# ============================================================

def load_dimproduct_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim,
        when
    )

    import clickhouse_connect


    print("Starting DimProduct SCD Type 2 ETL...")


    # ========================================================
    # PostgreSQL Configuration
    # ========================================================

    pg_conn = Connection.get(conn_id="northwind_postgres")
    ch_conn = Connection.get(conn_id="clickhouse_northwind")

    POSTGRES_URL = (
        f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port or 5432}/{pg_conn.schema or 'northwind'}"
    )

    POSTGRES_PROPERTIES = {
        "user": pg_conn.login,
        "password": pg_conn.password,
        "driver": "org.postgresql.Driver"
    }


    # ========================================================
    # ClickHouse Configuration
    # ========================================================

    CLICKHOUSE_URL = (
        f"jdbc:clickhouse://{ch_conn.host}:{ch_conn.port or 8123}/{ch_conn.schema or 'northwind_dw'}"
    )

    CLICKHOUSE_PROPERTIES = {
        "user": ch_conn.login,
        "password": ch_conn.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }


    # ========================================================
    # JDBC Driver JARs
    # ========================================================

    POSTGRES_JAR = (
        "/opt/airflow/spark-jars/"
        "postgresql-42.7.13.jar"
    )

    CLICKHOUSE_JAR = (
        "/opt/airflow/spark-jars/"
        "clickhouse-jdbc-all-0.10.0.jar"
    )


    # ========================================================
    # Create Spark Session
    # ========================================================

    spark = (
        SparkSession.builder
        .appName("DimProduct_SCD2")

        .config(
            "spark.jars",
            f"{POSTGRES_JAR},{CLICKHOUSE_JAR}"
        )

        .config(
            "spark.driver.extraClassPath",
            f"{POSTGRES_JAR}:{CLICKHOUSE_JAR}"
        )

        .config(
            "spark.executor.extraClassPath",
            f"{POSTGRES_JAR}:{CLICKHOUSE_JAR}"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created successfully.")


    ch_client = None


    try:

        # ====================================================
        # 1. Extract Source Data from PostgreSQL
        # ====================================================

        print(
            "Reading DimProduct from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimProduct"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} product records "
            "from PostgreSQL."
        )


        # ====================================================
        # 2. Read Current Target Data from ClickHouse
        # ====================================================

        print(
            "Reading current DimProduct records "
            "from ClickHouse..."
        )

        target_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,
                table="""
                (
                    SELECT
                        ProductKey,
                        ProductAlternateKey,
                        ProductName,
                        SupplierID,
                        CategoryID,
                        QuantityPerUnit,
                        UnitPrice,
                        UnitsInStock,
                        UnitsOnOrder,
                        ReorderLevel,
                        Discontinued,
                        StartDate,
                        EndDate,
                        IsCurrent,
                        Version
                    FROM DimProduct
                    WHERE IsCurrent = 1
                ) AS current_products
                """,
                properties=CLICKHOUSE_PROPERTIES
            )
        )

        target_count = target_df.count()

        print(
            "Total current records in ClickHouse: "
            f"{target_count}"
        )


        # ====================================================
        # 3. Business Columns
        # ====================================================

        business_columns = [
            "ProductName",
            "SupplierID",
            "CategoryID",
            "QuantityPerUnit",
            "UnitPrice",
            "UnitsInStock",
            "UnitsOnOrder",
            "ReorderLevel",
            "Discontinued"
        ]


        # ====================================================
        # 4. Deterministic NULL-safe Hash Function
        #
        # IMPORTANT:
        #
        # PostgreSQL:
        #   Discontinued = Boolean (true / false)
        #
        # ClickHouse:
        #   Discontinued = UInt8 (1 / 0)
        #
        # Therefore both sides are normalized to:
        #   1 / 0
        #
        # UnitPrice is explicitly cast to Decimal(12,2)
        # before conversion to string.
        #
        # All nullable strings:
        #   trim + NULL -> <NULL>
        # ====================================================

        def normalize_for_hash(column_name):

            # ------------------------------------------------
            # String columns
            # ------------------------------------------------

            if column_name in [
                "ProductName",
                "QuantityPerUnit"
            ]:

                return coalesce(
                    trim(
                        col(column_name)
                        .cast("string")
                    ),
                    lit("<NULL>")
                )


            # ------------------------------------------------
            # UnitPrice
            #
            # Normalize both PostgreSQL numeric and ClickHouse
            # Decimal to identical Decimal(12,2)
            # ------------------------------------------------

            elif column_name == "UnitPrice":

                return coalesce(
                    col(column_name)
                    .cast("decimal(12,2)")
                    .cast("string"),
                    lit("<NULL>")
                )


            # ------------------------------------------------
            # Discontinued
            #
            # PostgreSQL Boolean:
            #   true / false
            #
            # ClickHouse UInt8:
            #   1 / 0
            #
            # Normalize both to:
            #   1 / 0
            # ------------------------------------------------

            elif column_name == "Discontinued":

                return coalesce(

                    when(
                        col(column_name)
                        .cast("string")
                        .isin(
                            "true",
                            "TRUE",
                            "1"
                        ),
                        lit("1")
                    )

                    .otherwise(
                        lit("0")
                    ),

                    lit("<NULL>")
                )


            # ------------------------------------------------
            # Numeric columns
            # ------------------------------------------------

            else:

                return coalesce(
                    col(column_name)
                    .cast("long")
                    .cast("string"),
                    lit("<NULL>")
                )


        # ====================================================
        # Create Hash
        # ====================================================

        def create_hash(df, hash_column_name):

            normalized_columns = [

                normalize_for_hash(c)

                for c in business_columns
            ]


            return (

                df.withColumn(

                    hash_column_name,

                    sha2(

                        concat_ws(
                            "||",
                            *normalized_columns
                        ),

                        256
                    )
                )
            )


        # ====================================================
        # 5. Calculate Source Hash
        # ====================================================

        print(
            "Calculating source RowHash..."
        )

        source_with_hash = (
            create_hash(
                source_df,
                "SourceRowHash"
            )
        )


        # ====================================================
        # 6. Calculate Target Hash
        # ====================================================

        print(
            "Calculating current target RowHash..."
        )

        target_with_hash = (
            create_hash(
                target_df,
                "TargetRowHash"
            )
        )


        # ====================================================
        # 7. Prepare Target Comparison Dataset
        # ====================================================

        target_compare_df = (

            target_with_hash

            .select(

                col(
                    "ProductAlternateKey"
                )
                .cast("int")
                .alias(
                    "TargetProductID"
                ),

                col(
                    "Version"
                )
                .cast("long")
                .alias(
                    "TargetVersion"
                ),

                col(
                    "TargetRowHash"
                )
            )
        )


        # ====================================================
        # 8. Compare Source and Target
        # ====================================================

        comparison_df = (

            source_with_hash

            .join(

                target_compare_df,

                source_with_hash[
                    "ProductID"
                ].cast("int")

                ==

                target_compare_df[
                    "TargetProductID"
                ],

                "left"
            )
        )


        # ====================================================
        # 9. Materialize Comparison
        # ====================================================

        print(
            "Materializing SCD comparison results..."
        )

        comparison_df = (
            comparison_df.cache()
        )

        comparison_df.count()


        # ====================================================
        # 10. Detect New Products
        # ====================================================

        new_products_df = (

            comparison_df

            .filter(
                col(
                    "TargetProductID"
                ).isNull()
            )

            .cache()
        )

        new_count = (
            new_products_df.count()
        )


        # ====================================================
        # 11. Detect Changed Products
        # ====================================================

        changed_products_df = (

            comparison_df

            .filter(
                col(
                    "TargetProductID"
                ).isNotNull()
            )

            .filter(

                col(
                    "SourceRowHash"
                )

                !=

                col(
                    "TargetRowHash"
                )
            )

            .cache()
        )

        changed_count = (
            changed_products_df.count()
        )


        # ====================================================
        # 12. Detect Unchanged Products
        # ====================================================

        unchanged_products_df = (

            comparison_df

            .filter(
                col(
                    "TargetProductID"
                ).isNotNull()
            )

            .filter(

                col(
                    "SourceRowHash"
                )

                ==

                col(
                    "TargetRowHash"
                )
            )
        )

        unchanged_count = (
            unchanged_products_df.count()
        )


        print("=" * 60)

        print(
            f"New products: {new_count}"
        )

        print(
            f"Changed products: {changed_count}"
        )

        print(
            f"Unchanged products: "
            f"{unchanged_count}"
        )

        print("=" * 60)


        # ====================================================
        # 13. INITIAL LOAD
        # ====================================================

        if target_count == 0:

            print(
                "Initial load detected."
            )

            load_timestamp = datetime.now()


            initial_load_df = (

                source_df

                .select(

                    col("ProductKey")
                    .cast("long"),

                    col("ProductID")
                    .cast("long")
                    .alias(
                        "ProductAlternateKey"
                    ),

                    *[
                        col(c)
                        for c in business_columns
                    ],

                    lit(load_timestamp)
                    .cast("timestamp")
                    .alias(
                        "StartDate"
                    ),

                    lit(None)
                    .cast("timestamp")
                    .alias(
                        "EndDate"
                    ),

                    lit(1)
                    .cast("byte")
                    .alias(
                        "IsCurrent"
                    ),

                    lit(1)
                    .cast("int")
                    .alias(
                        "Version"
                    )
                )
            )


            print(
                f"Inserting {source_count} "
                "initial product records..."
            )


            (
                initial_load_df

                .write

                .jdbc(

                    url=CLICKHOUSE_URL,

                    table="DimProduct",

                    mode="append",

                    properties=CLICKHOUSE_PROPERTIES
                )
            )


            print("=" * 60)

            print(
                "DimProduct initial load "
                "completed successfully."
            )

            print(
                f"Source records: "
                f"{source_count}"
            )

            print(
                f"Initial records inserted: "
                f"{source_count}"
            )

            print("=" * 60)

            return


        # ====================================================
        # 14. NOTHING CHANGED
        # ====================================================

        if (

            new_count == 0

            and

            changed_count == 0
        ):

            print(
                "No new or changed products detected."
            )

            print(
                "SCD Type 2 load completed successfully."
            )

            return


        # ====================================================
        # 15. One Timestamp Per ETL Run
        # ====================================================

        load_timestamp = datetime.now()

        print(
            f"ETL timestamp: {load_timestamp}"
        )


        # ====================================================
        # 16. ClickHouse Native Client
        # ====================================================

        ch_client = (

            clickhouse_connect.get_client(

                host=ch_conn.host,

                port=ch_conn.port or 8123,

                username=ch_conn.login,

                password=ch_conn.password,

                database=ch_conn.schema or "northwind_dw"
            )
        )


        # ====================================================
        # 17. Prepare New SCD Type 2 Versions
        # ====================================================

        print(
            "Preparing new SCD Type 2 versions..."
        )


        # ----------------------------------------------------
        # Changed Product Versions
        # ----------------------------------------------------

        changed_versions_df = (

            changed_products_df

            .select(

                col("ProductKey")
                .cast("long"),

                col("ProductID")
                .cast("long")
                .alias(
                    "ProductAlternateKey"
                ),

                *[
                    col(c)
                    for c in business_columns
                ],

                lit(load_timestamp)
                .cast("timestamp")
                .alias(
                    "StartDate"
                ),

                lit(None)
                .cast("timestamp")
                .alias(
                    "EndDate"
                ),

                lit(1)
                .cast("byte")
                .alias(
                    "IsCurrent"
                ),

                (
                    col(
                        "TargetVersion"
                    )
                    .cast("long")

                    +

                    lit(1)
                )
                .cast("int")
                .alias(
                    "Version"
                )
            )
        )


        # ----------------------------------------------------
        # New Product Versions
        # ----------------------------------------------------

        new_versions_df = (

            new_products_df

            .select(

                col("ProductKey")
                .cast("long"),

                col("ProductID")
                .cast("long")
                .alias(
                    "ProductAlternateKey"
                ),

                *[
                    col(c)
                    for c in business_columns
                ],

                lit(load_timestamp)
                .cast("timestamp")
                .alias(
                    "StartDate"
                ),

                lit(None)
                .cast("timestamp")
                .alias(
                    "EndDate"
                ),

                lit(1)
                .cast("byte")
                .alias(
                    "IsCurrent"
                ),

                lit(1)
                .cast("int")
                .alias(
                    "Version"
                )
            )
        )


        # ----------------------------------------------------
        # Combine New + Changed Records
        # ----------------------------------------------------

        records_to_insert_df = (

            changed_versions_df

            .unionByName(
                new_versions_df
            )

            .cache()
        )


        insert_count = (
            records_to_insert_df.count()
        )


        print(
            f"Prepared {insert_count} "
            "new SCD Type 2 versions."
        )


        # ====================================================
        # 18. Get Changed Product IDs
        # ====================================================

        changed_ids = [

            row["ProductID"]

            for row in (

                changed_products_df

                .select(
                    "ProductID"
                )

                .collect()
            )
        ]


        close_count = (
            len(changed_ids)
        )


        # ====================================================
        # 19. Close Old Versions
        # ====================================================

        if changed_ids:

            print(
                "Closing old product versions..."
            )


            for product_id in changed_ids:

                ch_client.command(

                    """
                    ALTER TABLE northwind_dw.DimProduct
                    UPDATE
                        EndDate = %(end_date)s,
                        IsCurrent = 0
                    WHERE
                        ProductAlternateKey =
                            %(product_id)s
                        AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """,

                    parameters={

                        "end_date": load_timestamp,

                        "product_id": product_id
                    }
                )


            print(
                f"Closed {close_count} "
                "old product versions."
            )


        # ====================================================
        # 20. Insert New Versions
        # ====================================================

        if insert_count > 0:

            print(
                f"Inserting {insert_count} "
                "new product versions..."
            )


            (
                records_to_insert_df

                .write

                .jdbc(

                    url=CLICKHOUSE_URL,

                    table="DimProduct",

                    mode="append",

                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # 21. Final Summary
        # ====================================================

        print("=" * 60)

        print(
            "DimProduct SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: "
            f"{source_count}"
        )

        print(
            f"New products: "
            f"{new_count}"
        )

        print(
            f"Changed products: "
            f"{changed_count}"
        )

        print(
            f"Unchanged products: "
            f"{unchanged_count}"
        )

        print(
            f"Closed versions: "
            f"{close_count}"
        )

        print(
            f"New versions inserted: "
            f"{insert_count}"
        )

        print("=" * 60)


    finally:

        if ch_client is not None:

            ch_client.close()


        spark.stop()

        print(
            "Spark session stopped."
        )


# ============================================================
# Airflow DAG
# ============================================================

with DAG(

    dag_id="etl_dimproduct_clickhouse",

    start_date=datetime(2025, 1, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "northwind",
        "postgres",
        "clickhouse",
        "pyspark",
        "scd2",
        "dimension"
    ],

) as dag:


    load = PythonOperator(

        task_id="load_dimproduct",

        python_callable=load_dimproduct_to_clickhouse
    )