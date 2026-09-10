from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimSupplier - SCD Type 2 using PySpark
# ============================================================

def load_dimsupplier_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim
    )

    import clickhouse_connect


    print("Starting DimSupplier SCD Type 2 ETL...")


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
        .appName("DimSupplier_SCD2")

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
        # 1. Extract Source Data
        # ====================================================

        print(
            "Reading DimSupplier from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimSupplier"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} supplier records "
            "from PostgreSQL."
        )


        # ====================================================
        # 2. Read Current Target Data
        # ====================================================

        print(
            "Reading current DimSupplier records "
            "from ClickHouse..."
        )

        target_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,
                table="""
                (
                    SELECT
                        SupplierKey,
                        SupplierAlternateKey,
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
                        HomePage,
                        StartDate,
                        EndDate,
                        IsCurrent,
                        Version
                    FROM DimSupplier
                    WHERE IsCurrent = 1
                ) AS current_suppliers
                """,
                properties=CLICKHOUSE_PROPERTIES
            )
        )

        current_count = target_df.count()

        print(
            "Total current records in ClickHouse: "
            f"{current_count}"
        )


        # ====================================================
        # 3. Business Columns
        #
        # These columns determine whether a supplier changed.
        # ====================================================

        business_columns = [
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
        ]


        # ====================================================
        # 4. Deterministic NULL-safe Hash Function
        #
        # Rules:
        # - NULL -> <NULL>
        # - String values -> TRIM
        # - Explicit conversion to string
        # ====================================================

        def create_hash(df):

            return df.withColumn(

                "RowHash",

                sha2(

                    concat_ws(

                        "||",

                        *[
                            coalesce(
                                trim(
                                    col(c)
                                    .cast("string")
                                ),
                                lit("<NULL>")
                            )
                            for c in business_columns
                        ]
                    ),

                    256
                )
            )


        # ====================================================
        # 5. Source Hash
        # ====================================================

        print(
            "Calculating source RowHash..."
        )

        source_with_hash = (
            create_hash(source_df)
        )


        # ====================================================
        # 6. Target Hash
        # ====================================================

        print(
            "Calculating current target RowHash..."
        )

        current_target_with_hash = (
            create_hash(target_df)
        )


        # ====================================================
        # 7. Prepare Target Comparison Dataset
        # ====================================================

        target_compare_df = (

            current_target_with_hash

            .select(

                col(
                    "SupplierAlternateKey"
                )
                .cast("int")
                .alias(
                    "TargetSupplierID"
                ),

                col(
                    "Version"
                )
                .cast("long")
                .alias(
                    "TargetVersion"
                ),

                col(
                    "RowHash"
                )
                .alias(
                    "TargetRowHash"
                )
            )
        )


        # ====================================================
        # 8. Compare Source and Target
        # ====================================================

        comparison_df = (

            source_with_hash.alias("src")

            .join(

                target_compare_df.alias("tgt"),

                col("src.SupplierID")
                .cast("int")
                ==
                col("tgt.TargetSupplierID"),

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
        # 10. Detect New Suppliers
        # ====================================================

        new_suppliers_df = (

            comparison_df

            .filter(
                col(
                    "TargetSupplierID"
                ).isNull()
            )

            .cache()
        )

        new_count = (
            new_suppliers_df.count()
        )


        # ====================================================
        # 11. Detect Changed Suppliers
        # ====================================================

        changed_suppliers_df = (

            comparison_df

            .filter(
                col(
                    "TargetSupplierID"
                ).isNotNull()
            )

            .filter(
                col("RowHash")
                !=
                col("TargetRowHash")
            )

            .cache()
        )

        changed_count = (
            changed_suppliers_df.count()
        )


        # ====================================================
        # 12. Detect Unchanged Suppliers
        # ====================================================

        unchanged_suppliers_df = (

            comparison_df

            .filter(
                col(
                    "TargetSupplierID"
                ).isNotNull()
            )

            .filter(
                col("RowHash")
                ==
                col("TargetRowHash")
            )

            .cache()
        )

        unchanged_count = (
            unchanged_suppliers_df.count()
        )


        print("=" * 60)

        print(
            f"New suppliers: {new_count}"
        )

        print(
            f"Changed suppliers: {changed_count}"
        )

        print(
            f"Unchanged suppliers: "
            f"{unchanged_count}"
        )

        print("=" * 60)


        # ====================================================
        # 13. Initial Load
        # ====================================================

        if current_count == 0:

            print(
                "Initial load detected."
            )

            load_timestamp = datetime.now()


            initial_load_df = (

                source_df

                .select(

                    col("SupplierKey")
                    .cast("long"),

                    col("SupplierID")
                    .cast("long")
                    .alias(
                        "SupplierAlternateKey"
                    ),

                    *[
                        col(c)
                        for c in business_columns
                    ],

                    lit(load_timestamp)
                    .cast("timestamp")
                    .alias("StartDate"),

                    lit(None)
                    .cast("timestamp")
                    .alias("EndDate"),

                    lit(1)
                    .cast("byte")
                    .alias("IsCurrent"),

                    lit(1)
                    .cast("long")
                    .alias("Version")
                )
            )


            print(
                f"Inserting {source_count} "
                "initial supplier records..."
            )


            (
                initial_load_df

                .write

                .jdbc(

                    url=CLICKHOUSE_URL,

                    table="DimSupplier",

                    mode="append",

                    properties=CLICKHOUSE_PROPERTIES
                )
            )


            print("=" * 60)

            print(
                "DimSupplier initial load "
                "completed successfully."
            )

            print(
                f"Source records: {source_count}"
            )

            print(
                f"New suppliers inserted: "
                f"{source_count}"
            )

            print("=" * 60)

            return


        # ====================================================
        # 14. Nothing Changed
        # ====================================================

        if (
            new_count == 0
            and changed_count == 0
        ):

            print(
                "No new or changed suppliers detected."
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
        # Changed Supplier Versions
        # ----------------------------------------------------

        changed_new_versions_df = (

            changed_suppliers_df

            .select(

                col("SupplierKey")
                .cast("long"),

                col("SupplierID")
                .cast("long")
                .alias(
                    "SupplierAlternateKey"
                ),

                *[
                    col(c)
                    for c in business_columns
                ],

                lit(load_timestamp)
                .cast("timestamp")
                .alias("StartDate"),

                lit(None)
                .cast("timestamp")
                .alias("EndDate"),

                lit(1)
                .cast("byte")
                .alias("IsCurrent"),

                (
                    coalesce(

                        col("TargetVersion")
                        .cast("long"),

                        lit(0)
                    )

                    +
                    lit(1)
                )

                .cast("long")
                .alias("Version")
            )
        )


        # ----------------------------------------------------
        # New Supplier Versions
        # ----------------------------------------------------

        new_supplier_versions_df = (

            new_suppliers_df

            .select(

                col("SupplierKey")
                .cast("long"),

                col("SupplierID")
                .cast("long")
                .alias(
                    "SupplierAlternateKey"
                ),

                *[
                    col(c)
                    for c in business_columns
                ],

                lit(load_timestamp)
                .cast("timestamp")
                .alias("StartDate"),

                lit(None)
                .cast("timestamp")
                .alias("EndDate"),

                lit(1)
                .cast("byte")
                .alias("IsCurrent"),

                lit(1)
                .cast("long")
                .alias("Version")
            )
        )


        # ----------------------------------------------------
        # Combine New + Changed
        # ----------------------------------------------------

        records_to_insert_df = (

            new_supplier_versions_df

            .unionByName(
                changed_new_versions_df
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
        # 18. Close Old Versions
        # ====================================================

        close_count = 0


        if changed_count > 0:

            print(
                "Closing old supplier versions..."
            )


            changed_ids = [

                row["SupplierID"]

                for row in (

                    changed_suppliers_df

                    .select("SupplierID")

                    .collect()
                )
            ]


            for supplier_id in changed_ids:

                ch_client.command(

                    """
                    ALTER TABLE northwind_dw.DimSupplier
                    UPDATE
                        EndDate = %(end_date)s,
                        IsCurrent = 0
                    WHERE
                        SupplierAlternateKey = %(supplier_id)s
                        AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """,

                    parameters={

                        "end_date": load_timestamp,

                        "supplier_id": supplier_id
                    }
                )


            close_count = len(changed_ids)


            print(
                f"Closed {close_count} "
                "old supplier versions."
            )


        # ====================================================
        # 19. Insert New Versions
        # ====================================================

        if insert_count > 0:

            print(
                f"Inserting {insert_count} "
                "new supplier versions..."
            )


            (
                records_to_insert_df

                .write

                .jdbc(

                    url=CLICKHOUSE_URL,

                    table="DimSupplier",

                    mode="append",

                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # 20. Final Summary
        # ====================================================

        print("=" * 60)

        print(
            "DimSupplier SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: {source_count}"
        )

        print(
            f"New suppliers: {new_count}"
        )

        print(
            f"Changed suppliers: {changed_count}"
        )

        print(
            f"Unchanged suppliers: "
            f"{unchanged_count}"
        )

        print(
            f"Closed versions: {close_count}"
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

        print("Spark session stopped.")


# ============================================================
# Airflow DAG
# ============================================================

with DAG(

    dag_id="etl_dimsupplier_clickhouse",

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

        task_id="load_dimsupplier",

        python_callable=load_dimsupplier_to_clickhouse
    )