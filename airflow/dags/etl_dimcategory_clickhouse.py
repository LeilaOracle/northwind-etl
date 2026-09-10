from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimCategory - SCD Type 2 using PySpark
# ============================================================

def load_dimcategory_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim,
    )

    import clickhouse_connect


    print("Starting DimCategory SCD Type 2 ETL...")


    # ========================================================
    # JDBC Configuration
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
        .appName("DimCategory_SCD2")

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
        # Extract Source Data from PostgreSQL
        # ====================================================

        print(
            "Reading DimCategory from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimCategory"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} category records "
            "from PostgreSQL."
        )


        # ====================================================
        # Read Current Target Data
        # ====================================================

        print(
            "Reading current DimCategory records "
            "from ClickHouse..."
        )

        target_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,
                table="DimCategory",
                properties=CLICKHOUSE_PROPERTIES
            )
            .filter(
                col("IsCurrent") == 1
            )
        )

        target_count = target_df.count()

        print(
            f"Total current records in ClickHouse: "
            f"{target_count}"
        )


        # ====================================================
        # Business Columns
        #
        # Only these columns determine whether a Category
        # has changed.
        #
        # CategoryKey and CategoryID are excluded.
        # ====================================================

        business_columns = [
            "CategoryName",
            "Description"
        ]


        # ====================================================
        # Deterministic Hash Function
        #
        # Rules:
        # - NULL -> <NULL>
        # - Trim strings
        # - Explicit string conversion
        #
        # This ensures PostgreSQL and ClickHouse values
        # generate identical hashes.
        # ====================================================

        def create_hash(df, hash_column_name):

            normalized_columns = []

            for column_name in business_columns:

                normalized_columns.append(
                    coalesce(
                        trim(
                            col(column_name)
                            .cast("string")
                        ),
                        lit("<NULL>")
                    )
                )

            return df.withColumn(
                hash_column_name,
                sha2(
                    concat_ws(
                        "||",
                        *normalized_columns
                    ),
                    256
                )
            )


        # ====================================================
        # Source Hash
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
        # Target Hash
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
        # Target Comparison Dataset
        # ====================================================

        target_compare_df = (
            target_with_hash
            .select(
                col(
                    "CategoryAlternateKey"
                )
                .cast("int")
                .alias(
                    "TargetCategoryID"
                ),

                col("Version")
                .cast("long")
                .alias(
                    "TargetVersion"
                ),

                col("TargetRowHash")
            )
        )


        # ====================================================
        # Compare Source and Target
        # ====================================================

        comparison_df = (
            source_with_hash
            .join(
                target_compare_df,
                source_with_hash[
                    "CategoryID"
                ].cast("int")
                ==
                target_compare_df[
                    "TargetCategoryID"
                ],
                "left"
            )
            .cache()
        )


        # ====================================================
        # Materialize Comparison
        #
        # Avoid recalculating the full Spark lineage.
        # ====================================================

        print(
            "Materializing SCD comparison results..."
        )

        comparison_df.count()


        # ====================================================
        # New Categories
        # ====================================================

        new_categories_df = (
            comparison_df
            .filter(
                col(
                    "TargetCategoryID"
                ).isNull()
            )
        )

        new_count = (
            new_categories_df.count()
        )


        # ====================================================
        # Changed Categories
        # ====================================================

        changed_categories_df = (
            comparison_df
            .filter(
                col(
                    "TargetCategoryID"
                ).isNotNull()
            )
            .filter(
                col("SourceRowHash")
                !=
                col("TargetRowHash")
            )
        )

        changed_count = (
            changed_categories_df.count()
        )


        # ====================================================
        # Unchanged Categories
        # ====================================================

        unchanged_count = (
            comparison_df
            .filter(
                col(
                    "TargetCategoryID"
                ).isNotNull()
            )
            .filter(
                col("SourceRowHash")
                ==
                col("TargetRowHash")
            )
            .count()
        )


        print("=" * 60)

        print(
            f"New categories: {new_count}"
        )

        print(
            f"Changed categories: {changed_count}"
        )

        print(
            f"Unchanged categories: {unchanged_count}"
        )

        print("=" * 60)


        # ====================================================
        # One Timestamp Per ETL Run
        # ====================================================

        load_timestamp = datetime.now()

        print(
            f"ETL timestamp: {load_timestamp}"
        )


        # ====================================================
        # INITIAL LOAD
        # ====================================================

        if target_count == 0:

            print(
                "Initial load detected."
            )

            initial_load_df = (
                source_df
                .select(
                    col("CategoryKey")
                    .cast("long"),

                    col("CategoryID")
                    .cast("long")
                    .alias(
                        "CategoryAlternateKey"
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
                "initial category records..."
            )


            (
                initial_load_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimCategory",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


            print("=" * 60)

            print(
                "DimCategory initial load "
                "completed successfully."
            )

            print(
                f"Source records: {source_count}"
            )

            print(
                f"New categories inserted: "
                f"{source_count}"
            )

            print("=" * 60)

            return


        # ====================================================
        # NOTHING CHANGED
        # ====================================================

        if (
            new_count == 0
            and changed_count == 0
        ):

            print(
                "No new or changed categories detected."
            )

            print(
                "SCD Type 2 load completed successfully."
            )

            return


        # ====================================================
        # Prepare New SCD Versions
        #
        # Do this BEFORE closing current versions.
        # ====================================================

        print(
            "Preparing new SCD Type 2 versions..."
        )


        # ====================================================
        # Changed Category Versions
        #
        # Version = Current Version + 1
        # ====================================================

        changed_versions_df = (
            changed_categories_df
            .select(
                col("CategoryKey")
                .cast("long"),

                col("CategoryID")
                .cast("long")
                .alias(
                    "CategoryAlternateKey"
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
                    col("TargetVersion")
                    .cast("long")
                    + lit(1)
                )
                .cast("long")
                .alias("Version")
            )
        )


        # ====================================================
        # New Category Versions
        #
        # New categories always start at Version = 1
        # ====================================================

        new_versions_df = (
            new_categories_df
            .select(
                col("CategoryKey")
                .cast("long"),

                col("CategoryID")
                .cast("long")
                .alias(
                    "CategoryAlternateKey"
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


        # ====================================================
        # Combine New + Changed Versions
        # ====================================================

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
        # ClickHouse Native Client
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
        # Get Changed Category IDs
        # ====================================================

        changed_ids = [
            row["CategoryID"]
            for row in (
                changed_categories_df
                .select("CategoryID")
                .collect()
            )
        ]

        close_count = len(changed_ids)


        # ====================================================
        # Close Old Versions
        # ====================================================

        if changed_ids:

            print(
                "Closing old category versions..."
            )

            for category_id in changed_ids:

                ch_client.command(
                    """
                    ALTER TABLE northwind_dw.DimCategory
                    UPDATE
                        EndDate = %(end_date)s,
                        IsCurrent = 0
                    WHERE CategoryAlternateKey =
                        %(category_id)s
                      AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """,
                    parameters={
                        "end_date": load_timestamp,
                        "category_id": category_id
                    }
                )

            print(
                f"Closed {close_count} "
                "old category versions."
            )


        # ====================================================
        # Insert New Versions
        # ====================================================

        if insert_count > 0:

            print(
                f"Inserting {insert_count} "
                "new category versions..."
            )

            (
                records_to_insert_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimCategory",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # Final Logging
        # ====================================================

        print("=" * 60)

        print(
            "DimCategory SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: {source_count}"
        )

        print(
            f"New categories: {new_count}"
        )

        print(
            f"Changed categories: {changed_count}"
        )

        print(
            f"Unchanged categories: {unchanged_count}"
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

    dag_id="etl_dimcategory_clickhouse",

    start_date=datetime(2026, 1, 1),

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
        task_id="load_dimcategory",
        python_callable=load_dimcategory_to_clickhouse
    )