from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimGeography - SCD Type 2 using PySpark
# ============================================================

def load_dimgeography_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim,
    )
    from pyspark import StorageLevel

    import clickhouse_connect


    print("Starting DimGeography SCD Type 2 ETL...")


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
        .appName("DimGeography_SCD2")

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
    comparison_df = None
    records_to_insert_df = None


    try:

        # ====================================================
        # 1. Read Source Data from PostgreSQL
        # ====================================================

        print(
            "Reading DimGeography from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimGeography"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} geography records "
            "from PostgreSQL."
        )


        # ====================================================
        # 2. Create Stable GeographyAlternateKey
        #
        # GeographyKey is the stable identifier from staging.
        #
        # IMPORTANT:
        # Business attributes such as Address must NOT be used
        # to identify the entity because they can change.
        # ====================================================

        print(
            "Creating stable GeographyAlternateKey..."
        )

        source_with_key_df = (
            source_df
            .withColumn(
                "GeographyAlternateKey",
                sha2(
                    col("GeographyKey")
                    .cast("string"),
                    256
                )
            )
        )


        # ====================================================
        # 3. Business Columns
        #
        # Changes in these columns create a new SCD Type 2
        # version.
        # ====================================================

        business_columns = [
            "Country",
            "Region",
            "City",
            "PostalCode",
            "Address"
        ]


        # ====================================================
        # 4. Create Deterministic HashDiff
        #
        # Rules:
        # - NULL -> <NULL>
        # - Trim strings
        # - Explicit string conversion
        #
        # HashDiff is ONLY used for change detection.
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

            return (
                df
                .withColumn(
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
        # 5. Calculate Source HashDiff
        # ====================================================

        print(
            "Calculating source HashDiff..."
        )

        source_with_hash = (
            create_hash(
                source_with_key_df,
                "SourceHashDiff"
            )
        )


        # ====================================================
        # 6. Read Current Target Data
        # ====================================================

        print(
            "Reading current DimGeography records "
            "from ClickHouse..."
        )

        target_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,
                table="DimGeography",
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
        # 7. Calculate Target HashDiff
        # ====================================================

        print(
            "Calculating target HashDiff..."
        )

        target_with_hash = (
            create_hash(
                target_df,
                "TargetHashDiff"
            )
        )


        # ====================================================
        # 8. Target Comparison Dataset
        # ====================================================

        target_compare_df = (
            target_with_hash
            .select(

                col("GeographyAlternateKey")
                .alias(
                    "TargetGeographyAlternateKey"
                ),

                col("Version")
                .cast("long")
                .alias(
                    "TargetVersion"
                ),

                col("TargetHashDiff")
            )
        )


        # ====================================================
        # 9. Compare Source and Target
        #
        # Join by STABLE Alternate Key
        # ====================================================

        print(
            "Comparing source and target geography records..."
        )

        comparison_df = (
            source_with_hash
            .join(

                target_compare_df,

                source_with_hash[
                    "GeographyAlternateKey"
                ]
                ==
                target_compare_df[
                    "TargetGeographyAlternateKey"
                ],

                "left"
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )


        # ====================================================
        # 10. Materialize Comparison
        # ====================================================

        print(
            "Materializing comparison results..."
        )

        comparison_df.count()


        # ====================================================
        # 11. New Geographies
        # ====================================================

        new_geographies_df = (
            comparison_df
            .filter(
                col(
                    "TargetGeographyAlternateKey"
                ).isNull()
            )
        )

        new_count = new_geographies_df.count()


        # ====================================================
        # 12. Changed Geographies
        # ====================================================

        changed_geographies_df = (
            comparison_df
            .filter(
                col(
                    "TargetGeographyAlternateKey"
                ).isNotNull()
            )
            .filter(
                col("SourceHashDiff")
                !=
                col("TargetHashDiff")
            )
        )

        changed_count = changed_geographies_df.count()


        # ====================================================
        # 13. Unchanged Geographies
        # ====================================================

        unchanged_count = (
            comparison_df
            .filter(
                col(
                    "TargetGeographyAlternateKey"
                ).isNotNull()
            )
            .filter(
                col("SourceHashDiff")
                ==
                col("TargetHashDiff")
            )
            .count()
        )


        print("=" * 60)

        print(
            f"New geographies: {new_count}"
        )

        print(
            f"Changed geographies: {changed_count}"
        )

        print(
            f"Unchanged geographies: {unchanged_count}"
        )

        print("=" * 60)


        # ====================================================
        # 14. One Timestamp Per ETL Run
        # ====================================================

        load_timestamp = datetime.now()

        print(
            f"ETL timestamp: {load_timestamp}"
        )


        # ====================================================
        # 15. INITIAL LOAD
        # ====================================================

        if target_count == 0:

            print(
                "Initial load detected."
            )

            initial_load_df = (
                source_with_key_df
                .select(

                    col("GeographyKey")
                    .cast("long"),

                    col("GeographyAlternateKey"),

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
                "initial geography records..."
            )

            (
                initial_load_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimGeography",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


            print("=" * 60)

            print(
                "DimGeography initial load "
                "completed successfully."
            )

            print(
                f"Source records: {source_count}"
            )

            print(
                f"New geographies inserted: "
                f"{source_count}"
            )

            print("=" * 60)

            return


        # ====================================================
        # 16. NOTHING CHANGED
        # ====================================================

        if (
            new_count == 0
            and changed_count == 0
        ):

            print(
                "No new or changed geographies detected."
            )

            print(
                "DimGeography load completed successfully."
            )

            return


        # ====================================================
        # 17. Prepare New SCD Type 2 Versions
        # ====================================================

        print(
            "Preparing new SCD Type 2 versions..."
        )


        # ====================================================
        # Changed Geography Versions
        #
        # Version = Current Version + 1
        # ====================================================

        changed_versions_df = (
            changed_geographies_df
            .select(

                col("GeographyKey")
                .cast("long"),

                col("GeographyAlternateKey"),

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
        # New Geography Versions
        #
        # New geographies always start at Version = 1
        # ====================================================

        new_versions_df = (
            new_geographies_df
            .select(

                col("GeographyKey")
                .cast("long"),

                col("GeographyAlternateKey"),

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
        # 18. Combine New + Changed Versions
        # ====================================================

        records_to_insert_df = (
            changed_versions_df
            .unionByName(
                new_versions_df
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )


        insert_count = (
            records_to_insert_df.count()
        )


        print(
            f"Prepared {insert_count} "
            "new geography versions."
        )


        # ====================================================
        # 19. ClickHouse Native Client
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
        # 20. Get Changed Alternate Keys
        # ====================================================

        changed_alternate_keys = [

            row["GeographyAlternateKey"]

            for row in (

                changed_geographies_df
                .select(
                    "GeographyAlternateKey"
                )
                .distinct()
                .collect()

            )
        ]


        close_count = len(
            changed_alternate_keys
        )


        # ====================================================
        # 21. Close Old Versions
        # ====================================================

        if changed_alternate_keys:

            print(
                "Closing old geography versions..."
            )

            for alternate_key in changed_alternate_keys:

                ch_client.command(
                    """
                    ALTER TABLE northwind_dw.DimGeography
                    UPDATE
                        EndDate = %(end_date)s,
                        IsCurrent = 0
                    WHERE GeographyAlternateKey =
                        %(alternate_key)s
                      AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """,
                    parameters={
                        "end_date": load_timestamp,
                        "alternate_key": alternate_key
                    }
                )


            print(
                f"Closed {close_count} "
                "old geography versions."
            )


        # ====================================================
        # 22. Insert New Versions
        # ====================================================

        if insert_count > 0:

            print(
                f"Inserting {insert_count} "
                "new geography versions..."
            )

            (
                records_to_insert_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimGeography",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # 23. Final Logging
        # ====================================================

        print("=" * 60)

        print(
            "DimGeography SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: {source_count}"
        )

        print(
            f"New geographies: {new_count}"
        )

        print(
            f"Changed geographies: {changed_count}"
        )

        print(
            f"Unchanged geographies: {unchanged_count}"
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

        if comparison_df is not None:

            comparison_df.unpersist()


        if records_to_insert_df is not None:

            records_to_insert_df.unpersist()


        if ch_client is not None:

            ch_client.close()


        spark.stop()

        print("Spark session stopped.")


# ============================================================
# Airflow DAG
# ============================================================

with DAG(

    dag_id="etl_dimgeography_clickhouse",

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
        task_id="load_dimgeography",
        python_callable=load_dimgeography_to_clickhouse
    )