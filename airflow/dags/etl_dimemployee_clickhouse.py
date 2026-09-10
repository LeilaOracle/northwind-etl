from datetime import datetime

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimEmployee - SCD Type 2
# ============================================================


def load_dimemployee_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim
    )

    from pyspark import StorageLevel

    import clickhouse_connect


    print("Starting DimEmployee SCD Type 2 ETL...")


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
        .appName("DimEmployee_SCD2")

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

    new_employees_df = None
    changed_employees_df = None
    records_to_insert_df = None


    try:

        # ====================================================
        # 1. Read DimEmployee from PostgreSQL
        # ====================================================

        print(
            "Reading DimEmployee from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimEmployee"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} employee records "
            f"from PostgreSQL."
        )


        # ====================================================
        # 2. Prepare Source Dataset
        # ====================================================

        source_df = (
            source_df
            .select(

                col("EmployeeKey")
                .cast("long")
                .alias("EmployeeKey"),

                col("EmployeeID")
                .cast("long")
                .alias("EmployeeAlternateKey"),

                col("LastName"),

                col("FirstName"),

                col("Title"),

                col("TitleOfCourtesy"),

                col("BirthDate"),

                col("HireDate"),

                col("Address"),

                col("City"),

                col("Region"),

                col("PostalCode"),

                col("Country"),

                col("HomePhone"),

                col("Extension"),

                col("ReportsTo")
                .cast("long")
                .alias("ReportsTo")
            )
        )


        # ====================================================
        # 3. Read Current DimEmployee Records from ClickHouse
        # ====================================================

        print(
            "Reading current DimEmployee records "
            "from ClickHouse..."
        )

        current_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,

                table="""
                (
                    SELECT
                        EmployeeAlternateKey,
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
                        ReportsTo,
                        Version
                    FROM DimEmployee
                    WHERE IsCurrent = 1
                ) AS current_employees
                """,

                properties=CLICKHOUSE_PROPERTIES
            )
        )

        current_count = current_df.count()

        print(
            "Total current records in ClickHouse: "
            f"{current_count}"
        )


        # ====================================================
        # 4. Calculate Source RowHash
        #
        # IMPORTANT:
        # BirthDate and HireDate are normalized to DATE
        # before converting to STRING.
        #
        # This prevents PostgreSQL TIMESTAMP vs
        # ClickHouse Date32 representation differences.
        # ====================================================

        print(
            "Calculating source RowHash..."
        )

        source_df = (
            source_df
            .withColumn(

                "RowHash",

                sha2(
                    concat_ws(
                        "||",

                        coalesce(
                            col("EmployeeAlternateKey")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("LastName")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("FirstName")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Title")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("TitleOfCourtesy")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            col("BirthDate")
                            .cast("date")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            col("HireDate")
                            .cast("date")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Address")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("City")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Region")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("PostalCode")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Country")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("HomePhone")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Extension")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            col("ReportsTo")
                            .cast("string"),
                            lit("")
                        )
                    ),
                    256
                )
            )
        )


        # ====================================================
        # 5. Calculate Current Target RowHash
        #
        # IMPORTANT:
        # Same normalization must be applied to the target.
        # ====================================================

        print(
            "Calculating current target RowHash..."
        )

        current_df = (
            current_df
            .withColumn(

                "RowHash",

                sha2(
                    concat_ws(
                        "||",

                        coalesce(
                            col("EmployeeAlternateKey")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("LastName")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("FirstName")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Title")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("TitleOfCourtesy")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            col("BirthDate")
                            .cast("date")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            col("HireDate")
                            .cast("date")
                            .cast("string"),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Address")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("City")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Region")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("PostalCode")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Country")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("HomePhone")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            trim(
                                col("Extension")
                                .cast("string")
                            ),
                            lit("")
                        ),

                        coalesce(
                            col("ReportsTo")
                            .cast("string"),
                            lit("")
                        )
                    ),
                    256
                )
            )

            .select(
                "EmployeeAlternateKey",
                "RowHash",
                "Version"
            )
        )


        # ====================================================
        # 6. Detect New / Changed / Unchanged Employees
        # ====================================================

        comparison_df = (
            source_df.alias("src")

            .join(

                current_df.alias("cur"),

                col(
                    "src.EmployeeAlternateKey"
                )
                ==
                col(
                    "cur.EmployeeAlternateKey"
                ),

                "left"
            )

            .select(

                col("src.*"),

                col("cur.RowHash")
                .alias("CurrentRowHash"),

                col("cur.Version")
                .alias("CurrentVersion")
            )
        )


        # ====================================================
        # New Employees
        # ====================================================

        new_employees_df = (
            comparison_df

            .filter(
                col("CurrentRowHash").isNull()
            )

            .persist(StorageLevel.MEMORY_AND_DISK)
        )


        # ====================================================
        # Changed Employees
        # ====================================================

        changed_employees_df = (
            comparison_df

            .filter(
                col("CurrentRowHash").isNotNull()
                &
                (
                    col("RowHash")
                    !=
                    col("CurrentRowHash")
                )
            )

            .persist(StorageLevel.MEMORY_AND_DISK)
        )


        # ====================================================
        # Unchanged Employees
        # ====================================================

        unchanged_employees_df = (
            comparison_df

            .filter(
                col("CurrentRowHash").isNotNull()
                &
                (
                    col("RowHash")
                    ==
                    col("CurrentRowHash")
                )
            )
        )


        # ====================================================
        # Materialize Comparison Results
        # ====================================================

        print(
            "Materializing SCD comparison results..."
        )

        new_count = new_employees_df.count()

        changed_count = changed_employees_df.count()

        unchanged_count = unchanged_employees_df.count()


        print("=" * 60)
        print(f"New employees: {new_count}")
        print(f"Changed employees: {changed_count}")
        print(
            f"Unchanged employees: {unchanged_count}"
        )
        print("=" * 60)


        # ====================================================
        # 7. Prepare New SCD Type 2 Versions
        #
        # IMPORTANT:
        # Prepare and materialize BEFORE closing old versions.
        # ====================================================

        print(
            "Preparing new SCD Type 2 versions..."
        )


        # ----------------------------------------------------
        # Changed Employee Versions
        # ----------------------------------------------------

        changed_new_versions_df = (
            changed_employees_df

            .withColumn(
                "Version",
                col("CurrentVersion")
                .cast("long")
                + lit(1)
            )

            .drop(
                "CurrentRowHash",
                "CurrentVersion"
            )
        )


        # ----------------------------------------------------
        # New Employee Versions
        # ----------------------------------------------------

        new_versions_df = (
            new_employees_df

            .withColumn(
                "Version",
                lit(1)
            )

            .drop(
                "CurrentRowHash",
                "CurrentVersion"
            )
        )


        # ----------------------------------------------------
        # Combine New + Changed
        # ----------------------------------------------------

        records_to_insert_df = (
            new_versions_df

            .unionByName(
                changed_new_versions_df
            )

            .persist(StorageLevel.MEMORY_AND_DISK)
        )


        # Force materialization BEFORE mutations
        records_to_insert_count = (
            records_to_insert_df.count()
        )


        print(
            f"Prepared {records_to_insert_count} "
            f"new SCD Type 2 versions."
        )


        # ====================================================
        # 8. Create Single ETL Timestamp
        # ====================================================

        etl_timestamp = (
            spark.sql(
                "SELECT current_timestamp() AS ts"
            )
            .collect()[0]["ts"]
        )


        print(
            f"ETL timestamp: {etl_timestamp}"
        )


        # ====================================================
        # 9. Connect to ClickHouse
        # ====================================================

        ch_client = clickhouse_connect.get_client(
            host=ch_conn.host,
            port=ch_conn.port or 8123,
            username=ch_conn.login,
            password=ch_conn.password,
            database=ch_conn.schema or "northwind_dw"
        )


        # ====================================================
        # 10. Close Old Versions
        # ====================================================

        if changed_count > 0:

            print(
                "Closing old employee versions..."
            )

            changed_keys = (
                changed_employees_df

                .select(
                    "EmployeeAlternateKey"
                )

                .collect()
            )


            for row in changed_keys:

                employee_id = (
                    row["EmployeeAlternateKey"]
                )

                ch_client.command(
                    f"""
                    ALTER TABLE DimEmployee
                    UPDATE
                        IsCurrent = 0,
                        EndDate = toDateTime(
                            '{etl_timestamp}'
                        )
                    WHERE
                        EmployeeAlternateKey =
                            {employee_id}
                        AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """
                )


            print(
                f"Closed {changed_count} "
                f"old employee versions."
            )


        # ====================================================
        # 11. Insert New Versions
        # ====================================================

        if records_to_insert_count > 0:

            print(
                f"Inserting {records_to_insert_count} "
                f"new employee versions..."
            )


            insert_df = (
                records_to_insert_df

                .select(

                    col("EmployeeKey")
                    .cast("long"),

                    col("EmployeeAlternateKey")
                    .cast("long"),

                    col("LastName"),

                    col("FirstName"),

                    col("Title"),

                    col("TitleOfCourtesy"),

                    col("BirthDate")
                    .cast("date"),

                    col("HireDate")
                    .cast("date"),

                    col("Address"),

                    col("City"),

                    col("Region"),

                    col("PostalCode"),

                    col("Country"),

                    col("HomePhone"),

                    col("Extension"),

                    col("ReportsTo")
                    .cast("long"),

                    lit(etl_timestamp)
                    .cast("timestamp")
                    .alias("StartDate"),

                    lit(None)
                    .cast("timestamp")
                    .alias("EndDate"),

                    lit(1)
                    .cast("int")
                    .alias("IsCurrent"),

                    col("Version")
                    .cast("long")
                )
            )


            (
                insert_df

                .write

                .jdbc(

                    url=CLICKHOUSE_URL,

                    table="DimEmployee",

                    mode="append",

                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # Final Summary
        # ====================================================

        print("=" * 60)

        print(
            "DimEmployee SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: {source_count}"
        )

        print(
            f"New employees: {new_count}"
        )

        print(
            f"Changed employees: {changed_count}"
        )

        print(
            f"Unchanged employees: {unchanged_count}"
        )

        print(
            f"Closed versions: {changed_count}"
        )

        print(
            f"New versions inserted: "
            f"{records_to_insert_count}"
        )

        print("=" * 60)


    finally:

        # ====================================================
        # Cleanup Cached DataFrames
        # ====================================================

        if new_employees_df is not None:
            new_employees_df.unpersist()

        if changed_employees_df is not None:
            changed_employees_df.unpersist()

        if records_to_insert_df is not None:
            records_to_insert_df.unpersist()


        # ====================================================
        # Close ClickHouse Connection
        # ====================================================

        if ch_client is not None:
            ch_client.close()


        # ====================================================
        # Stop Spark
        # ====================================================

        spark.stop()

        print("Spark session stopped.")


# ============================================================
# Airflow DAG
# ============================================================

with DAG(

    dag_id="etl_dimemployee_clickhouse",

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
        task_id="load_dimemployee",
        python_callable=load_dimemployee_to_clickhouse
    )