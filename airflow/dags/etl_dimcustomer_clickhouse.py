from datetime import datetime
import unicodedata

from airflow import DAG
from airflow.sdk import Connection
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# PostgreSQL Staging -> ClickHouse DW
# DimCustomer - SCD Type 2 using PySpark
# ============================================================


def load_dimcustomer_to_clickhouse():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        lit,
        sha2,
        concat_ws,
        coalesce,
        trim,
        udf
    )

    from pyspark.sql.types import StringType

    from pyspark import StorageLevel

    import clickhouse_connect


    print("Starting DimCustomer SCD Type 2 ETL...")


    # ========================================================
    # Unicode Normalization Function
    # ========================================================

    def normalize_unicode(value):

        if value is None:
            return None

        normalized_value = unicodedata.normalize(
            "NFC",
            str(value)
        )

        normalized_value = normalized_value.strip()

        if normalized_value == "":
            return None

        return normalized_value


    normalize_unicode_udf = udf(
        normalize_unicode,
        StringType()
    )


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
        .appName("DimCustomer_SCD2")

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
        # 1. Read DimCustomer from PostgreSQL
        # ====================================================

        print(
            "Reading DimCustomer from PostgreSQL staging..."
        )

        source_df = (
            spark.read
            .jdbc(
                url=POSTGRES_URL,
                table='"DimCustomer"',
                properties=POSTGRES_PROPERTIES
            )
        )

        source_count = source_df.count()

        print(
            f"Extracted {source_count} customer records "
            "from PostgreSQL."
        )


        # ====================================================
        # 2. Read Current DimGeography from ClickHouse
        # ====================================================

        print(
            "Reading current DimGeography records "
            "from ClickHouse..."
        )

        geography_df = (
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

        geography_count = geography_df.count()

        print(
            f"Extracted {geography_count} current geography "
            "records from ClickHouse."
        )


        # ====================================================
        # 3. Normalize Geography Join Columns
        # ====================================================

        print(
            "Normalizing geography join columns..."
        )

        customer_geography_df = (
            source_df
            .select(

                "*",

                coalesce(
                    normalize_unicode_udf(
                        col("Country")
                    ),
                    lit("")
                )
                .alias("_Country"),

                coalesce(
                    normalize_unicode_udf(
                        col("Region")
                    ),
                    lit("")
                )
                .alias("_Region"),

                coalesce(
                    normalize_unicode_udf(
                        col("City")
                    ),
                    lit("")
                )
                .alias("_City"),

                coalesce(
                    normalize_unicode_udf(
                        col("PostalCode")
                    ),
                    lit("")
                )
                .alias("_PostalCode"),

                coalesce(
                    normalize_unicode_udf(
                        col("Address")
                    ),
                    lit("")
                )
                .alias("_Address")
            )
        )


        normalized_geography_df = (
            geography_df
            .select(

                col("GeographyKey"),

                coalesce(
                    normalize_unicode_udf(
                        col("Country")
                    ),
                    lit("")
                )
                .alias("_Country"),

                coalesce(
                    normalize_unicode_udf(
                        col("Region")
                    ),
                    lit("")
                )
                .alias("_Region"),

                coalesce(
                    normalize_unicode_udf(
                        col("City")
                    ),
                    lit("")
                )
                .alias("_City"),

                coalesce(
                    normalize_unicode_udf(
                        col("PostalCode")
                    ),
                    lit("")
                )
                .alias("_PostalCode"),

                coalesce(
                    normalize_unicode_udf(
                        col("Address")
                    ),
                    lit("")
                )
                .alias("_Address")
            )
        )


        # ====================================================
        # 4. Join Customer with Geography
        # ====================================================

        print(
            "Joining DimCustomer with current DimGeography..."
        )

        customer_df = (
            customer_geography_df.alias("c")

            .join(

                normalized_geography_df.alias("g"),

                (
                    col("c._Country")
                    ==
                    col("g._Country")
                )

                &

                (
                    col("c._Region")
                    ==
                    col("g._Region")
                )

                &

                (
                    col("c._City")
                    ==
                    col("g._City")
                )

                &

                (
                    col("c._PostalCode")
                    ==
                    col("g._PostalCode")
                )

                &

                (
                    col("c._Address")
                    ==
                    col("g._Address")
                ),

                "left"
            )

            .select(

                col("c.CustomerKey")
                .cast("long")
                .alias("CustomerKey"),

                normalize_unicode_udf(
                    col("c.CustomerID")
                )
                .alias("CustomerAlternateKey"),

                coalesce(
                    col("g.GeographyKey"),
                    lit(0)
                )
                .cast("long")
                .alias("GeographyKey"),

                normalize_unicode_udf(
                    col("c.CompanyName")
                )
                .alias("CompanyName"),

                normalize_unicode_udf(
                    col("c.ContactName")
                )
                .alias("ContactName"),

                normalize_unicode_udf(
                    col("c.ContactTitle")
                )
                .alias("ContactTitle"),

                normalize_unicode_udf(
                    col("c.Phone")
                )
                .alias("Phone"),

                normalize_unicode_udf(
                    col("c.Fax")
                )
                .alias("Fax")
            )
        )


        print(
            "Geography join completed successfully."
        )


        # ====================================================
        # Geography Join Statistics
        # ====================================================

        valid_geography_count = (
            customer_df
            .filter(
                col("GeographyKey") != 0
            )
            .count()
        )

        unknown_geography_count = (
            customer_df
            .filter(
                col("GeographyKey") == 0
            )
            .count()
        )


        print(
            f"Customers with valid geography: "
            f"{valid_geography_count}"
        )

        print(
            f"Customers with unknown geography: "
            f"{unknown_geography_count}"
        )


        # ====================================================
        # 5. Create Deterministic Source HashDiff
        # ====================================================

        print(
            "Calculating normalized source HashDiff..."
        )

        customer_with_hash = (
            customer_df
            .withColumn(
                "HashDiff",
                sha2(
                    concat_ws(
                        "||",

                        coalesce(
                            col("GeographyKey")
                            .cast("string"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("CompanyName"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("ContactName"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("ContactTitle"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("Phone"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("Fax"),
                            lit("<NULL>")
                        )
                    ),
                    256
                )
            )
        )


        # ====================================================
        # 6. Read Current DimCustomer from ClickHouse
        # ====================================================

        print(
            "Reading current DimCustomer records "
            "from ClickHouse..."
        )

        target_df = (
            spark.read
            .jdbc(
                url=CLICKHOUSE_URL,
                table="DimCustomer",
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
        # 7. Normalize Target Values
        # ====================================================

        normalized_target_df = (
            target_df
            .select(

                col("CustomerKey")
                .cast("long")
                .alias("TargetCustomerKey"),

                normalize_unicode_udf(
                    col("CustomerAlternateKey")
                )
                .alias("CustomerAlternateKey"),

                coalesce(
                    col("GeographyKey"),
                    lit(0)
                )
                .cast("long")
                .alias("TargetGeographyKey"),

                normalize_unicode_udf(
                    col("CompanyName")
                )
                .alias("TargetCompanyName"),

                normalize_unicode_udf(
                    col("ContactName")
                )
                .alias("TargetContactName"),

                normalize_unicode_udf(
                    col("ContactTitle")
                )
                .alias("TargetContactTitle"),

                normalize_unicode_udf(
                    col("Phone")
                )
                .alias("TargetPhone"),

                normalize_unicode_udf(
                    col("Fax")
                )
                .alias("TargetFax"),

                col("Version")
                .cast("long")
                .alias("Version")
            )
        )


        # ====================================================
        # 8. Calculate Target HashDiff
        # ====================================================

        print(
            "Calculating normalized target HashDiff..."
        )

        current_target_with_hash = (
            normalized_target_df
            .withColumn(
                "TargetHashDiff",
                sha2(
                    concat_ws(
                        "||",

                        coalesce(
                            col("TargetGeographyKey")
                            .cast("string"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("TargetCompanyName"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("TargetContactName"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("TargetContactTitle"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("TargetPhone"),
                            lit("<NULL>")
                        ),

                        coalesce(
                            col("TargetFax"),
                            lit("<NULL>")
                        )
                    ),
                    256
                )
            )
        )


        # ====================================================
        # 9. Prepare Target Comparison Dataset
        #
        # DEBUG:
        # Keep all target columns so Source and Target can be
        # compared side-by-side.
        # ====================================================

        target_compare_df = (
            current_target_with_hash
            .select(

                col(
                    "CustomerAlternateKey"
                )
                .alias(
                    "TargetCustomerID"
                ),

                col(
                    "TargetCustomerKey"
                ),

                col(
                    "TargetGeographyKey"
                ),

                col(
                    "TargetCompanyName"
                ),

                col(
                    "TargetContactName"
                ),

                col(
                    "TargetContactTitle"
                ),

                col(
                    "TargetPhone"
                ),

                col(
                    "TargetFax"
                ),

                col(
                    "Version"
                )
                .cast("long")
                .alias(
                    "TargetVersion"
                ),

                col(
                    "TargetHashDiff"
                )
            )
        )


        # ====================================================
        # 10. Compare Source and Target
        # ====================================================

        print(
            "Comparing source and target records..."
        )

        comparison_df = (
            customer_with_hash
            .join(

                target_compare_df,

                customer_with_hash[
                    "CustomerAlternateKey"
                ]
                ==
                target_compare_df[
                    "TargetCustomerID"
                ],

                "left"
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )


        # ====================================================
        # 11. Materialize Comparison Results
        # ====================================================

        print(
            "Materializing SCD comparison results..."
        )

        comparison_df.count()


        # ====================================================
        # 12. New Customers
        # ====================================================

        new_customers_df = (
            comparison_df
            .filter(
                col(
                    "TargetCustomerID"
                ).isNull()
            )
        )

        new_count = new_customers_df.count()


        # ====================================================
        # 13. Changed Customers
        # ====================================================

        changed_customers_df = (
            comparison_df
            .filter(
                col(
                    "TargetCustomerID"
                ).isNotNull()
            )
            .filter(
                col("HashDiff")
                !=
                col("TargetHashDiff")
            )
        )

        changed_count = changed_customers_df.count()


        # ====================================================
        # 14. Unchanged Customers
        # ====================================================

        unchanged_count = (
            comparison_df
            .filter(
                col(
                    "TargetCustomerID"
                ).isNotNull()
            )
            .filter(
                col("HashDiff")
                ==
                col("TargetHashDiff")
            )
            .count()
        )


        print("=" * 60)

        print(
            f"New customers: {new_count}"
        )

        print(
            f"Changed customers: {changed_count}"
        )

        print(
            f"Unchanged customers: {unchanged_count}"
        )

        print("=" * 60)


        # ====================================================
        # DEBUG SECTION
        #
        # Show exact Source vs Target differences.
        #
        # This section is temporary and is intended to identify
        # why records are incorrectly detected as changed.
        # ====================================================

        if changed_count > 0:

            print("=" * 120)

            print(
                "DEBUG: SAMPLE OF CHANGED CUSTOMERS"
            )

            print(
                "Showing Source and Target values side-by-side..."
            )

            print("=" * 120)


            (
                changed_customers_df
                .select(

                    # ----------------------------------------
                    # Customer Identity
                    # ----------------------------------------

                    col("CustomerKey")
                    .alias("SRC_CustomerKey"),

                    col("TargetCustomerKey")
                    .alias("TGT_CustomerKey"),

                    col("CustomerAlternateKey")
                    .alias("CustomerID"),


                    # ----------------------------------------
                    # Geography
                    # ----------------------------------------

                    col("GeographyKey")
                    .alias("SRC_GeographyKey"),

                    col("TargetGeographyKey")
                    .alias("TGT_GeographyKey"),


                    # ----------------------------------------
                    # Company
                    # ----------------------------------------

                    col("CompanyName")
                    .alias("SRC_CompanyName"),

                    col("TargetCompanyName")
                    .alias("TGT_CompanyName"),


                    # ----------------------------------------
                    # Contact Name
                    # ----------------------------------------

                    col("ContactName")
                    .alias("SRC_ContactName"),

                    col("TargetContactName")
                    .alias("TGT_ContactName"),


                    # ----------------------------------------
                    # Contact Title
                    # ----------------------------------------

                    col("ContactTitle")
                    .alias("SRC_ContactTitle"),

                    col("TargetContactTitle")
                    .alias("TGT_ContactTitle"),


                    # ----------------------------------------
                    # Phone
                    # ----------------------------------------

                    col("Phone")
                    .alias("SRC_Phone"),

                    col("TargetPhone")
                    .alias("TGT_Phone"),


                    # ----------------------------------------
                    # Fax
                    # ----------------------------------------

                    col("Fax")
                    .alias("SRC_Fax"),

                    col("TargetFax")
                    .alias("TGT_Fax"),


                    # ----------------------------------------
                    # Hashes
                    # ----------------------------------------

                    col("HashDiff")
                    .alias("SRC_HashDiff"),

                    col("TargetHashDiff")
                    .alias("TGT_HashDiff"),

                    col("TargetVersion")
                )
                .orderBy(
                    col("CustomerKey")
                )
                .show(
                    10,
                    truncate=False
                )
            )


            print("=" * 120)

            print(
                "DEBUG: COLUMN-LEVEL DIFFERENCE COUNTS"
            )

            print("=" * 120)


            from pyspark.sql.functions import (
                sum as spark_sum,
                when
            )


            difference_summary = (
                changed_customers_df
                .agg(

                    spark_sum(
                        when(
                            col("GeographyKey")
                            !=
                            col("TargetGeographyKey"),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("GeographyKey_Differences"),

                    spark_sum(
                        when(
                            ~col("CompanyName")
                            .eqNullSafe(
                                col("TargetCompanyName")
                            ),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("CompanyName_Differences"),

                    spark_sum(
                        when(
                            ~col("ContactName")
                            .eqNullSafe(
                                col("TargetContactName")
                            ),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("ContactName_Differences"),

                    spark_sum(
                        when(
                            ~col("ContactTitle")
                            .eqNullSafe(
                                col("TargetContactTitle")
                            ),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("ContactTitle_Differences"),

                    spark_sum(
                        when(
                            ~col("Phone")
                            .eqNullSafe(
                                col("TargetPhone")
                            ),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("Phone_Differences"),

                    spark_sum(
                        when(
                            ~col("Fax")
                            .eqNullSafe(
                                col("TargetFax")
                            ),
                            1
                        )
                        .otherwise(0)
                    )
                    .alias("Fax_Differences")
                )
            )


            difference_summary.show(
                truncate=False
            )


            print("=" * 120)


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
                customer_with_hash
                .select(

                    col("CustomerKey")
                    .cast("long"),

                    col("CustomerAlternateKey"),

                    col("GeographyKey")
                    .cast("long"),

                    col("CompanyName"),

                    col("ContactName"),

                    col("ContactTitle"),

                    col("Phone"),

                    col("Fax"),

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
                "initial customer records..."
            )

            (
                initial_load_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimCustomer",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


            print("=" * 60)

            print(
                "DimCustomer initial load "
                "completed successfully."
            )

            print(
                f"Source records: {source_count}"
            )

            print(
                f"New customers inserted: "
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
                "No new or changed customers detected."
            )

            print(
                "SCD Type 2 load completed successfully."
            )

            return


        # ====================================================
        # Prepare New SCD Type 2 Versions
        # ====================================================

        print(
            "Preparing new SCD Type 2 versions..."
        )


        # ====================================================
        # New Versions for Changed Customers
        # ====================================================

        changed_versions_df = (
            changed_customers_df
            .select(

                col("CustomerKey")
                .cast("long"),

                col("CustomerAlternateKey"),

                col("GeographyKey")
                .cast("long"),

                col("CompanyName"),

                col("ContactName"),

                col("ContactTitle"),

                col("Phone"),

                col("Fax"),

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
        # Initial Versions for New Customers
        # ====================================================

        new_versions_df = (
            new_customers_df
            .select(

                col("CustomerKey")
                .cast("long"),

                col("CustomerAlternateKey"),

                col("GeographyKey")
                .cast("long"),

                col("CompanyName"),

                col("ContactName"),

                col("ContactTitle"),

                col("Phone"),

                col("Fax"),

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
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
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
        # Close Old Versions
        # ====================================================

        changed_ids = [

            row["CustomerAlternateKey"]

            for row in (

                changed_customers_df
                .select(
                    "CustomerAlternateKey"
                )
                .distinct()
                .collect()

            )

        ]

        close_count = len(
            changed_ids
        )


        if changed_ids:

            print(
                "Closing old customer versions..."
            )


            for customer_id in changed_ids:

                ch_client.command(
                    """
                    ALTER TABLE northwind_dw.DimCustomer
                    UPDATE
                        EndDate = %(end_date)s,
                        IsCurrent = 0
                    WHERE CustomerAlternateKey = %(customer_id)s
                      AND IsCurrent = 1
                    SETTINGS mutations_sync = 1
                    """,
                    parameters={
                        "end_date": load_timestamp,
                        "customer_id": customer_id
                    }
                )


            print(
                f"Closed {close_count} "
                "old customer versions."
            )


        # ====================================================
        # Insert New Versions
        # ====================================================

        if insert_count > 0:

            print(
                f"Inserting {insert_count} "
                "new customer versions..."
            )

            (
                records_to_insert_df
                .coalesce(1)
                .write
                .jdbc(
                    url=CLICKHOUSE_URL,
                    table="DimCustomer",
                    mode="append",
                    properties=CLICKHOUSE_PROPERTIES
                )
            )


        # ====================================================
        # Final Logging
        # ====================================================

        print("=" * 60)

        print(
            "DimCustomer SCD Type 2 "
            "load completed successfully."
        )

        print(
            f"Source records: {source_count}"
        )

        print(
            f"New customers: {new_count}"
        )

        print(
            f"Changed customers: {changed_count}"
        )

        print(
            f"Unchanged customers: {unchanged_count}"
        )

        print(
            f"Closed versions: {close_count}"
        )

        print(
            f"New versions inserted: {insert_count}"
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

    dag_id="etl_dimcustomer_clickhouse",

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

        task_id="load_dimcustomer",

        python_callable=load_dimcustomer_to_clickhouse

    )