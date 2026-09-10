from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import clickhouse_connect

from airflow.sdk import Connection


# ============================================================
# CLICKHOUSE CONNECTION
# ============================================================

def get_clickhouse_client():

    conn = Connection.get(
        conn_id="clickhouse_northwind"
    )

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )

    return client


# ============================================================
# GET LAST PROCESSED CHECKPOINT
# ============================================================

def get_last_processed_checkpoint(client, table_name):

    result = client.query(
        """
        SELECT
            LastProcessedLSN,
            LastProcessedSeqVal
        FROM CDC_Clean_State FINAL
        WHERE TableName = %(table_name)s
        ORDER BY LastProcessedAt DESC
        LIMIT 1
        """,
        parameters={
            "table_name": table_name
        }
    )

    if not result.result_rows:

        print(
            f"[{table_name}] First run detected. "
            f"No previous CDC checkpoint found."
        )

        return None, None

    last_lsn = result.result_rows[0][0]
    last_seqval = result.result_rows[0][1]

    print(
        f"[{table_name}] Last Processed Checkpoint:"
    )

    print(
        f"    StartLSN : {last_lsn}"
    )

    print(
        f"    SeqVal   : {last_seqval}"
    )

    return last_lsn, last_seqval


# ============================================================
# UPDATE CDC CLEAN STATE
# ============================================================

def update_clean_state(
    client,
    table_name,
    last_processed_lsn,
    last_processed_seqval
):

    now = datetime.now()

    print(
        f"[{table_name}] Updating CDC Clean State..."
    )

    # --------------------------------------------------------
    # IMPORTANT
    #
    # CDC_Clean_State uses:
    #
    # ReplacingMergeTree(LastProcessedAt)
    # ORDER BY TableName
    #
    # Therefore LastProcessedAt is the ReplacingMergeTree
    # version column and must NOT be modified with
    # ALTER TABLE ... UPDATE.
    #
    # We always INSERT a new checkpoint version.
    # FINAL will return the latest version for each TableName.
    # --------------------------------------------------------

    client.insert(
        "CDC_Clean_State",
        [
            (
                table_name,
                last_processed_lsn,
                last_processed_seqval,
                now
            )
        ],
        column_names=[
            "TableName",
            "LastProcessedLSN",
            "LastProcessedSeqVal",
            "LastProcessedAt"
        ]
    )

    print(
        f"[{table_name}] CDC Clean State updated successfully."
    )

    print(
        f"    StartLSN : {last_processed_lsn}"
    )

    print(
        f"    SeqVal   : {last_processed_seqval}"
    )

    print(
        f"    At       : {now}"
    )


# ============================================================
# GET MAX CHECKPOINT
# ============================================================

def get_max_checkpoint(rows):

    """
    CDC ordering checkpoint:

    StartLSN
        ->
    SeqVal

    Operation is intentionally excluded because
    Operation 3 and Operation 4 belong to the same
    SQL Server UPDATE event.
    """

    max_row = max(
        rows,
        key=lambda row: (
            row[0],   # StartLSN
            row[1]    # SeqVal
        )
    )

    return max_row[0], max_row[1]


# ============================================================
# CLEAN CDC ORDERS
# ============================================================

def clean_orders():

    client = get_clickhouse_client()

    try:

        table_name = "Orders"

        last_lsn, last_seqval = get_last_processed_checkpoint(
            client,
            table_name
        )

        # ----------------------------------------------------
        # FIRST RUN
        # ----------------------------------------------------

        if last_lsn is None:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    OrderID,
                    CustomerID,
                    EmployeeID,
                    OrderDate,
                    RequiredDate,
                    ShippedDate,
                    ShipVia,
                    Freight,
                    ShipName,
                    ShipAddress,
                    ShipCity,
                    ShipRegion,
                    ShipPostalCode,
                    ShipCountry,
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_Orders_Raw
                    WHERE Operation != 3
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {}

        # ----------------------------------------------------
        # INCREMENTAL RUN
        # ----------------------------------------------------

        else:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    OrderID,
                    CustomerID,
                    EmployeeID,
                    OrderDate,
                    RequiredDate,
                    ShippedDate,
                    ShipVia,
                    Freight,
                    ShipName,
                    ShipAddress,
                    ShipCity,
                    ShipRegion,
                    ShipPostalCode,
                    ShipCountry,
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_Orders_Raw
                    WHERE
                        Operation != 3
                        AND
                        (
                            StartLSN > %(last_lsn)s
                            OR
                            (
                                StartLSN = %(last_lsn)s
                                AND SeqVal > %(last_seqval)s
                            )
                        )
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {
                "last_lsn": last_lsn,
                "last_seqval": last_seqval
            }

        # ----------------------------------------------------
        # EXECUTE QUERY
        # ----------------------------------------------------

        result = client.query(
            query,
            parameters=parameters
        )

        rows = result.result_rows

        print(
            f"[Orders] Clean CDC records found: "
            f"{len(rows)}"
        )

        if not rows:

            print(
                "[Orders] No new CDC records to process."
            )

            return

        # ----------------------------------------------------
        # INSERT CLEAN DATA
        # ----------------------------------------------------

        client.insert(
            "CDC_Orders_Clean",
            rows,
            column_names=[
                "StartLSN",
                "SeqVal",
                "Operation",
                "OrderID",
                "CustomerID",
                "EmployeeID",
                "OrderDate",
                "RequiredDate",
                "ShippedDate",
                "ShipVia",
                "Freight",
                "ShipName",
                "ShipAddress",
                "ShipCity",
                "ShipRegion",
                "ShipPostalCode",
                "ShipCountry",
                "CDCProcessedAt"
            ]
        )

        # ----------------------------------------------------
        # UPDATE CHECKPOINT
        # ----------------------------------------------------

        max_lsn, max_seqval = get_max_checkpoint(rows)

        update_clean_state(
            client,
            table_name,
            max_lsn,
            max_seqval
        )

        print(
            f"[Orders] Successfully cleaned "
            f"{len(rows)} CDC records."
        )

    finally:

        client.close()


# ============================================================
# CLEAN CDC ORDER DETAILS
# ============================================================

def clean_order_details():

    client = get_clickhouse_client()

    try:

        table_name = "Order Details"

        last_lsn, last_seqval = get_last_processed_checkpoint(
            client,
            table_name
        )

        # ----------------------------------------------------
        # FIRST RUN
        # ----------------------------------------------------

        if last_lsn is None:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    OrderID,
                    ProductID,
                    UnitPrice,
                    Quantity,
                    Discount,
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_OrderDetails_Raw
                    WHERE Operation != 3
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {}

        # ----------------------------------------------------
        # INCREMENTAL RUN
        # ----------------------------------------------------

        else:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    OrderID,
                    ProductID,
                    UnitPrice,
                    Quantity,
                    Discount,
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_OrderDetails_Raw
                    WHERE
                        Operation != 3
                        AND
                        (
                            StartLSN > %(last_lsn)s
                            OR
                            (
                                StartLSN = %(last_lsn)s
                                AND SeqVal > %(last_seqval)s
                            )
                        )
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {
                "last_lsn": last_lsn,
                "last_seqval": last_seqval
            }

        # ----------------------------------------------------
        # EXECUTE QUERY
        # ----------------------------------------------------

        result = client.query(
            query,
            parameters=parameters
        )

        rows = result.result_rows

        print(
            f"[Order Details] Clean CDC records found: "
            f"{len(rows)}"
        )

        if not rows:

            print(
                "[Order Details] No new CDC records to process."
            )

            return

        # ----------------------------------------------------
        # INSERT CLEAN DATA
        # ----------------------------------------------------

        client.insert(
            "CDC_OrderDetails_Clean",
            rows,
            column_names=[
                "StartLSN",
                "SeqVal",
                "Operation",
                "OrderID",
                "ProductID",
                "UnitPrice",
                "Quantity",
                "Discount",
                "CDCProcessedAt"
            ]
        )

        # ----------------------------------------------------
        # UPDATE CHECKPOINT
        # ----------------------------------------------------

        max_lsn, max_seqval = get_max_checkpoint(rows)

        update_clean_state(
            client,
            table_name,
            max_lsn,
            max_seqval
        )

        print(
            f"[Order Details] Successfully cleaned "
            f"{len(rows)} CDC records."
        )

    finally:

        client.close()


# ============================================================
# CLEAN CDC CUSTOMERS
# ============================================================

def clean_customers():

    client = get_clickhouse_client()

    try:

        table_name = "Customers"

        last_lsn, last_seqval = get_last_processed_checkpoint(
            client,
            table_name
        )

        # ----------------------------------------------------
        # FIRST RUN
        # ----------------------------------------------------

        if last_lsn is None:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    CustomerID,
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
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_Customers_Raw
                    WHERE Operation != 3
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {}

        # ----------------------------------------------------
        # INCREMENTAL RUN
        # ----------------------------------------------------

        else:

            query = """
                SELECT
                    StartLSN,
                    SeqVal,
                    Operation,
                    CustomerID,
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
                    CDCProcessedAt
                FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER
                        (
                            PARTITION BY
                                StartLSN,
                                SeqVal,
                                Operation
                            ORDER BY CDCProcessedAt DESC
                        ) AS rn
                    FROM CDC_Customers_Raw
                    WHERE
                        Operation != 3
                        AND
                        (
                            StartLSN > %(last_lsn)s
                            OR
                            (
                                StartLSN = %(last_lsn)s
                                AND SeqVal > %(last_seqval)s
                            )
                        )
                )
                WHERE rn = 1
                ORDER BY
                    StartLSN,
                    SeqVal,
                    Operation
            """

            parameters = {
                "last_lsn": last_lsn,
                "last_seqval": last_seqval
            }

        # ----------------------------------------------------
        # EXECUTE QUERY
        # ----------------------------------------------------

        result = client.query(
            query,
            parameters=parameters
        )

        rows = result.result_rows

        print(
            f"[Customers] Clean CDC records found: "
            f"{len(rows)}"
        )

        if not rows:

            print(
                "[Customers] No new CDC records to process."
            )

            return

        # ----------------------------------------------------
        # INSERT CLEAN DATA
        # ----------------------------------------------------

        client.insert(
            "CDC_Customers_Clean",
            rows,
            column_names=[
                "StartLSN",
                "SeqVal",
                "Operation",
                "CustomerID",
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
                "CDCProcessedAt"
            ]
        )

        # ----------------------------------------------------
        # UPDATE CHECKPOINT
        # ----------------------------------------------------

        max_lsn, max_seqval = get_max_checkpoint(rows)

        update_clean_state(
            client,
            table_name,
            max_lsn,
            max_seqval
        )

        print(
            f"[Customers] Successfully cleaned "
            f"{len(rows)} CDC records."
        )

    finally:

        client.close()


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="etl_cdc_clean",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "northwind",
        "cdc",
        "clean",
        "clickhouse"
    ]

) as dag:

    clean_orders_task = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders
    )

    clean_order_details_task = PythonOperator(
        task_id="clean_order_details",
        python_callable=clean_order_details
    )

    clean_customers_task = PythonOperator(
        task_id="clean_customers",
        python_callable=clean_customers
    )

    # Three independent CDC streams can run in parallel
    [
        clean_orders_task,
        clean_order_details_task,
        clean_customers_task
    ]