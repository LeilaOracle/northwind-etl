from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

import pyodbc
import clickhouse_connect

from airflow.sdk import Connection


# ============================================================
# SQL SERVER CONNECTION
# ============================================================

def get_sqlserver_connection(database):

    conn = Connection.get(conn_id="northwind_sqlserver")

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={conn.host},{conn.port or 1433};"
        f"DATABASE={database};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )


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
# TEST CLICKHOUSE CONNECTION
# ============================================================

def test_clickhouse_connection():

    client = get_clickhouse_client()

    try:

        result = client.query("""
            SELECT
                version(),
                currentDatabase(),
                currentUser()
        """)

        print("=" * 70)
        print("CLICKHOUSE CONNECTION TEST")
        print("=" * 70)

        for row in result.result_rows:
            print(row)

    finally:

        client.close()


# ============================================================
# GET CURRENT MAX LSN
# ============================================================

def get_current_max_lsn():

    conn = get_sqlserver_connection("Northwind")
    cursor = conn.cursor()

    try:

        cursor.execute("""
            SELECT sys.fn_cdc_get_max_lsn()
        """)

        max_lsn = cursor.fetchone()[0]

        return max_lsn

    finally:

        cursor.close()
        conn.close()


# ============================================================
# GET MIN LSN FOR CAPTURE INSTANCE
# ============================================================

def get_min_lsn(capture_instance):

    conn = get_sqlserver_connection("Northwind")
    cursor = conn.cursor()

    try:

        cursor.execute(
            "SELECT sys.fn_cdc_get_min_lsn(?)",
            capture_instance
        )

        min_lsn = cursor.fetchone()[0]

        return min_lsn

    finally:

        cursor.close()
        conn.close()


# ============================================================
# GET GLOBAL CDC WATERMARK
# ============================================================

def get_global_cdc_watermark(**context):

    max_lsn = get_current_max_lsn()

    max_lsn_hex = max_lsn.hex()

    print("=" * 70)
    print("GLOBAL CDC WATERMARK")
    print("=" * 70)
    print(f"Global Max LSN : {max_lsn_hex}")
    print("=" * 70)

    context["ti"].xcom_push(
        key="global_max_lsn",
        value=max_lsn_hex
    )


# ============================================================
# READ CDC STATE
# ============================================================

def get_cdc_state(table_name):

    conn = get_sqlserver_connection("ETL_Settings")
    cursor = conn.cursor()

    try:

        cursor.execute("""
            SELECT
                CaptureInstance,
                LastProcessedLSN
            FROM dbo.cdc_state
            WHERE TableName = ?
        """, table_name)

        row = cursor.fetchone()

        if row is None:

            raise Exception(
                f"CDC State not found for table: {table_name}"
            )

        return row[0], row[1]

    finally:

        cursor.close()
        conn.close()


# ============================================================
# DETERMINE CDC WINDOW
# ============================================================

def determine_cdc_window(table_name, global_max_lsn_hex):

    capture_instance, stored_lsn = get_cdc_state(
        table_name
    )

    min_lsn = get_min_lsn(
        capture_instance
    )

    to_lsn = bytes.fromhex(
        global_max_lsn_hex
    )

    recovery_mode = False

    print("=" * 70)
    print(f"CDC WINDOW ANALYSIS - {table_name}")
    print("=" * 70)
    print(f"Capture Instance : {capture_instance}")
    print(f"Current Min LSN  : {min_lsn.hex()}")
    print(f"Current Max LSN  : {to_lsn.hex()}")

    if stored_lsn is not None:

        print(
            f"Stored LSN       : {stored_lsn.hex()}"
        )

    else:

        print(
            "Stored LSN       : None"
        )

    # --------------------------------------------------------
    # FIRST RUN
    # --------------------------------------------------------

    if stored_lsn is None:

        from_lsn = min_lsn

        print(
            f"[{table_name}] First run detected."
        )

    # --------------------------------------------------------
    # CDC STATE EXPIRED
    # --------------------------------------------------------

    elif stored_lsn < min_lsn:

        from_lsn = min_lsn

        recovery_mode = True

        print(
            f"WARNING: [{table_name}] CDC STATE EXPIRED!"
        )

        print(
            "Stored LSN is older than "
            "the current CDC minimum LSN."
        )

        print(
            "Recovery mode activated."
        )

    # --------------------------------------------------------
    # NORMAL INCREMENTAL MODE
    # --------------------------------------------------------

    else:

        from_lsn = stored_lsn

        print(
            f"[{table_name}] Continuing incrementally."
        )

    print("-" * 70)
    print(f"From LSN         : {from_lsn.hex()}")
    print(f"To LSN           : {to_lsn.hex()}")
    print(f"Recovery Mode    : {recovery_mode}")
    print("=" * 70)

    return {
        "table_name": table_name,
        "capture_instance": capture_instance,
        "from_lsn": from_lsn,
        "to_lsn": to_lsn,
        "recovery_mode": recovery_mode
    }


# ============================================================
# EXTRACT + LOAD ORDERS CDC
# ============================================================

def extract_orders_cdc(**context):

    ti = context["ti"]

    global_max_lsn_hex = ti.xcom_pull(
        task_ids="get_global_cdc_watermark",
        key="global_max_lsn"
    )

    if not global_max_lsn_hex:

        raise Exception(
            "Global CDC Watermark was not found."
        )

    window = determine_cdc_window(
        "Orders",
        global_max_lsn_hex
    )

    from_lsn = window["from_lsn"]
    to_lsn = window["to_lsn"]

    sql_conn = get_sqlserver_connection("Northwind")
    cursor = sql_conn.cursor()

    try:

        cursor.execute("""
            SELECT
                __$start_lsn,
                __$seqval,
                __$operation,

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
                ShipCountry

            FROM cdc.fn_cdc_get_all_changes_dbo_Orders(
                ?,
                ?,
                N'all update old'
            )

            ORDER BY
                __$start_lsn,
                __$seqval,
                __$operation

        """, (from_lsn, to_lsn))

        rows = cursor.fetchall()

        print(
            f"[Orders] CDC records found: {len(rows)}"
        )

        # ----------------------------------------------------
        # LOAD TO CLICKHOUSE
        # ----------------------------------------------------

        if rows:

            clickhouse_client = get_clickhouse_client()

            try:

                clickhouse_rows = []

                for row in rows:

                    clickhouse_rows.append(
                        (
                            row[0].hex(),
                            row[1].hex(),
                            int(row[2]),

                            int(row[3]),

                            row[4],

                            int(row[5])
                            if row[5] is not None
                            else None,

                            row[6],
                            row[7],
                            row[8],

                            int(row[9])
                            if row[9] is not None
                            else None,

                            row[10],
                            row[11],
                            row[12],
                            row[13],
                            row[14],
                            row[15],
                            row[16],

                            datetime.now()
                        )
                    )

                clickhouse_client.insert(
                    "CDC_Orders_Raw",
                    clickhouse_rows,
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

                print(
                    "Orders loaded to ClickHouse: "
                    f"{len(clickhouse_rows)}"
                )

            finally:

                clickhouse_client.close()

        # ----------------------------------------------------
        # PUSH WATERMARK
        # ALWAYS PUSH - EVEN IF ZERO ROWS
        # ----------------------------------------------------

        ti.xcom_push(
            key="orders_to_lsn",
            value=to_lsn.hex()
        )

        ti.xcom_push(
            key="orders_record_count",
            value=len(rows)
        )

    finally:

        cursor.close()
        sql_conn.close()


# ============================================================
# EXTRACT ORDER DETAILS CDC
# ============================================================

def extract_order_details_cdc(**context):

    ti = context["ti"]

    global_max_lsn_hex = ti.xcom_pull(
        task_ids="get_global_cdc_watermark",
        key="global_max_lsn"
    )

    if not global_max_lsn_hex:

        raise Exception(
            "Global CDC Watermark was not found."
        )

    window = determine_cdc_window(
        "Order Details",
        global_max_lsn_hex
    )

    from_lsn = window["from_lsn"]
    to_lsn = window["to_lsn"]

    conn = get_sqlserver_connection("Northwind")
    cursor = conn.cursor()

    try:

        cursor.execute("""
            SELECT
                __$start_lsn,
                __$seqval,
                __$operation,

                OrderID,
                ProductID,
                UnitPrice,
                Quantity,
                Discount

            FROM cdc.[fn_cdc_get_all_changes_dbo_Order Details](
                ?,
                ?,
                N'all update old'
            )

            ORDER BY
                __$start_lsn,
                __$seqval,
                __$operation

        """, (from_lsn, to_lsn))

        rows = cursor.fetchall()

        print(
            "[Order Details] CDC records found: "
            f"{len(rows)}"
        )

        # ----------------------------------------------------
        # LOAD TO CLICKHOUSE
        # ----------------------------------------------------

        if rows:

            clickhouse_client = get_clickhouse_client()

            try:

                clickhouse_rows = []

                for row in rows:

                    clickhouse_rows.append(
                        (
                            row[0].hex(),
                            row[1].hex(),
                            int(row[2]),

                            int(row[3]),
                            int(row[4]),

                            row[5],
                            int(row[6]),
                            row[7],

                            datetime.now()
                        )
                    )

                clickhouse_client.insert(
                    "CDC_OrderDetails_Raw",
                    clickhouse_rows,
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

                print(
                    "Order Details loaded to ClickHouse: "
                    f"{len(clickhouse_rows)}"
                )

            finally:

                clickhouse_client.close()

        # ----------------------------------------------------
        # PUSH WATERMARK
        # ALWAYS PUSH - EVEN IF ZERO ROWS
        # ----------------------------------------------------

        ti.xcom_push(
            key="order_details_to_lsn",
            value=to_lsn.hex()
        )

        ti.xcom_push(
            key="order_details_record_count",
            value=len(rows)
        )

    finally:

        cursor.close()
        conn.close()


# ============================================================
# EXTRACT CUSTOMERS CDC
# ============================================================

def extract_customers_cdc(**context):

    ti = context["ti"]

    global_max_lsn_hex = ti.xcom_pull(
        task_ids="get_global_cdc_watermark",
        key="global_max_lsn"
    )

    if not global_max_lsn_hex:

        raise Exception(
            "Global CDC Watermark was not found."
        )

    window = determine_cdc_window(
        "Customers",
        global_max_lsn_hex
    )

    from_lsn = window["from_lsn"]
    to_lsn = window["to_lsn"]

    conn = get_sqlserver_connection("Northwind")
    cursor = conn.cursor()

    try:

        cursor.execute("""
            SELECT
                __$start_lsn,
                __$seqval,
                __$operation,

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
                Fax

            FROM cdc.fn_cdc_get_all_changes_dbo_Customers(
                ?,
                ?,
                N'all update old'
            )

            ORDER BY
                __$start_lsn,
                __$seqval,
                __$operation

        """, (from_lsn, to_lsn))

        rows = cursor.fetchall()

        print(
            f"[Customers] CDC records found: {len(rows)}"
        )

        # ----------------------------------------------------
        # LOAD TO CLICKHOUSE
        # ----------------------------------------------------

        if rows:

            clickhouse_client = get_clickhouse_client()

            try:

                clickhouse_rows = []

                for row in rows:

                    clickhouse_rows.append(
                        (
                            row[0].hex(),
                            row[1].hex(),
                            int(row[2]),

                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                            row[10],
                            row[11],
                            row[12],
                            row[13],

                            datetime.now()
                        )
                    )

                clickhouse_client.insert(
                    "CDC_Customers_Raw",
                    clickhouse_rows,
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

                print(
                    "Customers loaded to ClickHouse: "
                    f"{len(clickhouse_rows)}"
                )

            finally:

                clickhouse_client.close()

        # ----------------------------------------------------
        # PUSH WATERMARK
        # ALWAYS PUSH - EVEN IF ZERO ROWS
        # ----------------------------------------------------

        ti.xcom_push(
            key="customers_to_lsn",
            value=to_lsn.hex()
        )

        ti.xcom_push(
            key="customers_record_count",
            value=len(rows)
        )

    finally:

        cursor.close()
        conn.close()


# ============================================================
# UPDATE CDC STATE
# ============================================================

def update_cdc_state(**context):

    ti = context["ti"]

    orders_lsn_hex = ti.xcom_pull(
        task_ids="extract_orders_cdc",
        key="orders_to_lsn"
    )

    order_details_lsn_hex = ti.xcom_pull(
        task_ids="extract_order_details_cdc",
        key="order_details_to_lsn"
    )

    customers_lsn_hex = ti.xcom_pull(
        task_ids="extract_customers_cdc",
        key="customers_to_lsn"
    )

    conn = get_sqlserver_connection("ETL_Settings")
    cursor = conn.cursor()

    try:

        # ----------------------------------------------------
        # UPDATE ORDERS
        # ----------------------------------------------------

        if orders_lsn_hex:

            orders_lsn = bytes.fromhex(
                orders_lsn_hex
            )

            cursor.execute("""
                UPDATE dbo.cdc_state
                SET
                    LastProcessedLSN = ?,
                    LastProcessedAt = GETDATE()
                WHERE TableName = 'Orders'
            """, orders_lsn)

            print(
                "[Orders] CDC state updated to: "
                f"{orders_lsn_hex}"
            )

        # ----------------------------------------------------
        # UPDATE ORDER DETAILS
        # ----------------------------------------------------

        if order_details_lsn_hex:

            order_details_lsn = bytes.fromhex(
                order_details_lsn_hex
            )

            cursor.execute("""
                UPDATE dbo.cdc_state
                SET
                    LastProcessedLSN = ?,
                    LastProcessedAt = GETDATE()
                WHERE TableName = 'Order Details'
            """, order_details_lsn)

            print(
                "[Order Details] CDC state updated to: "
                f"{order_details_lsn_hex}"
            )

        # ----------------------------------------------------
        # UPDATE CUSTOMERS
        # ----------------------------------------------------

        if customers_lsn_hex:

            customers_lsn = bytes.fromhex(
                customers_lsn_hex
            )

            cursor.execute("""
                UPDATE dbo.cdc_state
                SET
                    LastProcessedLSN = ?,
                    LastProcessedAt = GETDATE()
                WHERE TableName = 'Customers'
            """, customers_lsn)

            print(
                "[Customers] CDC state updated to: "
                f"{customers_lsn_hex}"
            )

        conn.commit()

        print("=" * 70)
        print("CDC STATE UPDATE COMPLETED")
        print("=" * 70)

    finally:

        cursor.close()
        conn.close()


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="etl_cdc_fact_sales",

    start_date=datetime(2026, 1, 1),

    schedule="*/30 * * * *",

    catchup=False,

    tags=[
        "northwind",
        "cdc",
        "fact",
        "sqlserver"
    ]

) as dag:

    # ========================================================
    # TEST CLICKHOUSE CONNECTION
    # ========================================================

    test_clickhouse_task = PythonOperator(
        task_id="test_clickhouse_connection",
        python_callable=test_clickhouse_connection
    )

    # ========================================================
    # GET GLOBAL CDC WATERMARK
    # ========================================================

    global_watermark_task = PythonOperator(
        task_id="get_global_cdc_watermark",
        python_callable=get_global_cdc_watermark
    )

    # ========================================================
    # CDC EXTRACT TASKS
    # ========================================================

    extract_orders_task = PythonOperator(
        task_id="extract_orders_cdc",
        python_callable=extract_orders_cdc
    )

    extract_order_details_task = PythonOperator(
        task_id="extract_order_details_cdc",
        python_callable=extract_order_details_cdc
    )

    extract_customers_task = PythonOperator(
        task_id="extract_customers_cdc",
        python_callable=extract_customers_cdc
    )

    # ========================================================
    # UPDATE CDC STATE
    # ========================================================

    update_state_task = PythonOperator(
        task_id="update_cdc_state",
        python_callable=update_cdc_state
    )

    # ========================================================
    # DAG FLOW
    # ========================================================

    test_clickhouse_task >> global_watermark_task

    global_watermark_task >> [
        extract_orders_task,
        extract_order_details_task,
        extract_customers_task
    ]

    [
        extract_orders_task,
        extract_order_details_task,
        extract_customers_task
    ] >> update_state_task