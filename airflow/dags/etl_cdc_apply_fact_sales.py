from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Connection

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import clickhouse_connect


# ============================================================
# CONFIGURATION
# ============================================================

CLICKHOUSE_CONN_ID = "clickhouse_northwind"
CLICKHOUSE_DATABASE = "northwind_dw"

DAG_ID = "etl_cdc_apply_fact_sales"


# ============================================================
# CLICKHOUSE CONNECTION
# ============================================================

def get_clickhouse_client():

    conn = Connection.get(
        conn_id=CLICKHOUSE_CONN_ID
    )

    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema or CLICKHOUSE_DATABASE
    )


# ============================================================
# DECIMAL HELPERS
# ============================================================

def decimal_2(value):

    if value is None:
        return Decimal("0.00")

    return Decimal(str(value)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )


def decimal_6(value):

    if value is None:
        return Decimal("0.000000")

    return Decimal(str(value)).quantize(
        Decimal("0.000001"),
        rounding=ROUND_HALF_UP
    )


# ============================================================
# CHECKPOINT
# ============================================================

def get_apply_state(client, table_name):

    result = client.query(
        """
        SELECT
            LastProcessedLSN,
            LastProcessedSeqVal
        FROM CDC_FactSales_State FINAL
        WHERE TableName = %(table_name)s
        ORDER BY LastProcessedAt DESC
        LIMIT 1
        """,
        parameters={
            "table_name": table_name
        }
    )

    if not result.result_rows:
        return None

    return (
        result.result_rows[0][0],
        result.result_rows[0][1]
    )


def update_apply_state(
    client,
    table_name,
    last_lsn,
    last_seqval
):

    now = datetime.now()

    client.insert(
        "CDC_FactSales_State",
        [
            (
                table_name,
                last_lsn,
                last_seqval,
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
        f"CDC FactSales state updated: "
        f"{table_name} -> "
        f"{last_lsn} / {last_seqval}"
    )


# ============================================================
# READ NEW EVENTS
# ============================================================

def get_new_events(
    client,
    table_name,
    checkpoint
):

    if checkpoint is None:

        result = client.query(
            f"""
            SELECT *
            FROM {table_name}
            WHERE Operation != 3
            ORDER BY StartLSN, SeqVal, Operation
            """
        )

    else:

        last_lsn, last_seqval = checkpoint

        result = client.query(
            f"""
            SELECT *
            FROM {table_name}
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
            ORDER BY StartLSN, SeqVal, Operation
            """,
            parameters={
                "last_lsn": last_lsn,
                "last_seqval": last_seqval
            }
        )

    return result.result_rows


# ============================================================
# DEDUPLICATE CDC EVENTS
# ============================================================

def deduplicate_events(events, source_table):

    seen = set()
    result = []

    for event in events:

        start_lsn = event[0]
        seqval = event[1]
        operation = int(event[2])

        if source_table == "Orders":

            order_id = int(event[3])

            event_key = (
                start_lsn,
                seqval,
                operation,
                order_id
            )

        else:

            order_id = int(event[3])
            product_id = int(event[4])

            event_key = (
                start_lsn,
                seqval,
                operation,
                order_id,
                product_id
            )

        if event_key in seen:
            continue

        seen.add(event_key)
        result.append(event)

    return result


# ============================================================
# FACT EVENT IDENTITY
# ============================================================

def fact_event_already_applied(
    client,
    order_id,
    product_id,
    source_table,
    start_lsn,
    seqval
):

    result = client.query(
        """
        SELECT count()
        FROM FactSales
        WHERE
            OrderID = %(order_id)s
            AND ProductID = %(product_id)s
            AND CDCSourceTable = %(source_table)s
            AND CDCStartLSN = %(start_lsn)s
            AND CDCSeqVal = %(seqval)s
        """,
        parameters={
            "order_id": order_id,
            "product_id": product_id,
            "source_table": source_table,
            "start_lsn": start_lsn,
            "seqval": seqval
        }
    )

    return int(result.result_rows[0][0]) > 0


# ============================================================
# CURRENT FACT ROW
# ============================================================

def get_current_fact_row(
    client,
    order_id,
    product_id
):

    result = client.query(
        """
        SELECT
            SalesKey,
            OrderID,
            ProductID,
            ProductKey,
            CustomerKey,
            EmployeeKey,
            OrderDateKey,
            RequiredDateKey,
            ShippedDateKey,
            GeographyKey,
            UnitPrice,
            Quantity,
            Discount,
            GrossSales,
            DiscountAmount,
            NetSales,
            AllocatedFreight,
            CDCOperation,
            CDCVersion,
            IsDeleted,
            CDCStartLSN,
            CDCSeqVal,
            CDCSourceTable
        FROM FactSales FINAL
        WHERE
            OrderID = %(order_id)s
            AND ProductID = %(product_id)s
        LIMIT 1
        """,
        parameters={
            "order_id": order_id,
            "product_id": product_id
        }
    )

    if not result.result_rows:
        return None

    return result.result_rows[0]


# ============================================================
# CURRENT FACT ROWS FOR ORDER
# ============================================================

def get_current_fact_rows_for_order(
    client,
    order_id
):

    result = client.query(
        """
        SELECT
            SalesKey,
            OrderID,
            ProductID,
            ProductKey,
            CustomerKey,
            EmployeeKey,
            OrderDateKey,
            RequiredDateKey,
            ShippedDateKey,
            GeographyKey,
            UnitPrice,
            Quantity,
            Discount,
            GrossSales,
            DiscountAmount,
            NetSales,
            AllocatedFreight,
            CDCOperation,
            CDCVersion,
            IsDeleted,
            CDCStartLSN,
            CDCSeqVal,
            CDCSourceTable
        FROM FactSales FINAL
        WHERE
            OrderID = %(order_id)s
            AND IsDeleted = 0
        ORDER BY ProductID
        """,
        parameters={
            "order_id": order_id
        }
    )

    return result.result_rows


# ============================================================
# LATEST ORDER HEADER AS OF EVENT
# ============================================================

def get_order_clean_as_of_event(
    client,
    order_id,
    event_lsn,
    event_seqval
):

    result = client.query(
        """
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
            Freight,
            ShipCountry,
            ShipRegion,
            ShipCity,
            ShipPostalCode,
            ShipAddress
        FROM CDC_Orders_Clean
        WHERE
            OrderID = %(order_id)s
            AND Operation != 3
            AND
            (
                StartLSN < %(event_lsn)s
                OR
                (
                    StartLSN = %(event_lsn)s
                    AND SeqVal <= %(event_seqval)s
                )
            )
        ORDER BY
            StartLSN DESC,
            SeqVal DESC
        LIMIT 1
        """,
        parameters={
            "order_id": order_id,
            "event_lsn": event_lsn,
            "event_seqval": event_seqval
        }
    )

    if not result.result_rows:
        return None

    row = result.result_rows[0]

    return {
        "OrderID": int(row[3]),
        "CustomerID": row[4],
        "EmployeeID": row[5],
        "OrderDate": row[6],
        "RequiredDate": row[7],
        "ShippedDate": row[8],
        "Freight": row[9],
        "ShipCountry": row[10],
        "ShipRegion": row[11],
        "ShipCity": row[12],
        "ShipPostalCode": row[13],
        "ShipAddress": row[14]
    }


# ============================================================
# LATEST ORDER HEADER
# ============================================================

def get_latest_order_clean(
    client,
    order_id
):

    result = client.query(
        """
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
            Freight,
            ShipCountry,
            ShipRegion,
            ShipCity,
            ShipPostalCode,
            ShipAddress
        FROM CDC_Orders_Clean
        WHERE
            OrderID = %(order_id)s
            AND Operation != 3
        ORDER BY
            StartLSN DESC,
            SeqVal DESC
        LIMIT 1
        """,
        parameters={
            "order_id": order_id
        }
    )

    if not result.result_rows:
        return None

    row = result.result_rows[0]

    return {
        "OrderID": int(row[3]),
        "CustomerID": row[4],
        "EmployeeID": row[5],
        "OrderDate": row[6],
        "RequiredDate": row[7],
        "ShippedDate": row[8],
        "Freight": row[9],
        "ShipCountry": row[10],
        "ShipRegion": row[11],
        "ShipCity": row[12],
        "ShipPostalCode": row[13],
        "ShipAddress": row[14]
    }


# ============================================================
# DIMENSION LOOKUPS
# ============================================================

def get_product_key(client, product_id):

    result = client.query(
        """
        SELECT ProductKey
        FROM DimProduct
        WHERE
            ProductAlternateKey = %(product_id)s
            AND IsCurrent = 1
        ORDER BY StartDate DESC
        LIMIT 1
        """,
        parameters={
            "product_id": product_id
        }
    )

    if not result.result_rows:
        raise Exception(
            f"ProductKey not found for ProductID={product_id}"
        )

    return int(result.result_rows[0][0])


def get_customer_key(client, customer_id):

    if customer_id is None:
        return None

    result = client.query(
        """
        SELECT CustomerKey
        FROM DimCustomer
        WHERE
            CustomerAlternateKey = %(customer_id)s
            AND IsCurrent = 1
        ORDER BY StartDate DESC
        LIMIT 1
        """,
        parameters={
            "customer_id": str(customer_id)
        }
    )

    if not result.result_rows:
        raise Exception(
            f"CustomerKey not found for CustomerID={customer_id}"
        )

    return int(result.result_rows[0][0])


def get_employee_key(client, employee_id):

    if employee_id is None:
        return None

    result = client.query(
        """
        SELECT EmployeeKey
        FROM DimEmployee
        WHERE
            EmployeeAlternateKey = %(employee_id)s
            AND IsCurrent = 1
        ORDER BY StartDate DESC
        LIMIT 1
        """,
        parameters={
            "employee_id": employee_id
        }
    )

    if not result.result_rows:
        raise Exception(
            f"EmployeeKey not found for EmployeeID={employee_id}"
        )

    return int(result.result_rows[0][0])


def get_date_key(client, value):

    if value is None:
        return None

    date_value = value.date()

    result = client.query(
        """
        SELECT DateKey
        FROM DimDate
        WHERE FullDate = %(full_date)s
        LIMIT 1
        """,
        parameters={
            "full_date": date_value
        }
    )

    if not result.result_rows:
        raise Exception(
            f"DateKey not found for date={date_value}"
        )

    return int(result.result_rows[0][0])


def get_geography_key(
    client,
    country,
    region,
    city,
    postal_code,
    address
):

    result = client.query(
        """
        SELECT GeographyKey
        FROM DimGeography
        WHERE
            Country = %(country)s
            AND
            (
                Region = %(region)s
                OR
                (
                    Region IS NULL
                    AND %(region)s IS NULL
                )
            )
            AND City = %(city)s
            AND
            (
                PostalCode = %(postal_code)s
                OR
                (
                    PostalCode IS NULL
                    AND %(postal_code)s IS NULL
                )
            )
            AND
            (
                Address = %(address)s
                OR
                (
                    Address IS NULL
                    AND %(address)s IS NULL
                )
            )
            AND IsCurrent = 1
        ORDER BY StartDate DESC
        LIMIT 1
        """,
        parameters={
            "country": country,
            "region": region,
            "city": city,
            "postal_code": postal_code,
            "address": address
        }
    )

    if not result.result_rows:

        print(
            "WARNING: GeographyKey not found. "
            f"Country={country}, "
            f"Region={region}, "
            f"City={city}, "
            f"PostalCode={postal_code}, "
            f"Address={address}"
        )

        return None

    return int(result.result_rows[0][0])


# ============================================================
# CALCULATE FINANCIAL VALUES
# ============================================================

def calculate_financials(
    unit_price,
    quantity,
    discount
):

    unit_price = decimal_2(unit_price)
    quantity = int(quantity)
    discount = decimal_6(discount)

    gross_sales = (
        unit_price * Decimal(quantity)
    ).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )

    discount_amount = (
        gross_sales * discount
    ).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )

    net_sales = (
        gross_sales - discount_amount
    ).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP
    )

    return (
        unit_price,
        quantity,
        discount,
        gross_sales,
        discount_amount,
        net_sales
    )


# ============================================================
# NEXT VERSION
# ============================================================

def get_next_version(
    client,
    order_id,
    product_id
):

    result = client.query(
        """
        SELECT max(CDCVersion)
        FROM FactSales
        WHERE
            OrderID = %(order_id)s
            AND ProductID = %(product_id)s
        """,
        parameters={
            "order_id": order_id,
            "product_id": product_id
        }
    )

    max_version = result.result_rows[0][0]

    if max_version is None:
        return 1

    return int(max_version) + 1


# ============================================================
# INSERT FACT ROW
# ============================================================

def insert_fact_rows(client, rows):

    if not rows:
        return

    client.insert(
        "FactSales",
        rows,
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
            "CDCStartLSN",
            "CDCSeqVal",
            "CDCSourceTable"
        ]
    )


# ============================================================
# BUILD ROW FROM EXISTING FACT ROW
# ============================================================

def build_detail_update_row(
    client,
    current_row,
    start_lsn,
    seqval,
    operation,
    unit_price,
    quantity,
    discount
):

    order_id = int(current_row[1])
    product_id = int(current_row[2])

    (
        unit_price,
        quantity,
        discount,
        gross_sales,
        discount_amount,
        net_sales
    ) = calculate_financials(
        unit_price,
        quantity,
        discount
    )

    version = get_next_version(
        client,
        order_id,
        product_id
    )

    return (
        int(current_row[0]),       # SalesKey
        order_id,
        product_id,
        int(current_row[3]),       # ProductKey
        current_row[4],            # CustomerKey
        current_row[5],            # EmployeeKey
        current_row[6],            # OrderDateKey
        current_row[7],            # RequiredDateKey
        current_row[8],            # ShippedDateKey
        current_row[9],            # GeographyKey
        unit_price,
        quantity,
        discount,
        gross_sales,
        discount_amount,
        net_sales,
        current_row[16],           # AllocatedFreight
        operation,
        version,
        0,
        datetime.now(),
        start_lsn,
        seqval,
        "Order Details"
    )


# ============================================================
# APPLY ORDER DETAIL EVENT
# ============================================================

def apply_order_detail_event(
    client,
    event
):

    (
        start_lsn,
        seqval,
        operation,
        order_id,
        product_id,
        unit_price,
        quantity,
        discount,
        processed_at
    ) = event

    order_id = int(order_id)
    product_id = int(product_id)
    operation = int(operation)

    # --------------------------------------------------------
    # IDEMPOTENCY CHECK
    # --------------------------------------------------------

    if fact_event_already_applied(
        client,
        order_id,
        product_id,
        "Order Details",
        start_lsn,
        seqval
    ):

        print(
            f"SKIPPED already-applied Order Details event: "
            f"OrderID={order_id}, "
            f"ProductID={product_id}, "
            f"LSN={start_lsn}, "
            f"SeqVal={seqval}"
        )

        return

    current_row = get_current_fact_row(
        client,
        order_id,
        product_id
    )

    # --------------------------------------------------------
    # DELETE
    # --------------------------------------------------------

    if operation == 1:

        if current_row is None or int(current_row[19]) == 1:

            print(
                f"Order Details DELETE ignored: "
                f"OrderID={order_id}, "
                f"ProductID={product_id}"
            )

            return

        version = get_next_version(
            client,
            order_id,
            product_id
        )

        row = (
            int(current_row[0]),
            order_id,
            product_id,
            int(current_row[3]),
            current_row[4],
            current_row[5],
            current_row[6],
            current_row[7],
            current_row[8],
            current_row[9],
            current_row[10],
            int(current_row[11]),
            current_row[12],
            current_row[13],
            current_row[14],
            current_row[15],
            current_row[16],
            "D",
            version,
            1,
            datetime.now(),
            start_lsn,
            seqval,
            "Order Details"
        )

        insert_fact_rows(
            client,
            [row]
        )

        print(
            f"FactSales tombstone created: "
            f"OrderID={order_id}, "
            f"ProductID={product_id}, "
            f"Version={version}"
        )

        return

    # --------------------------------------------------------
    # INSERT / UPDATE NEW
    # --------------------------------------------------------

    if current_row is None:

        order_header = get_order_clean_as_of_event(
            client,
            order_id,
            start_lsn,
            seqval
        )

        if order_header is None:

            order_header = get_latest_order_clean(
                client,
                order_id
            )

        if order_header is None:

            raise Exception(
                f"Cannot create new FactSales line. "
                f"Order header not found for OrderID={order_id}"
            )

        product_key = get_product_key(
            client,
            product_id
        )

        customer_key = get_customer_key(
            client,
            order_header["CustomerID"]
        )

        employee_key = get_employee_key(
            client,
            order_header["EmployeeID"]
        )

        order_date_key = get_date_key(
            client,
            order_header["OrderDate"]
        )

        required_date_key = get_date_key(
            client,
            order_header["RequiredDate"]
        )

        shipped_date_key = get_date_key(
            client,
            order_header["ShippedDate"]
        )

        geography_key = get_geography_key(
            client,
            order_header["ShipCountry"],
            order_header["ShipRegion"],
            order_header["ShipCity"],
            order_header["ShipPostalCode"],
            order_header["ShipAddress"]
        )

        (
            unit_price,
            quantity,
            discount,
            gross_sales,
            discount_amount,
            net_sales
        ) = calculate_financials(
            unit_price,
            quantity,
            discount
        )

        # ----------------------------------------------------
        # LINE COUNT
        # ----------------------------------------------------

        line_count = client.query(
            """
            SELECT count()
            FROM FactSales FINAL
            WHERE
                OrderID = %(order_id)s
                AND IsDeleted = 0
            """,
            parameters={
                "order_id": order_id
            }
        ).result_rows[0][0]

        line_count = int(line_count) + 1

        freight = decimal_2(
            order_header["Freight"]
        )

        allocated_freight = (
            freight / Decimal(line_count)
        ).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )

        row = (
            0,
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
            "I" if operation == 2 else "U",
            1,
            0,
            datetime.now(),
            start_lsn,
            seqval,
            "Order Details"
        )

        insert_fact_rows(
            client,
            [row]
        )

        print(
            f"FactSales detail INSERT applied: "
            f"OrderID={order_id}, "
            f"ProductID={product_id}"
        )

        return

    # --------------------------------------------------------
    # EXISTING LINE UPDATE
    # --------------------------------------------------------

    operation_name = (
        "I"
        if operation == 2
        else "U"
    )

    row = build_detail_update_row(
        client,
        current_row,
        start_lsn,
        seqval,
        operation_name,
        unit_price,
        quantity,
        discount
    )

    insert_fact_rows(
        client,
        [row]
    )

    print(
        f"FactSales detail change applied: "
        f"OrderID={order_id}, "
        f"ProductID={product_id}, "
        f"Operation={operation}, "
        f"Version={row[18]}"
    )


# ============================================================
# APPLY ORDER EVENT
# ============================================================

def apply_order_event(
    client,
    event
):

    (
        start_lsn,
        seqval,
        operation,
        order_id,
        customer_id,
        employee_id,
        order_date,
        required_date,
        shipped_date,
        ship_via,
        freight,
        ship_name,
        ship_address,
        ship_city,
        ship_region,
        ship_postal_code,
        ship_country,
        processed_at
    ) = event

    order_id = int(order_id)
    operation = int(operation)

    current_rows = get_current_fact_rows_for_order(
        client,
        order_id
    )

    # --------------------------------------------------------
    # DELETE ORDER
    # --------------------------------------------------------

    if operation == 1:

        if not current_rows:

            print(
                f"Order DELETE ignored: "
                f"no current FactSales rows for "
                f"OrderID={order_id}"
            )

            return

        output_rows = []

        for current_row in current_rows:

            product_id = int(current_row[2])

            if fact_event_already_applied(
                client,
                order_id,
                product_id,
                "Orders",
                start_lsn,
                seqval
            ):
                continue

            version = get_next_version(
                client,
                order_id,
                product_id
            )

            row = (
                int(current_row[0]),
                order_id,
                product_id,
                int(current_row[3]),
                current_row[4],
                current_row[5],
                current_row[6],
                current_row[7],
                current_row[8],
                current_row[9],
                current_row[10],
                int(current_row[11]),
                current_row[12],
                current_row[13],
                current_row[14],
                current_row[15],
                current_row[16],
                "D",
                version,
                1,
                datetime.now(),
                start_lsn,
                seqval,
                "Orders"
            )

            output_rows.append(row)

        insert_fact_rows(
            client,
            output_rows
        )

        print(
            f"Order tombstones created: "
            f"OrderID={order_id}, "
            f"Lines={len(output_rows)}"
        )

        return

    # --------------------------------------------------------
    # ORDER INSERT / UPDATE
    # --------------------------------------------------------

    if not current_rows:

        print(
            f"Order event has no current FactSales lines: "
            f"OrderID={order_id}"
        )

        return

    order_header = {
        "OrderID": order_id,
        "CustomerID": customer_id,
        "EmployeeID": employee_id,
        "OrderDate": order_date,
        "RequiredDate": required_date,
        "ShippedDate": shipped_date,
        "Freight": freight,
        "ShipCountry": ship_country,
        "ShipRegion": ship_region,
        "ShipCity": ship_city,
        "ShipPostalCode": ship_postal_code,
        "ShipAddress": ship_address
    }

    output_rows = []

    for current_row in current_rows:

        product_id = int(current_row[2])

        if fact_event_already_applied(
            client,
            order_id,
            product_id,
            "Orders",
            start_lsn,
            seqval
        ):
            continue

        version = get_next_version(
            client,
            order_id,
            product_id
        )

        unit_price = current_row[10]
        quantity = int(current_row[11])
        discount = current_row[12]

        (
            unit_price,
            quantity,
            discount,
            gross_sales,
            discount_amount,
            net_sales
        ) = calculate_financials(
            unit_price,
            quantity,
            discount
        )

        line_count = len(current_rows)

        allocated_freight = (
            decimal_2(freight)
            / Decimal(line_count)
        ).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )

        customer_key = get_customer_key(
            client,
            customer_id
        )

        employee_key = get_employee_key(
            client,
            employee_id
        )

        order_date_key = get_date_key(
            client,
            order_date
        )

        required_date_key = get_date_key(
            client,
            required_date
        )

        shipped_date_key = get_date_key(
            client,
            shipped_date
        )

        geography_key = get_geography_key(
            client,
            ship_country,
            ship_region,
            ship_city,
            ship_postal_code,
            ship_address
        )

        row = (
            int(current_row[0]),
            order_id,
            product_id,
            int(current_row[3]),
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
            "U",
            version,
            0,
            datetime.now(),
            start_lsn,
            seqval,
            "Orders"
        )

        output_rows.append(row)

    insert_fact_rows(
        client,
        output_rows
    )

    print(
        f"Order change propagated: "
        f"OrderID={order_id}, "
        f"Lines={len(output_rows)}"
    )


# ============================================================
# APPLY CDC
# ============================================================

def apply_cdc_to_fact_sales():

    client = get_clickhouse_client()

    try:

        print("=" * 80)
        print("STARTING FACTSALES CDC APPLY")
        print("=" * 80)

        orders_checkpoint = get_apply_state(
            client,
            "Orders"
        )

        details_checkpoint = get_apply_state(
            client,
            "Order Details"
        )

        if orders_checkpoint is None:
            raise Exception(
                "CDC_FactSales_State for Orders is missing."
            )

        if details_checkpoint is None:
            raise Exception(
                "CDC_FactSales_State for Order Details is missing."
            )

        print(
            f"Orders checkpoint: {orders_checkpoint}"
        )

        print(
            f"Order Details checkpoint: {details_checkpoint}"
        )

        # ----------------------------------------------------
        # READ EVENTS
        # ----------------------------------------------------

        orders_events = get_new_events(
            client,
            "CDC_Orders_Clean",
            orders_checkpoint
        )

        details_events = get_new_events(
            client,
            "CDC_OrderDetails_Clean",
            details_checkpoint
        )

        print(
            f"Raw new Orders events: "
            f"{len(orders_events)}"
        )

        print(
            f"Raw new Order Details events: "
            f"{len(details_events)}"
        )

        # ----------------------------------------------------
        # DEDUP
        # ----------------------------------------------------

        orders_events = deduplicate_events(
            orders_events,
            "Orders"
        )

        details_events = deduplicate_events(
            details_events,
            "Order Details"
        )

        print(
            f"Deduplicated Orders events: "
            f"{len(orders_events)}"
        )

        print(
            f"Deduplicated Order Details events: "
            f"{len(details_events)}"
        )

        # ----------------------------------------------------
        # COMBINE GLOBAL CDC ORDER
        # ----------------------------------------------------

        combined_events = []

        for event in orders_events:

            combined_events.append(
                (
                    event[0],
                    event[1],
                    "Orders",
                    event
                )
            )

        for event in details_events:

            combined_events.append(
                (
                    event[0],
                    event[1],
                    "Order Details",
                    event
                )
            )

        combined_events.sort(
            key=lambda x: (
                x[0],
                x[1],
                x[2]
            )
        )

        # ----------------------------------------------------
        # PROCESS
        # ----------------------------------------------------

        latest_orders_checkpoint = orders_checkpoint
        latest_details_checkpoint = details_checkpoint

        orders_processed = 0
        details_processed = 0

        for _, _, source_table, event in combined_events:

            if source_table == "Orders":

                apply_order_event(
                    client,
                    event
                )

                latest_orders_checkpoint = (
                    event[0],
                    event[1]
                )

                orders_processed += 1

            else:

                apply_order_detail_event(
                    client,
                    event
                )

                latest_details_checkpoint = (
                    event[0],
                    event[1]
                )

                details_processed += 1

        # ----------------------------------------------------
        # CHECKPOINT
        # ----------------------------------------------------

        if orders_events:

            update_apply_state(
                client,
                "Orders",
                latest_orders_checkpoint[0],
                latest_orders_checkpoint[1]
            )

        if details_events:

            update_apply_state(
                client,
                "Order Details",
                latest_details_checkpoint[0],
                latest_details_checkpoint[1]
            )

        print("=" * 80)
        print("FACTSALES CDC APPLY COMPLETED")
        print("=" * 80)

        print(
            f"Orders events processed: "
            f"{orders_processed}"
        )

        print(
            f"Order Details events processed: "
            f"{details_processed}"
        )

        print(
            f"Final Orders checkpoint: "
            f"{latest_orders_checkpoint}"
        )

        print(
            f"Final Order Details checkpoint: "
            f"{latest_details_checkpoint}"
        )

        print("=" * 80)

    finally:

        client.close()


# ============================================================
# VALIDATE FACTSALES
# ============================================================

def validate_fact_sales():

    client = get_clickhouse_client()

    try:

        result = client.query(
            """
            SELECT
                count(),
                countDistinct(OrderID),
                countDistinct(ProductID)
            FROM FactSales FINAL
            WHERE IsDeleted = 0
            """
        )

        rows = result.result_rows[0]

        print("=" * 80)
        print("FACTSALES VALIDATION")
        print("=" * 80)

        print(f"Rows     : {rows[0]}")
        print(f"Orders   : {rows[1]}")
        print(f"Products : {rows[2]}")

        validation = client.query(
            """
            SELECT count()
            FROM FactSales FINAL
            WHERE
                IsDeleted = 0
                AND round(NetSales, 2)
                    !=
                    round(GrossSales - DiscountAmount, 2)
            """
        )

        errors = int(
            validation.result_rows[0][0]
        )

        print(
            f"Financial calculation errors: {errors}"
        )

        if errors != 0:

            raise Exception(
                f"FactSales financial validation failed: "
                f"{errors} rows."
            )

        print(
            "FactSales validation completed successfully."
        )

        print("=" * 80)

    finally:

        client.close()


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id=DAG_ID,

    start_date=datetime(2026, 1, 1),

    schedule="*/30 * * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "northwind",
        "cdc",
        "fact",
        "clickhouse"
    ]

) as dag:

    apply_task = PythonOperator(
        task_id="apply_cdc_to_fact_sales",
        python_callable=apply_cdc_to_fact_sales
    )

    validate_task = PythonOperator(
        task_id="validate_fact_sales",
        python_callable=validate_fact_sales
    )

    apply_task >> validate_task