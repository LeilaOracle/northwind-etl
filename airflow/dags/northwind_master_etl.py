from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="northwind_master_etl",
    start_date=datetime(2026, 1, 1),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "master", "etl"],
) as dag:

    # ============================================================
    # 1. Clear PostgreSQL FactSales
    #
    # FactSales has foreign keys to the PostgreSQL dimensions.
    # Therefore FactSales must be cleared BEFORE dimensions
    # are fully refreshed.
    # ============================================================

    trigger_cleanup_fact_sales = TriggerDagRunOperator(
        task_id="trigger_cleanup_fact_sales",
        trigger_dag_id="etl_cleanup_fact_sales",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    # ============================================================
    # 2. PostgreSQL Dimensions
    # ============================================================

    trigger_dim_date = TriggerDagRunOperator(
        task_id="trigger_dim_date",
        trigger_dag_id="etl_dim_date",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_geography = TriggerDagRunOperator(
        task_id="trigger_dim_geography",
        trigger_dag_id="etl_dim_geography",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_customer = TriggerDagRunOperator(
        task_id="trigger_dim_customer",
        trigger_dag_id="etl_dim_customer",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_employee = TriggerDagRunOperator(
        task_id="trigger_dim_employee",
        trigger_dag_id="etl_dim_employee",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_category = TriggerDagRunOperator(
        task_id="trigger_dim_category",
        trigger_dag_id="etl_dim_category",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_supplier = TriggerDagRunOperator(
        task_id="trigger_dim_supplier",
        trigger_dag_id="etl_dimsupplier_postgres",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dim_product = TriggerDagRunOperator(
        task_id="trigger_dim_product",
        trigger_dag_id="etl_dim_product",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    # ============================================================
    # 3. PostgreSQL Dimension Dependencies
    # ============================================================

    # Geography must be loaded before Customer.
    trigger_dim_geography >> trigger_dim_customer

    # Category and Supplier must be loaded before Product.
    trigger_dim_category >> trigger_dim_product
    trigger_dim_supplier >> trigger_dim_product

    # ============================================================
    # 4. PostgreSQL FactSales
    # ============================================================

    trigger_fact_sales = TriggerDagRunOperator(
        task_id="trigger_fact_sales",
        trigger_dag_id="etl_fact_sales",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    [
        trigger_dim_date,
        trigger_dim_geography,
        trigger_dim_customer,
        trigger_dim_employee,
        trigger_dim_category,
        trigger_dim_supplier,
        trigger_dim_product,
    ] >> trigger_fact_sales

    # ============================================================
    # 5. ClickHouse Dimensions
    # ============================================================

    trigger_dimdate_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimdate_clickhouse",
        trigger_dag_id="etl_dimdate_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimgeography_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimgeography_clickhouse",
        trigger_dag_id="etl_dimgeography_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimcustomer_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimcustomer_clickhouse",
        trigger_dag_id="etl_dimcustomer_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimemployee_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimemployee_clickhouse",
        trigger_dag_id="etl_dimemployee_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimcategory_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimcategory_clickhouse",
        trigger_dag_id="etl_dimcategory_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimsupplier_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimsupplier_clickhouse",
        trigger_dag_id="etl_dimsupplier_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    trigger_dimproduct_clickhouse = TriggerDagRunOperator(
        task_id="trigger_dimproduct_clickhouse",
        trigger_dag_id="etl_dimproduct_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    # ============================================================
    # 6. ClickHouse Dimension Dependencies
    # ============================================================

    # PostgreSQL → ClickHouse dimension flow.
    trigger_dim_date >> trigger_dimdate_clickhouse
    trigger_dim_geography >> trigger_dimgeography_clickhouse
    trigger_dim_customer >> trigger_dimcustomer_clickhouse
    trigger_dim_employee >> trigger_dimemployee_clickhouse
    trigger_dim_category >> trigger_dimcategory_clickhouse
    trigger_dim_supplier >> trigger_dimsupplier_clickhouse
    trigger_dim_product >> trigger_dimproduct_clickhouse

    # Geography must be ready before ClickHouse Customer.
    trigger_dimgeography_clickhouse >> trigger_dimcustomer_clickhouse

    # Category + Supplier must be ready before ClickHouse Product.
    trigger_dimcategory_clickhouse >> trigger_dimproduct_clickhouse
    trigger_dimsupplier_clickhouse >> trigger_dimproduct_clickhouse

    # ============================================================
    # 7. ClickHouse FactSales
    # ============================================================

    trigger_factsales_clickhouse = TriggerDagRunOperator(
        task_id="trigger_factsales_clickhouse",
        trigger_dag_id="etl_factsales_clickhouse",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=10,
    )

    [
        trigger_dimdate_clickhouse,
        trigger_dimgeography_clickhouse,
        trigger_dimcustomer_clickhouse,
        trigger_dimemployee_clickhouse,
        trigger_dimcategory_clickhouse,
        trigger_dimsupplier_clickhouse,
        trigger_dimproduct_clickhouse,
        trigger_fact_sales,
    ] >> trigger_factsales_clickhouse

    # ============================================================
    # 8. Critical ordering:
    #    Cleanup FactSales MUST happen before PostgreSQL dimensions.
    # ============================================================

    trigger_cleanup_fact_sales >> [
        trigger_dim_date,
        trigger_dim_geography,
        trigger_dim_employee,
        trigger_dim_category,
        trigger_dim_supplier,
    ]