# مستند فارسی پروژه Northwind ETL / Data Warehouse

## 1. معرفی پروژه

این پروژه یک Pipeline کامل ETL/ELT برای پایگاه داده Northwind است که با Docker، Apache Airflow، PostgreSQL، SQL Server، ClickHouse و Grafana پیاده‌سازی شده است.

هدف، انتقال داده‌ها از SQL Server، ایجاد لایه Staging در PostgreSQL، ساخت Data Warehouse در ClickHouse و ارائه تحلیل در Grafana است.

```text
SQL Server / Northwind
        │
        │ Full Load - Daily
        ▼
PostgreSQL / Staging
        │
        │ Incremental SCD Type 2
        ▼
ClickHouse / Data Warehouse
        │
        ▼
Grafana
```

مسیر CDC برای FactSales مستقل است:

```text
SQL Server CDC → CDC Raw → CDC Clean → CDC Apply → ClickHouse FactSales
```

## 2. اجزای اصلی

| Component | وظیفه |
|---|---|
| SQL Server | Source Database |
| PostgreSQL | Staging Database |
| Apache Airflow | Orchestration و Scheduling |
| Celery / Redis | اجرای Taskهای Airflow |
| ClickHouse | Data Warehouse تحلیلی |
| PySpark | ETL و SCD Type 2 |
| Grafana | Visualization |
| Docker Compose | اجرای سرویس‌ها |

## 3. لایه PostgreSQL

Dimensionهای اصلی:

- DimGeography
- DimCustomer
- DimEmployee
- DimProduct
- DimDate
- DimCategory
- DimSupplier

Fact:

- FactSales

Dimensionهای PostgreSQL به‌صورت Full Refresh از SQL Server بارگذاری می‌شوند.

## 4. DAGهای PostgreSQL

### etl_dim_geography.py
استخراج Geography از SQL Server و Full Refresh جدول DimGeography.

### etl_dim_customer.py
استخراج Customer، اتصال به Geography و بارگذاری DimCustomer.

Dependency:

```text
DimGeography → DimCustomer
```

### etl_dim_employee.py
استخراج و بارگذاری DimEmployee.

### etl_dim_product.py
استخراج Product و اتصال به Supplier و Category.

```text
DimCategory ──┐
              ├──> DimProduct
DimSupplier ──┘
```

### etl_dim_category.py
استخراج و Full Refresh جدول DimCategory.

### etl_dimsupplier_postgres.py
استخراج و بارگذاری DimSupplier.

### etl_dim_date.py
ساخت و بارگذاری DimDate و فراهم کردن DateKey.

## 5. PostgreSQL FactSales

### etl_fact_sales.py

Orders و Order Details را با Dimensionها ترکیب کرده و FactSales را تولید می‌کند.

```text
Orders + Order Details
        │
        ├── Customer Lookup
        ├── Employee Lookup
        ├── Product Lookup
        ├── Geography Lookup
        └── Date Lookup
                │
                ▼
            FactSales
```

محاسبات مالی:

```text
GrossSales      = UnitPrice × Quantity
DiscountAmount  = GrossSales × Discount
NetSales        = GrossSales - DiscountAmount
```

مقادیر مالی با Decimal و rounding مناسب محاسبه می‌شوند.

## 6. ClickHouse Dimensions

DAGهای زیر مسئول انتقال Dimensionها به ClickHouse هستند:

- etl_dimgeography_clickhouse.py
- etl_dimcustomer_clickhouse.py
- etl_dimcategory_clickhouse.py
- etl_dimproduct_clickhouse.py
- etl_dimsupplier_clickhouse.py
- etl_dimdate_clickhouse.py
- etl_dimemployee_clickhouse.py

این DAGها از PySpark استفاده می‌کنند.

الگوی SCD Type 2:

```text
Business Key
    │
    ├── Version 1  IsCurrent = 0
    │
    └── Version 2  IsCurrent = 1
```

رکورد قدیمی بسته شده و رکورد جدید با Version جدید ایجاد می‌شود. DAGها با `max_active_runs=1` از اجرای همزمان جلوگیری می‌کنند.

## 7. ClickHouse FactSales

### etl_factsales_clickhouse.py

FactSales را از PostgreSQL به ClickHouse منتقل می‌کند، baseline را آماده می‌کند و validation تعداد رکوردها و مقادیر مالی را انجام می‌دهد.

## 8. CDC

برای Orders و Order Details، CDC در SQL Server فعال است.

State اصلی SQL Server در:

```text
ETL_Settings.dbo.cdc_state
```

قرار دارد و شامل:

- TableName
- CaptureInstance
- LastProcessedLSN
- LastProcessedAt

است.

## 9. DAGهای CDC

### etl_cdc_fact_sales.py

Changeهای Orders و Order Details را از SQL Server CDC استخراج کرده و به لایه CDC Raw منتقل می‌کند.

Schedule:

```text
*/30 * * * *
```

### etl_cdc_clean.py

Raw CDC را پاک‌سازی و استاندارد کرده و برای Apply آماده می‌کند.

### etl_cdc_apply_fact_sales.py

Changeها را روی ClickHouse FactSales اعمال می‌کند:

```text
INSERT → Insert new row
UPDATE → Close old version + Insert new version
DELETE → Tombstone
```

برای Idempotency و checkpoint از LSN، SeqVal و Source Table استفاده می‌شود.

State نهایی در:

```text
CDC_FactSales_State
```

نگهداری می‌شود.

## 10. Master DAG

### northwind_master_etl.py

Master DAG مسئول orchestration بارگذاری شبانه است.

Schedule:

```text
0 22 * * *
```

دارای 17 Task است.

Dependencyهای مهم:

```text
DimGeography → DimCustomer

DimCategory + DimSupplier → DimProduct

All PostgreSQL Dimensions → PostgreSQL FactSales

PostgreSQL Dimensions → Corresponding ClickHouse Dimensions

All ClickHouse Dimensions + PostgreSQL FactSales
                         ↓
                  ClickHouse FactSales
```

ترتیب کلی:

```text
Cleanup FactSales
       ↓
PostgreSQL Dimensions
       ↓
PostgreSQL FactSales
       ↓
ClickHouse Dimensions
       ↓
ClickHouse FactSales
```

CDC در Master DAG قرار ندارد و مستقل اجرا می‌شود.

## 11. Cleanup DAG

### etl_cleanup_fact_sales.py

قبل از Full Load، FactSales را پاک‌سازی می‌کند تا baseline قبلی باعث duplicate نشود.

## 12. زمان‌بندی

| Pipeline | Schedule |
|---|---|
| Master ETL | هر روز ساعت 22:00 |
| CDC FactSales | هر 30 دقیقه |
| Grafana | Auto Refresh هر 5 دقیقه |

## 13. Grafana

Grafana به ClickHouse متصل است.

Database:

```text
northwind_dw
```

چهار Dashboard اصلی:

### Northwind - Sales Overview
- Total Net Sales
- Gross Sales
- Discount Amount
- Total Orders

### Northwind - Sales Analysis
- Sales Over Time
- Gross vs Net Sales
- Discount Trend
- Sales by Quarter

### Northwind - Product & Customer Analysis
- Sales by Category
- Top 10 Products
- Sales by Country
- Top 10 Customers

### Northwind - Employee & Operations Analysis
- Sales by Employee
- Orders by Employee
- Freight by Employee
- Average Order Value

تمام Dashboardها باید با Refresh Interval برابر `5m` تنظیم شوند.

## 14. Validation

Validationهای انجام‌شده شامل:

- تعداد رکوردهای FactSales
- تعداد Orders و Products
- GrossSales
- DiscountAmount
- NetSales
- AllocatedFreight
- consistency محاسبات مالی
- CDC UPDATE
- CDC DELETE
- CDC Idempotency
- CDC Tombstone
- تطبیق PostgreSQL و ClickHouse

پس از تست CDC، مقادیر منطقی نهایی:

```text
Rows               = 2154
Orders             = 830
Products           = 77
GrossSales         = 1354420.39
DiscountAmount     = 88665.82
NetSales           = 1265754.57
AllocatedFreight   = 64932.60
Calculation Errors = 0
```

## 15. Docker

سرویس‌های اصلی:

```text
northwind-postgres
northwind-clickhouse
northwind-airflow-...
northwind-redis
northwind-grafana
northwind-sqlserver
```

Network:

```text
northwind-network
```

Airflow با Python 3.10 و PySpark در Worker اجرا می‌شود.

## 16. Airflow Connections و Secret Management

Credentialها داخل DAGها Hardcode نشده‌اند.

Connectionهای اصلی:

```text
northwind_postgres
northwind_sqlserver
clickhouse_northwind
```

DAGها credential را از Airflow Connections دریافت می‌کنند.

Credentialهای واقعی در `.env` محلی قرار دارند و `.env` در Git commit نمی‌شود.

در repository فقط `.env.example` با مقادیر `CHANGE_ME` قرار دارد.

## 17. ساختار Repository

```text
northwind-etl/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── instnwnd.sql
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   └── jars/
└── spark/
    └── jars/
```

## 18. Git

Repository پروژه روی GitHub قرار گرفته است.

Branch اصلی:

```text
main
```

اولین commit:

```text
87ae9c2 Initial Northwind ETL pipeline
```

Working tree پس از push اولیه clean بوده است.

## 19. معماری نهایی

```text
                    ┌─────────────────────┐
                    │ SQL Server Northwind│
                    └──────────┬──────────┘
                               │
                         Daily Full Load
                               │
                               ▼
                    ┌─────────────────────┐
                    │ PostgreSQL Staging  │
                    │ Dimensions          │
                    │ FactSales           │
                    └──────────┬──────────┘
                               │
                         PySpark / SCD2
                               │
                               ▼
                    ┌─────────────────────┐
                    │ ClickHouse DW       │
                    │ Dimensions SCD2     │
                    │ FactSales           │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Grafana             │
                    │ Analytics           │
                    └─────────────────────┘


CDC Path:

SQL Server CDC
      │
      ▼
CDC Raw
      │
      ▼
CDC Clean
      │
      ▼
CDC Apply
      │
      ▼
ClickHouse FactSales
```

## 20. وضعیت پروژه

- Docker architecture پیاده‌سازی شده است.
- Airflow با PostgreSQL metadata database اجرا می‌شود.
- Python 3.10 و PySpark آماده هستند.
- PostgreSQL Dimensions و FactSales پیاده‌سازی و validation شده‌اند.
- ClickHouse Dimensions با SCD Type 2 پیاده‌سازی شده‌اند.
- ClickHouse FactSales پیاده‌سازی شده است.
- SQL Server CDC برای Orders و Order Details پیاده‌سازی و تست شده است.
- Master DAG با موفقیت اجرا شده است.
- Grafana به ClickHouse متصل و چهار Dashboard ایجاد شده است.
- Secretها از DAGها خارج و به Airflow Connections منتقل شده‌اند.
- Repository پروژه روی GitHub قرار گرفته است.
