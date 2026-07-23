# ============================================================
# ADVENTUREWORKS GOLD LAYER
#
# Source:
# workspace.silver
#
# Target:
# workspace.gold
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    date_format,
    dayofmonth,
    month,
    quarter,
    trim,
    weekofyear,
    year,
)


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_CATALOG = "workspace"
SOURCE_SCHEMA = "silver"
SOURCE_DATABASE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}"

TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "gold"
TARGET_DATABASE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"

SILVER_METADATA_COLUMNS = [
    "_silver_processed_at",
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_silver_table(table_name: str) -> DataFrame:
    """
    Load one Silver table using its full Unity Catalog name and
    remove Silver processing metadata.
    """

    full_table_name = f"{SOURCE_DATABASE}.{table_name}"

    print(f"Loading Silver table: {full_table_name}")

    dataframe = spark.table(full_table_name)

    existing_metadata_columns = [
        column_name
        for column_name in SILVER_METADATA_COLUMNS
        if column_name in dataframe.columns
    ]

    if existing_metadata_columns:
        dataframe = dataframe.drop(*existing_metadata_columns)

    return dataframe


def validate_zero(
    validation_name: str,
    invalid_count: int,
) -> None:
    """
    Fail the Gold job when a critical dimensional-model
    validation returns invalid records.
    """

    print(f"{validation_name}: {invalid_count:,}")

    if invalid_count > 0:
        raise ValueError(
            f"Gold-layer validation failed: "
            f"{validation_name} = {invalid_count:,}"
        )


def validate_unique_key(
    dataframe: DataFrame,
    key_column: str,
    object_name: str,
) -> None:
    """
    Confirm that a dimension key contains no nulls or duplicates.
    """

    null_count = (
        dataframe
        .filter(col(key_column).isNull())
        .count()
    )

    duplicate_count = (
        dataframe
        .groupBy(key_column)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    validate_zero(
        f"{object_name} null {key_column}",
        null_count,
    )

    validate_zero(
        f"{object_name} duplicate {key_column}",
        duplicate_count,
    )


def save_gold_table(
    dataframe: DataFrame,
    table_name: str,
) -> int:
    """
    Add Gold processing metadata and save a managed Delta table.
    """

    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    final_dataframe = dataframe.withColumn(
        "_gold_processed_at",
        current_timestamp(),
    )

    print(f"Writing Gold table: {full_table_name}")

    (
        final_dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    row_count = spark.table(full_table_name).count()

    if row_count == 0:
        raise ValueError(
            f"Gold table was created but contains no rows: "
            f"{full_table_name}"
        )

    print(
        f"Completed: {full_table_name} "
        f"with {row_count:,} rows"
    )

    return row_count


# ============================================================
# STEP 1 — LOAD SILVER TABLES
# ============================================================

customer_df = load_silver_table("customer")
person_df = load_silver_table("person")
product_df = load_silver_table("product")
product_subcategory_df = load_silver_table("product_subcategory")
product_category_df = load_silver_table("product_category")
sales_person_df = load_silver_table("sales_person")
sales_territory_df = load_silver_table("sales_territory")
sales_order_header_df = load_silver_table("sales_order_header")
sales_order_detail_df = load_silver_table("sales_order_detail")


# ============================================================
# STEP 2 — BUILD DIMENSIONS
# ============================================================

# ------------------------------------------------------------
# Dim Customer
# ------------------------------------------------------------

dim_customer = (
    customer_df.alias("c")
    .join(
        person_df.alias("p"),
        col("c.PersonID") == col("p.BusinessEntityID"),
        "left",
    )
    .select(
        col("c.CustomerID").alias("CustomerKey"),
        col("c.AccountNumber"),
        trim(
            concat_ws(
                " ",
                col("p.FirstName"),
                col("p.MiddleName"),
                col("p.LastName"),
            )
        ).alias("CustomerName"),
        col("c.TerritoryID"),
    )
    .dropDuplicates(["CustomerKey"])
)


# ------------------------------------------------------------
# Dim Product
# ------------------------------------------------------------

dim_product = (
    product_df.alias("p")
    .join(
        product_subcategory_df.alias("ps"),
        col("p.ProductSubcategoryID")
        == col("ps.ProductSubcategoryID"),
        "left",
    )
    .join(
        product_category_df.alias("pc"),
        col("ps.ProductCategoryID")
        == col("pc.ProductCategoryID"),
        "left",
    )
    .select(
        col("p.ProductID").alias("ProductKey"),
        col("p.Name").alias("ProductName"),
        col("p.ProductNumber"),
        col("p.Color"),
        col("p.Size"),
        col("p.StandardCost"),
        col("p.ListPrice"),
        col("ps.Name").alias("Subcategory"),
        col("pc.Name").alias("Category"),
    )
    .dropDuplicates(["ProductKey"])
)


# ------------------------------------------------------------
# Dim Territory
# ------------------------------------------------------------

dim_territory = (
    sales_territory_df
    .select(
        col("TerritoryID").alias("TerritoryKey"),
        col("Name").alias("Territory"),
        col("CountryRegionCode"),
        col("Group"),
    )
    .dropDuplicates(["TerritoryKey"])
)


# ------------------------------------------------------------
# Dim Salesperson
# ------------------------------------------------------------

dim_salesperson = (
    sales_person_df.alias("sp")
    .join(
        person_df.alias("p"),
        col("sp.BusinessEntityID")
        == col("p.BusinessEntityID"),
        "left",
    )
    .select(
        col("sp.BusinessEntityID").alias("SalesPersonKey"),
        trim(
            concat_ws(
                " ",
                col("p.FirstName"),
                col("p.MiddleName"),
                col("p.LastName"),
            )
        ).alias("SalesPersonName"),
        col("sp.TerritoryID"),
        col("sp.SalesQuota"),
        col("sp.Bonus"),
        col("sp.CommissionPct"),
        col("sp.SalesYTD"),
        col("sp.SalesLastYear"),
    )
    .dropDuplicates(["SalesPersonKey"])
)


# ------------------------------------------------------------
# Dim Date
# ------------------------------------------------------------

dim_date = (
    sales_order_header_df
    .select("OrderDate")
    .filter(col("OrderDate").isNotNull())
    .distinct()
    .withColumn(
        "DateKey",
        date_format(col("OrderDate"), "yyyyMMdd").cast("int"),
    )
    .withColumn("Year", year(col("OrderDate")))
    .withColumn("Quarter", quarter(col("OrderDate")))
    .withColumn("Month", month(col("OrderDate")))
    .withColumn(
        "MonthName",
        date_format(col("OrderDate"), "MMMM"),
    )
    .withColumn("Week", weekofyear(col("OrderDate")))
    .withColumn("Day", dayofmonth(col("OrderDate")))
    .withColumn(
        "WeekDay",
        date_format(col("OrderDate"), "EEEE"),
    )
    .select(
        "DateKey",
        "OrderDate",
        "Year",
        "Quarter",
        "Month",
        "MonthName",
        "Week",
        "Day",
        "WeekDay",
    )
    .dropDuplicates(["DateKey"])
)


# ============================================================
# STEP 3 — BUILD FACT TABLE
# ============================================================

fact_sales = (
    sales_order_detail_df.alias("d")
    .join(
        sales_order_header_df.alias("h"),
        col("d.SalesOrderID") == col("h.SalesOrderID"),
        "inner",
    )
    .select(
        col("d.SalesOrderID"),
        col("d.SalesOrderDetailID"),
        date_format(
            col("h.OrderDate"),
            "yyyyMMdd",
        ).cast("int").alias("DateKey"),
        col("h.CustomerID").alias("CustomerKey"),
        col("h.SalesPersonID").alias("SalesPersonKey"),
        col("h.TerritoryID").alias("TerritoryKey"),
        col("d.ProductID").alias("ProductKey"),
        col("d.OrderQty"),
        col("d.UnitPrice"),
        col("d.UnitPriceDiscount"),
        col("d.LineTotal"),
        col("h.SubTotal"),
        col("h.TaxAmt"),
        col("h.Freight"),
        col("h.TotalDue"),
    )
    .dropDuplicates(
        [
            "SalesOrderID",
            "SalesOrderDetailID",
        ]
    )
)


# ============================================================
# STEP 4 — VALIDATE DIMENSIONS
# ============================================================

validate_unique_key(
    dim_customer,
    "CustomerKey",
    "dim_customer",
)

validate_unique_key(
    dim_product,
    "ProductKey",
    "dim_product",
)

validate_unique_key(
    dim_territory,
    "TerritoryKey",
    "dim_territory",
)

validate_unique_key(
    dim_salesperson,
    "SalesPersonKey",
    "dim_salesperson",
)

validate_unique_key(
    dim_date,
    "DateKey",
    "dim_date",
)


# ============================================================
# STEP 5 — VALIDATE FACT TABLE
# ============================================================

validate_zero(
    "Fact rows with null SalesOrderID",
    fact_sales
    .filter(col("SalesOrderID").isNull())
    .count(),
)

validate_zero(
    "Fact rows with null SalesOrderDetailID",
    fact_sales
    .filter(col("SalesOrderDetailID").isNull())
    .count(),
)

validate_zero(
    "Fact rows with null DateKey",
    fact_sales
    .filter(col("DateKey").isNull())
    .count(),
)

validate_zero(
    "Fact rows with null CustomerKey",
    fact_sales
    .filter(col("CustomerKey").isNull())
    .count(),
)

validate_zero(
    "Fact rows with null ProductKey",
    fact_sales
    .filter(col("ProductKey").isNull())
    .count(),
)

validate_zero(
    "Fact rows with non-positive quantity",
    fact_sales
    .filter(
        col("OrderQty").isNull()
        | (col("OrderQty") <= 0)
    )
    .count(),
)

validate_zero(
    "Fact rows with negative LineTotal",
    fact_sales
    .filter(
        col("LineTotal").isNotNull()
        & (col("LineTotal") < 0)
    )
    .count(),
)


# ============================================================
# STEP 6 — REFERENTIAL-INTEGRITY VALIDATION
# ============================================================

invalid_fact_customers = (
    fact_sales
    .select("CustomerKey")
    .filter(col("CustomerKey").isNotNull())
    .distinct()
    .join(
        dim_customer.select("CustomerKey"),
        on="CustomerKey",
        how="left_anti",
    )
)

validate_zero(
    "Fact customer keys missing from dim_customer",
    invalid_fact_customers.count(),
)


invalid_fact_products = (
    fact_sales
    .select("ProductKey")
    .filter(col("ProductKey").isNotNull())
    .distinct()
    .join(
        dim_product.select("ProductKey"),
        on="ProductKey",
        how="left_anti",
    )
)

validate_zero(
    "Fact product keys missing from dim_product",
    invalid_fact_products.count(),
)


invalid_fact_dates = (
    fact_sales
    .select("DateKey")
    .filter(col("DateKey").isNotNull())
    .distinct()
    .join(
        dim_date.select("DateKey"),
        on="DateKey",
        how="left_anti",
    )
)

validate_zero(
    "Fact date keys missing from dim_date",
    invalid_fact_dates.count(),
)


invalid_fact_territories = (
    fact_sales
    .select("TerritoryKey")
    .filter(col("TerritoryKey").isNotNull())
    .distinct()
    .join(
        dim_territory.select("TerritoryKey"),
        on="TerritoryKey",
        how="left_anti",
    )
)

validate_zero(
    "Fact territory keys missing from dim_territory",
    invalid_fact_territories.count(),
)


# SalesPersonID is nullable in AdventureWorks, so only validate
# the non-null values.

invalid_fact_salespersons = (
    fact_sales
    .select("SalesPersonKey")
    .filter(col("SalesPersonKey").isNotNull())
    .distinct()
    .join(
        dim_salesperson.select("SalesPersonKey"),
        on="SalesPersonKey",
        how="left_anti",
    )
)

validate_zero(
    "Fact salesperson keys missing from dim_salesperson",
    invalid_fact_salespersons.count(),
)


# ============================================================
# STEP 7 — CREATE GOLD SCHEMA
# ============================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}
    COMMENT 'AdventureWorks dimensional model for analytics'
    """
)

print(f"Gold schema ready: {TARGET_DATABASE}")


# ============================================================
# STEP 8 — SAVE GOLD DELTA TABLES
# ============================================================

GOLD_TABLES = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_salesperson": dim_salesperson,
    "dim_territory": dim_territory,
    "dim_date": dim_date,
    "fact_sales": fact_sales,
}

gold_results = {}

for table_name, dataframe in GOLD_TABLES.items():

    print("=" * 70)
    print(f"STARTING GOLD TABLE: {table_name}")
    print("=" * 70)

    gold_results[table_name] = save_gold_table(
        dataframe=dataframe,
        table_name=table_name,
    )


# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("")
print("=" * 70)
print("GOLD LAYER EXECUTION SUMMARY")
print("=" * 70)

for table_name, row_count in gold_results.items():
    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    print(
        f"{full_table_name:<50} "
        f"{row_count:>12,} rows"
    )

print("-" * 70)
print(f"TOTAL TABLES CREATED: {len(gold_results)}")
print("=" * 70)
print("Gold layer completed successfully.")



