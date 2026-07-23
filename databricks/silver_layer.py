# ============================================================
# ADVENTUREWORKS SILVER LAYER
#
# Source:
# workspace.bronze
#
# Target:
# workspace.silver
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    initcap,
    trim,
    upper,
    when,
)


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_CATALOG = "workspace"
SOURCE_SCHEMA = "bronze"
SOURCE_DATABASE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}"

TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "silver"
TARGET_DATABASE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"

BRONZE_METADATA_COLUMNS = [
    "_source_file",
    "_ingested_at",
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_bronze_table(table_name: str) -> DataFrame:
    """
    Load a Bronze table using its complete Unity Catalog name
    and remove Bronze-specific ingestion metadata.
    """

    full_table_name = f"{SOURCE_DATABASE}.{table_name}"

    print(f"Loading Bronze table: {full_table_name}")

    dataframe = spark.table(full_table_name)

    existing_metadata_columns = [
        column_name
        for column_name in BRONZE_METADATA_COLUMNS
        if column_name in dataframe.columns
    ]

    if existing_metadata_columns:
        dataframe = dataframe.drop(*existing_metadata_columns)

    return dataframe


def clean_string(column_name: str):
    """
    Trim a value and convert blank strings or textual NULL values
    into actual nulls.
    """

    string_value = trim(col(column_name).cast("string"))

    return (
        when(
            string_value.isin("", "NULL", "null", "None"),
            None,
        )
        .otherwise(string_value)
    )


def clean_int(column_name: str):
    """Clean and cast a column to integer."""

    return clean_string(column_name).cast("int")


def clean_double(column_name: str):
    """Clean and cast a column to double."""

    return clean_string(column_name).cast("double")


def validate_zero(
    validation_name: str,
    invalid_count: int,
) -> None:
    """
    Fail the Silver job when a critical validation returns
    invalid records.
    """

    print(f"{validation_name}: {invalid_count:,}")

    if invalid_count > 0:
        raise ValueError(
            f"Data-quality validation failed: "
            f"{validation_name} = {invalid_count:,}"
        )


def save_silver_table(
    dataframe: DataFrame,
    table_name: str,
) -> int:
    """
    Add Silver processing metadata and save the DataFrame as a
    managed Unity Catalog Delta table.
    """

    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    final_dataframe = dataframe.withColumn(
        "_silver_processed_at",
        current_timestamp(),
    )

    print(f"Writing Silver table: {full_table_name}")

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
            f"Silver table was created but contains no rows: "
            f"{full_table_name}"
        )

    print(
        f"Completed: {full_table_name} "
        f"with {row_count:,} rows"
    )

    return row_count


# ============================================================
# STEP 1 — LOAD BRONZE TABLES
# ============================================================

address_df = load_bronze_table("address")
country_region_df = load_bronze_table("country_region")
customer_df = load_bronze_table("customer")
employee_df = load_bronze_table("employee")
person_df = load_bronze_table("person")
product_df = load_bronze_table("product")
product_category_df = load_bronze_table("product_category")
product_subcategory_df = load_bronze_table("product_subcategory")
sales_order_detail_df = load_bronze_table("sales_order_detail")
sales_order_header_df = load_bronze_table("sales_order_header")
sales_person_df = load_bronze_table("sales_person")
sales_territory_df = load_bronze_table("sales_territory")
state_province_df = load_bronze_table("state_province")


# ============================================================
# STEP 2 — CLEAN AND STANDARDIZE DATA
# ============================================================

# ------------------------------------------------------------
# Address
# ------------------------------------------------------------

address_df = (
    address_df
    .dropDuplicates(["AddressID"])
    .withColumn("AddressID", clean_int("AddressID"))
    .withColumn("AddressLine1", clean_string("AddressLine1"))
    .withColumn("AddressLine2", clean_string("AddressLine2"))
    .withColumn("City", initcap(clean_string("City")))
    .withColumn("StateProvinceID", clean_int("StateProvinceID"))
    .withColumn("PostalCode", clean_string("PostalCode"))
)


# ------------------------------------------------------------
# Country Region
# ------------------------------------------------------------

country_region_df = (
    country_region_df
    .dropDuplicates(["CountryRegionCode"])
    .withColumn(
        "CountryRegionCode",
        upper(clean_string("CountryRegionCode")),
    )
    .withColumn("Name", initcap(clean_string("Name")))
)


# ------------------------------------------------------------
# Customer
# ------------------------------------------------------------

customer_df = (
    customer_df
    .dropDuplicates(["CustomerID"])
    .withColumn("CustomerID", clean_int("CustomerID"))
    .withColumn("PersonID", clean_int("PersonID"))
    .withColumn("StoreID", clean_int("StoreID"))
    .withColumn("TerritoryID", clean_int("TerritoryID"))
    .withColumn("AccountNumber", clean_string("AccountNumber"))
)


# ------------------------------------------------------------
# Employee
# ------------------------------------------------------------

employee_df = (
    employee_df
    .dropDuplicates(["BusinessEntityID"])
    .withColumn(
        "BusinessEntityID",
        clean_int("BusinessEntityID"),
    )
    .withColumn(
        "NationalIDNumber",
        clean_string("NationalIDNumber"),
    )
    .withColumn("LoginID", clean_string("LoginID"))
    .withColumn("JobTitle", initcap(clean_string("JobTitle")))
    .withColumn("MaritalStatus", upper(clean_string("MaritalStatus")))
    .withColumn("Gender", upper(clean_string("Gender")))
    .withColumn("SalariedFlag", clean_int("SalariedFlag"))
    .withColumn("VacationHours", clean_int("VacationHours"))
    .withColumn("SickLeaveHours", clean_int("SickLeaveHours"))
    .withColumn("CurrentFlag", clean_int("CurrentFlag"))
)


# ------------------------------------------------------------
# Person
# ------------------------------------------------------------

person_df = (
    person_df
    .dropDuplicates(["BusinessEntityID"])
    .withColumn(
        "BusinessEntityID",
        clean_int("BusinessEntityID"),
    )
    .withColumn("PersonType", upper(clean_string("PersonType")))
    .withColumn("Title", clean_string("Title"))
    .withColumn("FirstName", initcap(clean_string("FirstName")))
    .withColumn("MiddleName", initcap(clean_string("MiddleName")))
    .withColumn("LastName", initcap(clean_string("LastName")))
    .withColumn("Suffix", clean_string("Suffix"))
    .withColumn("EmailPromotion", clean_int("EmailPromotion"))
)


# ------------------------------------------------------------
# Product Category
# ------------------------------------------------------------

product_category_df = (
    product_category_df
    .dropDuplicates(["ProductCategoryID"])
    .withColumn(
        "ProductCategoryID",
        clean_int("ProductCategoryID"),
    )
    .withColumn("Name", initcap(clean_string("Name")))
)


# ------------------------------------------------------------
# Product Subcategory
# ------------------------------------------------------------

product_subcategory_df = (
    product_subcategory_df
    .dropDuplicates(["ProductSubcategoryID"])
    .withColumn(
        "ProductSubcategoryID",
        clean_int("ProductSubcategoryID"),
    )
    .withColumn(
        "ProductCategoryID",
        clean_int("ProductCategoryID"),
    )
    .withColumn("Name", initcap(clean_string("Name")))
)


# ------------------------------------------------------------
# Product
# ------------------------------------------------------------

product_df = (
    product_df
    .dropDuplicates(["ProductID"])
    .withColumn("ProductID", clean_int("ProductID"))
    .withColumn("Name", clean_string("Name"))
    .withColumn("ProductNumber", clean_string("ProductNumber"))
    .withColumn("MakeFlag", clean_int("MakeFlag"))
    .withColumn("FinishedGoodsFlag", clean_int("FinishedGoodsFlag"))
    .withColumn("Color", initcap(clean_string("Color")))
    .withColumn("SafetyStockLevel", clean_int("SafetyStockLevel"))
    .withColumn("ReorderPoint", clean_int("ReorderPoint"))
    .withColumn("StandardCost", clean_double("StandardCost"))
    .withColumn("ListPrice", clean_double("ListPrice"))
    .withColumn("Size", clean_string("Size"))
    .withColumn("Weight", clean_double("Weight"))
    .withColumn("DaysToManufacture", clean_int("DaysToManufacture"))
    .withColumn(
        "ProductSubcategoryID",
        clean_int("ProductSubcategoryID"),
    )
    .withColumn("ProductModelID", clean_int("ProductModelID"))
)


# ------------------------------------------------------------
# Sales Order Header
# ------------------------------------------------------------

sales_order_header_df = (
    sales_order_header_df
    .dropDuplicates(["SalesOrderID"])
    .withColumn("SalesOrderID", clean_int("SalesOrderID"))
    .withColumn("RevisionNumber", clean_int("RevisionNumber"))
    .withColumn("Status", clean_int("Status"))
    .withColumn("OnlineOrderFlag", clean_int("OnlineOrderFlag"))
    .withColumn("CustomerID", clean_int("CustomerID"))
    .withColumn("SalesPersonID", clean_int("SalesPersonID"))
    .withColumn("TerritoryID", clean_int("TerritoryID"))
    .withColumn("BillToAddressID", clean_int("BillToAddressID"))
    .withColumn("ShipToAddressID", clean_int("ShipToAddressID"))
    .withColumn("ShipMethodID", clean_int("ShipMethodID"))
    .withColumn("CreditCardID", clean_int("CreditCardID"))
    .withColumn("CurrencyRateID", clean_int("CurrencyRateID"))
    .withColumn("SubTotal", clean_double("SubTotal"))
    .withColumn("TaxAmt", clean_double("TaxAmt"))
    .withColumn("Freight", clean_double("Freight"))
    .withColumn("TotalDue", clean_double("TotalDue"))
)


# ------------------------------------------------------------
# Sales Order Detail
# ------------------------------------------------------------

sales_order_detail_df = (
    sales_order_detail_df
    .dropDuplicates(
        [
            "SalesOrderID",
            "SalesOrderDetailID",
        ]
    )
    .withColumn("SalesOrderID", clean_int("SalesOrderID"))
    .withColumn(
        "SalesOrderDetailID",
        clean_int("SalesOrderDetailID"),
    )
    .withColumn("OrderQty", clean_int("OrderQty"))
    .withColumn("ProductID", clean_int("ProductID"))
    .withColumn("SpecialOfferID", clean_int("SpecialOfferID"))
    .withColumn("UnitPrice", clean_double("UnitPrice"))
    .withColumn(
        "UnitPriceDiscount",
        clean_double("UnitPriceDiscount"),
    )
    .withColumn("LineTotal", clean_double("LineTotal"))
)


# ------------------------------------------------------------
# Sales Person
# ------------------------------------------------------------

sales_person_df = (
    sales_person_df
    .dropDuplicates(["BusinessEntityID"])
    .withColumn(
        "BusinessEntityID",
        clean_int("BusinessEntityID"),
    )
    .withColumn("TerritoryID", clean_int("TerritoryID"))
    .withColumn("SalesQuota", clean_double("SalesQuota"))
    .withColumn("Bonus", clean_double("Bonus"))
    .withColumn("CommissionPct", clean_double("CommissionPct"))
    .withColumn("SalesYTD", clean_double("SalesYTD"))
    .withColumn("SalesLastYear", clean_double("SalesLastYear"))
)


# ------------------------------------------------------------
# Sales Territory
# ------------------------------------------------------------

sales_territory_df = (
    sales_territory_df
    .dropDuplicates(["TerritoryID"])
    .withColumn("TerritoryID", clean_int("TerritoryID"))
    .withColumn("Name", initcap(clean_string("Name")))
    .withColumn(
        "CountryRegionCode",
        upper(clean_string("CountryRegionCode")),
    )
    .withColumn("Group", clean_string("Group"))
    .withColumn("SalesYTD", clean_double("SalesYTD"))
    .withColumn("SalesLastYear", clean_double("SalesLastYear"))
    .withColumn("CostYTD", clean_double("CostYTD"))
    .withColumn("CostLastYear", clean_double("CostLastYear"))
)


# ------------------------------------------------------------
# State Province
# ------------------------------------------------------------

state_province_df = (
    state_province_df
    .dropDuplicates(["StateProvinceID"])
    .withColumn(
        "StateProvinceID",
        clean_int("StateProvinceID"),
    )
    .withColumn(
        "StateProvinceCode",
        upper(clean_string("StateProvinceCode")),
    )
    .withColumn(
        "CountryRegionCode",
        upper(clean_string("CountryRegionCode")),
    )
    .withColumn(
        "IsOnlyStateProvinceFlag",
        clean_int("IsOnlyStateProvinceFlag"),
    )
    .withColumn("Name", initcap(clean_string("Name")))
    .withColumn("TerritoryID", clean_int("TerritoryID"))
)


# ============================================================
# STEP 3 — CRITICAL DATA-QUALITY VALIDATION
# ============================================================

validate_zero(
    "Null AddressID",
    address_df.filter(col("AddressID").isNull()).count(),
)

validate_zero(
    "Null CountryRegionCode",
    country_region_df
    .filter(col("CountryRegionCode").isNull())
    .count(),
)

validate_zero(
    "Null CustomerID",
    customer_df.filter(col("CustomerID").isNull()).count(),
)

validate_zero(
    "Null Employee BusinessEntityID",
    employee_df
    .filter(col("BusinessEntityID").isNull())
    .count(),
)

validate_zero(
    "Null Person BusinessEntityID",
    person_df
    .filter(col("BusinessEntityID").isNull())
    .count(),
)

validate_zero(
    "Null ProductID",
    product_df.filter(col("ProductID").isNull()).count(),
)

validate_zero(
    "Null ProductCategoryID",
    product_category_df
    .filter(col("ProductCategoryID").isNull())
    .count(),
)

validate_zero(
    "Null ProductSubcategoryID",
    product_subcategory_df
    .filter(col("ProductSubcategoryID").isNull())
    .count(),
)

validate_zero(
    "Null SalesOrderID in header",
    sales_order_header_df
    .filter(col("SalesOrderID").isNull())
    .count(),
)

validate_zero(
    "Null SalesOrderDetailID",
    sales_order_detail_df
    .filter(col("SalesOrderDetailID").isNull())
    .count(),
)

validate_zero(
    "Non-positive OrderQty",
    sales_order_detail_df
    .filter(
        col("OrderQty").isNull()
        | (col("OrderQty") <= 0)
    )
    .count(),
)

validate_zero(
    "Negative UnitPrice",
    sales_order_detail_df
    .filter(
        col("UnitPrice").isNotNull()
        & (col("UnitPrice") < 0)
    )
    .count(),
)

validate_zero(
    "Negative LineTotal",
    sales_order_detail_df
    .filter(
        col("LineTotal").isNotNull()
        & (col("LineTotal") < 0)
    )
    .count(),
)

validate_zero(
    "ShipDate before OrderDate",
    sales_order_header_df
    .filter(
        col("ShipDate").isNotNull()
        & col("OrderDate").isNotNull()
        & (col("ShipDate") < col("OrderDate"))
    )
    .count(),
)

validate_zero(
    "DueDate before OrderDate",
    sales_order_header_df
    .filter(
        col("DueDate").isNotNull()
        & col("OrderDate").isNotNull()
        & (col("DueDate") < col("OrderDate"))
    )
    .count(),
)


# ============================================================
# STEP 4 — REFERENTIAL-INTEGRITY VALIDATION
# ============================================================

invalid_products = (
    sales_order_detail_df
    .select("ProductID")
    .filter(col("ProductID").isNotNull())
    .join(
        product_df.select("ProductID"),
        on="ProductID",
        how="left_anti",
    )
)

validate_zero(
    "Sales products missing from Product",
    invalid_products.count(),
)


invalid_customers = (
    sales_order_header_df
    .select("CustomerID")
    .filter(col("CustomerID").isNotNull())
    .join(
        customer_df.select("CustomerID"),
        on="CustomerID",
        how="left_anti",
    )
)

validate_zero(
    "Sales customers missing from Customer",
    invalid_customers.count(),
)


invalid_territories = (
    sales_order_header_df
    .select("TerritoryID")
    .filter(col("TerritoryID").isNotNull())
    .join(
        sales_territory_df.select("TerritoryID"),
        on="TerritoryID",
        how="left_anti",
    )
)

validate_zero(
    "Sales territories missing from Territory",
    invalid_territories.count(),
)


invalid_product_subcategories = (
    product_df
    .select("ProductSubcategoryID")
    .filter(col("ProductSubcategoryID").isNotNull())
    .join(
        product_subcategory_df.select("ProductSubcategoryID"),
        on="ProductSubcategoryID",
        how="left_anti",
    )
)

validate_zero(
    "Products with invalid subcategories",
    invalid_product_subcategories.count(),
)


invalid_product_categories = (
    product_subcategory_df
    .select("ProductCategoryID")
    .filter(col("ProductCategoryID").isNotNull())
    .join(
        product_category_df.select("ProductCategoryID"),
        on="ProductCategoryID",
        how="left_anti",
    )
)

validate_zero(
    "Subcategories with invalid categories",
    invalid_product_categories.count(),
)


# ============================================================
# STEP 5 — CREATE SILVER SCHEMA
# ============================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}
    COMMENT 'Cleaned and validated AdventureWorks data'
    """
)

print(f"Silver schema ready: {TARGET_DATABASE}")


# ============================================================
# STEP 6 — SAVE SILVER DELTA TABLES
# ============================================================

SILVER_TABLES = {
    "address": address_df,
    "country_region": country_region_df,
    "customer": customer_df,
    "employee": employee_df,
    "person": person_df,
    "product": product_df,
    "product_category": product_category_df,
    "product_subcategory": product_subcategory_df,
    "sales_order_header": sales_order_header_df,
    "sales_order_detail": sales_order_detail_df,
    "sales_person": sales_person_df,
    "sales_territory": sales_territory_df,
    "state_province": state_province_df,
}

silver_results = {}

for table_name, dataframe in SILVER_TABLES.items():

    print("=" * 70)
    print(f"STARTING SILVER TABLE: {table_name}")
    print("=" * 70)

    silver_results[table_name] = save_silver_table(
        dataframe=dataframe,
        table_name=table_name,
    )


# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("")
print("=" * 70)
print("SILVER LAYER EXECUTION SUMMARY")
print("=" * 70)

for table_name, row_count in silver_results.items():
    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    print(
        f"{full_table_name:<50} "
        f"{row_count:>12,} rows"
    )

print("-" * 70)
print(f"TOTAL TABLES CREATED: {len(silver_results)}")
print("=" * 70)
print("Silver layer completed successfully.")


