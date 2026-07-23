# ============================================================
# ADVENTUREWORKS BRONZE LAYER
#
# Source:
# /Volumes/workspace/default/adventureworks_raw
#
# Target:
# workspace.bronze
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_VOLUME = "/Volumes/workspace/default/adventureworks_raw"

TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "bronze"

TARGET_DATABASE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"


# ============================================================
# TABLE CONFIGURATION
# ============================================================

TABLE_CONFIG = {
    "address": {
        "file_name": "Address.csv",
        "columns": [
            "AddressID",
            "AddressLine1",
            "AddressLine2",
            "City",
            "StateProvinceID",
            "PostalCode",
            "SpatialLocation",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "country_region": {
        "file_name": "CountryRegion.csv",
        "columns": [
            "CountryRegionCode",
            "Name",
            "ModifiedDate",
        ],
    },

    "customer": {
        "file_name": "Customer.csv",
        "columns": [
            "CustomerID",
            "PersonID",
            "StoreID",
            "TerritoryID",
            "AccountNumber",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "employee": {
        "file_name": "Employee.csv",
        "columns": [
            "BusinessEntityID",
            "NationalIDNumber",
            "LoginID",
            "OrganizationNode",
            "OrganizationLevel",
            "JobTitle",
            "BirthDate",
            "MaritalStatus",
            "Gender",
            "HireDate",
            "SalariedFlag",
            "VacationHours",
            "SickLeaveHours",
            "CurrentFlag",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "person": {
        "file_name": "Person.csv",
        "columns": [
            "BusinessEntityID",
            "PersonType",
            "NameStyle",
            "Title",
            "FirstName",
            "MiddleName",
            "LastName",
            "Suffix",
            "EmailPromotion",
            "AdditionalContactInfo",
            "Demographics",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "product": {
        "file_name": "Product.csv",
        "columns": [
            "ProductID",
            "Name",
            "ProductNumber",
            "MakeFlag",
            "FinishedGoodsFlag",
            "Color",
            "SafetyStockLevel",
            "ReorderPoint",
            "StandardCost",
            "ListPrice",
            "Size",
            "SizeUnitMeasureCode",
            "WeightUnitMeasureCode",
            "Weight",
            "DaysToManufacture",
            "ProductLine",
            "Class",
            "Style",
            "ProductSubcategoryID",
            "ProductModelID",
            "SellStartDate",
            "SellEndDate",
            "DiscontinuedDate",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "product_category": {
        "file_name": "ProductCategory.csv",
        "columns": [
            "ProductCategoryID",
            "Name",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "product_subcategory": {
        "file_name": "ProductSubcategory.csv",
        "columns": [
            "ProductSubcategoryID",
            "ProductCategoryID",
            "Name",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "sales_order_detail": {
        "file_name": "SalesOrderDetail.csv",
        "columns": [
            "SalesOrderID",
            "SalesOrderDetailID",
            "CarrierTrackingNumber",
            "OrderQty",
            "ProductID",
            "SpecialOfferID",
            "UnitPrice",
            "UnitPriceDiscount",
            "LineTotal",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "sales_order_header": {
        "file_name": "SalesOrderHeader.csv",
        "columns": [
            "SalesOrderID",
            "RevisionNumber",
            "OrderDate",
            "DueDate",
            "ShipDate",
            "Status",
            "OnlineOrderFlag",
            "SalesOrderNumber",
            "PurchaseOrderNumber",
            "AccountNumber",
            "CustomerID",
            "SalesPersonID",
            "TerritoryID",
            "BillToAddressID",
            "ShipToAddressID",
            "ShipMethodID",
            "CreditCardID",
            "CreditCardApprovalCode",
            "CurrencyRateID",
            "SubTotal",
            "TaxAmt",
            "Freight",
            "TotalDue",
            "Comment",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "sales_person": {
        "file_name": "SalesPerson.csv",
        "columns": [
            "BusinessEntityID",
            "TerritoryID",
            "SalesQuota",
            "Bonus",
            "CommissionPct",
            "SalesYTD",
            "SalesLastYear",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "sales_territory": {
        "file_name": "SalesTerritory.csv",
        "columns": [
            "TerritoryID",
            "Name",
            "CountryRegionCode",
            "Group",
            "SalesYTD",
            "SalesLastYear",
            "CostYTD",
            "CostLastYear",
            "rowguid",
            "ModifiedDate",
        ],
    },

    "state_province": {
        "file_name": "StateProvince.csv",
        "columns": [
            "StateProvinceID",
            "StateProvinceCode",
            "CountryRegionCode",
            "IsOnlyStateProvinceFlag",
            "Name",
            "TerritoryID",
            "rowguid",
            "ModifiedDate",
        ],
    },
}


# ============================================================
# CREATE BRONZE SCHEMA
# ============================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}
    COMMENT 'Raw AdventureWorks data loaded from Unity Catalog Volume'
    """
)

print(f"Bronze schema ready: {TARGET_DATABASE}")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def read_raw_csv(
    file_name: str,
    expected_columns: list[str],
) -> DataFrame:
    """
    Read one headerless AdventureWorks CSV from the Unity Catalog
    Volume, validate the column count, add source metadata, and
    assign the expected column names.
    """

    source_path = f"{SOURCE_VOLUME}/{file_name}"

    print(f"Reading source file: {source_path}")

    raw_df = (
        spark.read
        .format("csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .option("mode", "FAILFAST")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .load(source_path)
    )

    actual_column_count = len(raw_df.columns)
    expected_column_count = len(expected_columns)

    if actual_column_count != expected_column_count:
        raise ValueError(
            f"Column-count mismatch for {file_name}. "
            f"Expected {expected_column_count}, "
            f"but found {actual_column_count}."
        )

    raw_with_metadata = raw_df.select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
    )

    bronze_df = (
        raw_with_metadata
        .toDF(*expected_columns, "_source_file")
        .withColumn("_ingested_at", current_timestamp())
    )

    return bronze_df


def save_bronze_table(
    dataframe: DataFrame,
    table_name: str,
) -> int:
    """
    Save a DataFrame as a managed Unity Catalog Delta table
    and return the resulting row count.
    """

    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    print(f"Writing Delta table: {full_table_name}")

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    row_count = spark.table(full_table_name).count()

    if row_count == 0:
        raise ValueError(
            f"Bronze table was created but contains no rows: "
            f"{full_table_name}"
        )

    print(
        f"Completed: {full_table_name} "
        f"with {row_count:,} rows"
    )

    return row_count


# ============================================================
# LOAD ALL BRONZE TABLES
# ============================================================

bronze_results = {}

for table_name, config in TABLE_CONFIG.items():

    file_name = config["file_name"]
    expected_columns = config["columns"]

    print("=" * 70)
    print(f"STARTING BRONZE TABLE: {table_name}")
    print("=" * 70)

    dataframe = read_raw_csv(
        file_name=file_name,
        expected_columns=expected_columns,
    )

    row_count = save_bronze_table(
        dataframe=dataframe,
        table_name=table_name,
    )

    bronze_results[table_name] = row_count


# ============================================================
# EXECUTION SUMMARY
# ============================================================

print("")
print("=" * 70)
print("BRONZE LAYER EXECUTION SUMMARY")
print("=" * 70)

for table_name, row_count in bronze_results.items():
    full_table_name = f"{TARGET_DATABASE}.{table_name}"

    print(
        f"{full_table_name:<50} "
        f"{row_count:>12,} rows"
    )

print("-" * 70)
print(f"TOTAL TABLES CREATED: {len(bronze_results)}")
print("=" * 70)
print("Bronze layer completed successfully.")
