# Pipeline Execution

This document explains how to execute the AdventureWorks end-to-end data engineering pipeline.

The pipeline includes:

1. AWS S3 extraction
2. File existence validation and metadata logging
3. Local download of validated CSV files
4. Upload to a Databricks Unity Catalog Volume
5. Bronze, Silver, and Gold processing in Databricks
6. SQL analytics execution
7. Power BI reporting
8. End-to-end orchestration with Apache Airflow

---

## 1. Prerequisites

Before running the pipeline, confirm that the following tools are available:

- Python 3
- AWS account with access to the AdventureWorks S3 bucket
- Databricks workspace
- Databricks personal access token
- Apache Airflow
- Power BI Desktop
- Required Python packages

Install the ingestion dependencies:

```bash
pip install -r requirements.txt
```

Install the Airflow dependencies inside the Airflow virtual environment:

```bash
pip install -r requirements-airflow.txt
```

---

## 2. Environment Configuration

The ingestion scripts require AWS and Databricks credentials.

Create a `.env` file or configure the required environment variables.

Example:

```env
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=your_aws_region
S3_BUCKET_NAME=your_bucket_name

DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token
```

> **Important:** Never commit your `.env` file or credentials to GitHub.

---

## 3. Run the Local Ingestion Pipeline

From the project directory, run:

```bash
python3 run_pipeline.py
```

The local pipeline executes the following workflow:

```text
AWS S3
   │
   ▼
Validate expected files
   │
   ▼
Download validated CSV files
   │
   ▼
Upload files to Databricks Unity Catalog Volume
```

The pipeline executes the following modules:

```text
scripts.s3_extractor
scripts.upload_to_databricks_volume
```

If the S3 extraction step fails, the upload step will not be executed.

The pipeline returns:

- **Exit code 0** when the execution succeeds.
- **A non-zero exit code** if any pipeline step fails.

---

## 4. Local Output

Downloaded CSV files are stored in:

```text
data/raw/
```

The folder structure preserves the original S3 organization.

Example:

```text
data/raw/
├── customer/
│   └── Customer.csv
├── product/
│   └── Product.csv
├── sales_order_header/
│   └── SalesOrderHeader.csv
└── sales_order_detail/
    └── SalesOrderDetail.csv
```

Pipeline logs are written to:

```text
logs/etl.log
```

---

## 5. Databricks Processing

After the validated files are uploaded to the Databricks Unity Catalog Volume, Databricks executes the following stages:

```text
Databricks Volume
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer
        │
        ▼
SQL Analytics
```

### Bronze Layer

The Bronze layer:

- Reads the uploaded CSV files.
- Preserves the raw data.
- Adds processing metadata.
- Stores the data as Delta tables.

### Silver Layer

The Silver layer:

- Cleans and standardizes the data.
- Validates required columns.
- Handles missing values.
- Removes duplicate records.
- Applies data quality rules.
- Stores validated Delta tables.

### Gold Layer

The Gold layer builds the dimensional model consisting of:

- `dim_customer`
- `dim_product`
- `dim_salesperson`
- `dim_territory`
- `dim_date`
- `fact_sales`

The Gold process also validates:

- Null dimension keys.
- Duplicate dimension keys.
- Invalid fact records.
- Missing foreign-key relationships.
- Non-positive quantities.
- Negative sales values.

### SQL Analytics

The SQL Analytics stage executes the business queries stored in:

```text
databricks/sql_analytics/
```

These queries generate business-ready datasets for Power BI reporting.

---

## 6. Execute with Apache Airflow

Apache Airflow orchestrates the complete workflow and manages task dependencies.

Start Airflow:

```bash
airflow standalone
```

Open the Airflow web interface:

```text
http://localhost:8080
```

Locate the **AdventureWorks** DAG, unpause it, and trigger a new run.

The execution order is:

```text
Local Ingestion
      │
      ▼
Bronze Processing
      │
      ▼
Silver Processing
      │
      ▼
Gold Processing
      │
      ▼
SQL Analytics
```

Each task executes only after the previous task completes successfully.

---

## 7. Monitor Pipeline Execution

Pipeline execution can be monitored through:

- Airflow Grid View
- Airflow Graph View
- Airflow task logs
- Local Python logs
- Databricks job output
- Databricks validation results

A successful Airflow execution should show every task in the **Success** state.

If a task fails:

1. Open the failed Airflow task.
2. Review the task log.
3. Identify the failing module or Databricks stage.
4. Correct the issue.
5. Clear or rerun the failed task.

---

## 8. Validate the Results

After a successful execution, verify that the Gold tables were created:

```text
workspace.gold.dim_customer
workspace.gold.dim_product
workspace.gold.dim_salesperson
workspace.gold.dim_territory
workspace.gold.dim_date
workspace.gold.fact_sales
```

You can verify them with SQL:

```sql
SHOW TABLES IN workspace.gold;
```

Validate the fact table row count:

```sql
SELECT COUNT(*)
FROM workspace.gold.fact_sales;
```

Example referential integrity validation:

```sql
SELECT COUNT(*)
FROM workspace.gold.fact_sales AS f
LEFT JOIN workspace.gold.dim_product AS p
    ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL;
```

Expected result:

```text
0
```

---

## 9. Power BI Refresh

After the Gold layer and SQL Analytics complete:

1. Open `AdventureWorksDashboard.pbix`.
2. Confirm the Databricks connection.
3. Refresh the dataset.
4. Verify that all report pages load successfully.
5. Review the dashboards and business metrics.

---

## 10. Successful Execution Criteria

The pipeline is considered successful when:

- All required S3 files are found.
- All validated files are downloaded.
- All files are uploaded to the Databricks Unity Catalog Volume.
- Bronze, Silver, and Gold tables are created successfully.
- Data quality validations pass.
- Referential integrity validations return zero invalid records.
- SQL Analytics queries execute successfully.
- All Airflow tasks complete successfully.
- The Power BI dashboard refreshes without errors.

---

## 11. Failure Behavior

The pipeline is designed to stop whenever a critical stage fails.

Examples include:

- Missing required S3 files.
- AWS authentication failures.
- Databricks authentication failures.
- Empty uploaded files.
- Invalid dimension keys.
- Duplicate dimension records.
- Missing foreign-key relationships.
- Databricks job failures.

When a local module fails, `run_pipeline.py` returns a non-zero exit code. Airflow detects the failure and prevents downstream tasks from executing.

---

## 12. Execution Summary

```text
AdventureWorks CSV Files
        │
        ▼
AWS S3
        │
        ▼
Python Data Ingestion
        │
        ├── File existence validation
        ├── Metadata logging
        ├── CSV download
        └── Upload to Databricks Unity Catalog Volume
        │
        ▼
Databricks Bronze
        │
        ▼
Databricks Silver
        │
        ▼
Databricks Gold (Star Schema)
        │
        ▼
SQL Analytics
        │
        ▼
Power BI Dashboard
```

Apache Airflow controls the execution order, task dependencies, monitoring, retries, and logging for the complete pipeline.
