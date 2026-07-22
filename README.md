# AdventureWorks End-to-End Data Engineering Pipeline

## Overview

This project demonstrates the design and implementation of a production-inspired end-to-end data engineering pipeline using the AdventureWorks dataset. Raw data is extracted from AWS S3, validated and uploaded to Databricks, processed through a Medallion Architecture (Bronze, Silver, Gold), orchestrated with Apache Airflow, and transformed into business-ready datasets for advanced SQL analytics and Power BI reporting.

---

## Architecture

```
AdventureWorks CSV Files
        │
        ▼
AWS S3 (Raw Layer)
        │
        ▼
Python Data Ingestion Layer
• AWS S3 Connection
• File Validation
• Metadata Validation
• Logging
• Download CSV Files
• Upload to Databricks Unity Catalog Volume
        │
        ▼
Databricks (PySpark)
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer (Star Schema)
        │
        ▼
SQL Analytics
        │
        ▼
Power BI Dashboard

Apache Airflow orchestrates the complete workflow.
```

---

## Technology Stack

- Python
- AWS S3
- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Apache Airflow
- SQL
- Power BI

---

## Project Features

- Automated end-to-end data pipeline
- AWS S3 data ingestion with file and metadata validation
- Upload of validated files to Databricks Unity Catalog Volumes
- Bronze, Silver, and Gold Medallion Architecture
- Data cleaning, standardization, and quality validation
- Referential integrity validation
- Star Schema dimensional model
- Automated workflow orchestration using Apache Airflow
- Interactive Power BI dashboard
- 15 advanced SQL analytical solutions

---

## SQL Business Analytics

The Gold layer supports advanced business analytics by answering real business questions, including:

- Monthly sales trends and month-over-month growth
- Product category revenue contribution
- Top customers by lifetime value
- Customer inactivity and churn risk
- Top-performing products
- Frequently purchased product combinations
- Territory and salesperson performance
- Seasonal sales trends
- Customer value migration
- RFM customer segmentation

Advanced SQL concepts used include:

- Common Table Expressions (CTEs)
- Window Functions
- LAG() and LEAD()
- DENSE_RANK()
- NTILE()
- CASE Expressions
- Self Joins
- Subqueries
- Aggregations
- Date Functions

---

## Pipeline Workflow

1. Extract AdventureWorks CSV files from AWS S3
2. Validate files and metadata
3. Upload validated files to Databricks Unity Catalog Volumes
4. Load raw data into the Bronze Layer
5. Clean and validate data in the Silver Layer
6. Build a Star Schema in the Gold Layer
7. Execute SQL analytical views
8. Visualize business insights in Power BI

The complete workflow is orchestrated with Apache Airflow, providing automated execution, task dependency management, monitoring, and logging.

---

## Repository Structure

```
architecture/
airflow/
etl/
databricks/
powerbi/
docs/
README.md
requirements.txt
```

---

## Future Improvements

- CI/CD with GitHub Actions
- Infrastructure as Code (Terraform)
- Incremental data loading
- Automated testing
- Data quality monitoring
- Docker containerization
