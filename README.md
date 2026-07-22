# AdventureWorks End-to-End Data Engineering Pipeline

## Overview

This project demonstrates a production-inspired end-to-end data engineering pipeline built with AWS, Python, Databricks, Apache Airflow, SQL, and Power BI. Raw AdventureWorks data is extracted from AWS S3, validated, processed through a Medallion Architecture (Bronze, Silver, Gold), and transformed into business-ready datasets for analytics and reporting.

---

## Architecture

```
AWS S3
   │
   ▼
Python ETL
(Data Validation & Logging)
   │
   ▼
Databricks Unity Catalog Volume
   │
   ▼
Bronze → Silver → Gold
   │
   ▼
SQL Analytics
   │
   ▼
Power BI Dashboard

(Apache Airflow orchestrates the complete workflow)
```

---

## Technology Stack

- Python
- AWS S3
- Databricks
- PySpark
- Delta Lake
- Apache Airflow
- SQL
- Power BI
- Git & GitHub

---

## Key Features

- Automated end-to-end ETL pipeline
- AWS S3 data ingestion and validation
- Medallion Architecture (Bronze, Silver, Gold)
- Data quality and referential integrity validations
- Star Schema dimensional model
- Apache Airflow orchestration
- Interactive Power BI dashboard
- 15 advanced SQL analytical solutions

---

## Business Analytics

The SQL layer answers real business questions using advanced SQL techniques, including:

- Monthly sales growth and MoM analysis
- Revenue contribution by product category
- Top customer lifetime value analysis
- Customer churn identification
- Top-performing products
- Product affinity analysis
- Territory and salesperson performance
- Seasonal sales trends
- RFM customer segmentation

Advanced SQL concepts used include:

- CTEs
- Window Functions
- LAG() / LEAD()
- DENSE_RANK()
- NTILE()
- CASE Expressions
- Self Joins
- Subqueries
- Aggregations
- Date Functions

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
- Incremental Data Loading
- Automated Testing
- Real-time Data Streaming
