# AdventureWorks End-to-End Data Engineering Pipeline

## Overview

This project demonstrates the design and implementation of a production-inspired end-to-end data engineering pipeline using the AdventureWorks dataset. The pipeline extracts raw data from AWS S3, validates and uploads it to Databricks, processes it through a Medallion Architecture (Bronze, Silver, Gold), automates the entire workflow with Apache Airflow, and delivers business-ready insights through advanced SQL analytics and Power BI dashboards.

---

## Architecture

```
AdventureWorks CSV Files
        │
        ▼
AWS S3 (Raw Layer)
        │
        ▼
Python ETL
• File Validation
• Metadata Validation
• Logging
        │
        ▼
Databricks Unity Catalog Volume
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

| Category | Technologies |
|----------|--------------|
| Programming | Python |
| Cloud Storage | AWS S3 |
| Data Platform | Databricks |
| Processing | PySpark, Delta Lake |
| Orchestration | Apache Airflow |
| Analytics | SQL |
| Visualization | Power BI |
| Version Control | Git & GitHub |

---

## Project Features

- Automated end-to-end ETL pipeline
- AWS S3 data ingestion with validation and logging
- Upload of validated files to Databricks Unity Catalog Volumes
- Bronze, Silver, and Gold Medallion Architecture
- Data cleaning, standardization, and quality validation
- Referential integrity checks across tables
- Star Schema dimensional model for analytics
- Automated pipeline orchestration with Apache Airflow
- Business intelligence dashboard in Power BI
- 15 advanced SQL analytical solutions

---

## SQL Business Analytics

The Gold layer supports advanced business analytics through SQL views that answer questions such as:

- Monthly sales trends and month-over-month growth
- Product category revenue contribution
- Top customers by lifetime value
- Customer inactivity and churn risk
- Top-performing products
- Frequently purchased product combinations
- Territory and salesperson performance
- Seasonal and weekday sales patterns
- Customer value migration
- Complete RFM customer segmentation

Advanced SQL techniques used include CTEs, Window Functions, LAG(), LEAD(), DENSE_RANK(), NTILE(), CASE expressions, Self Joins, Subqueries, and Aggregations.

---

## Pipeline Workflow

1. Extract AdventureWorks data from AWS S3
2. Validate files and metadata
3. Upload validated files to Databricks Unity Catalog Volumes
4. Load raw data into the Bronze Layer
5. Clean and validate data in the Silver Layer
6. Build a Star Schema in the Gold Layer
7. Execute SQL analytical views
8. Visualize business insights with Power BI

The complete workflow is automatically orchestrated by Apache Airflow, providing task dependency management, execution monitoring, and logging.

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

- CI/CD pipeline with GitHub Actions
- Infrastructure as Code (Terraform)
- Incremental data loading
- Automated testing
- Data quality monitoring
- Docker containerization
