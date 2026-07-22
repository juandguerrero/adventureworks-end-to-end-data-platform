# AdventureWorks End-to-End Data Engineering Pipeline

## Overview

This project demonstrates the design and implementation of a complete end-to-end data engineering pipeline using the AdventureWorks dataset. The pipeline extracts raw data from AWS S3, performs data validation, processes the data through a Medallion Architecture in Databricks, automates the workflow with Apache Airflow, and delivers business insights through SQL analytics and Power BI dashboards.

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
• Data Validation
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
Gold Layer
        │
        ▼
SQL Analytics Views
        │
        ▼
Power BI Dashboard
```

The complete workflow is orchestrated using **Apache Airflow**, allowing the pipeline to run automatically from data ingestion through analytics.

---

## Technologies

- Python
- AWS S3
- boto3
- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Apache Airflow
- SQL
- Power BI
- Git

---

## Project Features

- Extracts raw AdventureWorks CSV files from AWS S3
- Validates file structure and metadata before processing
- Uploads validated files into Databricks Unity Catalog Volumes
- Implements a complete Bronze, Silver, and Gold Medallion Architecture
- Performs data quality and referential integrity validations
- Builds a dimensional Star Schema for analytics
- Creates business-ready SQL views
- Automates the entire pipeline using Apache Airflow
- Visualizes insights with an interactive Power BI dashboard

---

## Data Model

The Gold layer contains a dimensional model composed of:

- FactSales
- DimCustomer
- DimProduct
- DimDate
- DimTerritory
- DimSalesperson

This model is optimized for analytical queries and reporting.

---

## SQL Analytics

The project includes 15 business-oriented SQL analyses, including:

- Monthly sales growth
- Revenue by product category
- Top customers by lifetime value
- Inactive customers
- Top-performing products
- Sales trends
- Customer segmentation
- Revenue analysis

---

## Power BI Dashboard

The Power BI dashboard provides interactive visualizations including:

- Monthly Sales Trend
- Month-over-Month Growth
- Sales by Category
- Sales by Season
- Sales by Weekday
- Additional KPI dashboards

---

## Airflow Automation

The pipeline is fully orchestrated with Apache Airflow.

Workflow:

1. Extract data from AWS S3
2. Upload validated files to Databricks
3. Execute Bronze transformation
4. Execute Silver transformation
5. Execute Gold transformation
6. Execute SQL Analytics

Each task is monitored independently with automatic status tracking and logging.

---

## Repository Structure

```
architecture/
airflow/
databricks/
sql/
powerbi/
docs/
etl/
README.md
requirements.txt
```

---

## Future Improvements

- CI/CD pipeline using GitHub Actions
- Infrastructure as Code with Terraform
- Automated testing
- Data quality monitoring
- Incremental data loading
