# Data Model

## Overview

The Gold layer uses a Star Schema to organize the AdventureWorks data into a dimensional model optimized for analytical queries and Power BI reporting.

The model consists of one central fact table surrounded by multiple dimension tables. This design improves query performance, simplifies reporting, and supports business intelligence workloads.

---

## Star Schema

```text
                    Dim Customer
                          │
                          │
Dim Date ───────┐          │          ┌────── Dim Product
                │          │          │
                ▼          ▼          ▼
                +-------------------------+
                |       Fact Sales        |
                +-------------------------+
                ▲          ▲          ▲
                │          │          │
      Dim Territory      Dim Salesperson
```

---

## Fact Table

### fact_sales

The fact table stores transactional sales records and contains the numerical measures used for business analysis.

Primary measures include:

- Order Quantity
- Unit Price
- Unit Price Discount
- Line Total
- Subtotal
- Tax Amount
- Freight
- Total Due

Foreign keys:

- DateKey
- CustomerKey
- ProductKey
- TerritoryKey
- SalesPersonKey

---

## Dimension Tables

### dim_customer

Contains customer information used for customer-based analysis.

Example attributes:

- CustomerKey
- CustomerName
- AccountNumber
- TerritoryID

---

### dim_product

Contains product information.

Example attributes:

- ProductKey
- ProductName
- ProductNumber
- Category
- Subcategory
- Color
- Size
- StandardCost
- ListPrice

---

### dim_date

Provides calendar attributes for time-based analysis.

Example attributes:

- DateKey
- OrderDate
- Year
- Quarter
- Month
- MonthName
- Week
- Day
- WeekDay

---

### dim_salesperson

Contains sales representative information.

Example attributes:

- SalesPersonKey
- SalesPersonName
- TerritoryID
- SalesQuota
- Bonus
- CommissionPct
- SalesYTD
- SalesLastYear

---

### dim_territory

Contains geographical sales information.

Example attributes:

- TerritoryKey
- Territory
- CountryRegionCode
- Group

---

## Relationships

The fact table is linked to each dimension through surrogate keys.

| Fact Table | Dimension |
|------------|-----------|
| CustomerKey | dim_customer |
| ProductKey | dim_product |
| DateKey | dim_date |
| TerritoryKey | dim_territory |
| SalesPersonKey | dim_salesperson |

---

## Data Quality Validation

Before the Gold tables are published, several validation checks are performed, including:

- Duplicate key validation
- Null key validation
- Referential integrity validation
- Invalid quantity validation
- Negative sales validation

These checks ensure that the dimensional model is consistent and ready for business reporting.

---

## Business Benefits

The Star Schema enables:

- Faster analytical queries
- Simplified SQL development
- Efficient Power BI reporting
- Better aggregation performance
- Clear separation between facts and dimensions
