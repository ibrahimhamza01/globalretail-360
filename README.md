# GlobalRetail 360

## An End-to-End Enterprise Analytics & ML System

### Project Overview

GlobalRetail 360 is a production-oriented analytics and machine learning system designed to demonstrate end-to-end data capabilities across data engineering, analytics, applied statistics, and deployable machine learning.

This project is intentionally scoped to reflect realistic enterprise workflows rather than overengineered solutions.

### Target Roles

- Data Analyst
- Analytics Engineer
- Applied Data Scientist
- Entry-to-mid Machine Learning Engineer

### System Architecture

The system follows a layered architecture:

**ETL → Analytics Warehouse → Statistical Analysis → Machine Learning → API → BI Dashboards**

Design decisions emphasize:

- Statistical validity
- Interpretability
- Reproducibility
- Clear separation of concerns

### Explicit Non-Goals

To maintain focus and realism, the project intentionally excludes:

- Real-time streaming systems (Kafka, Spark Streaming)
- Deep learning or neural networks
- Fully managed feature stores
- Kubernetes-level orchestration

## Data Sources

### Internal Data (Statistically Valid)

These datasets are the primary source for statistical testing and machine learning. All files are CSVs generated from the original Excel files and are now ingested into PostgreSQL via the ETL pipeline:

- `data/raw/global_superstore/orders.csv` — Order-level transaction data  
- `data/raw/global_superstore/returns.csv` — Order return information  
- `data/raw/global_superstore/people.csv` — Lead information  
- `data/raw/global_superstore/orders.csv` → customers table — Customer demographic and account data derived from orders  

**Initial Data Assessment (IDA)** has been performed on all internal CSVs to ensure data quality and readiness for analysis:

- **orders.csv** – 51,290 rows, 24 columns. Clean dataset; Postal Code partially missing. Ready for analysis.  
- **returns.csv** – 2,033 rows, 3 columns. No missing values. Can serve as a target for return prediction tasks.  
- **customers table** – 17,415 unique customers extracted from orders.csv. All critical columns complete; ready for enrichment and analysis.  
- **leads table** – 24 rows from people.csv. Names have been split into First Name and Last Name with robust parsing. Ready for analysis and enrichment.  

All internal datasets are loaded into PostgreSQL tables:
    - orders
    - returns
    - customers
    - leads
    - exchange_rates (166 currencies, timestamped)

### External Data (Enrichment Only)

- Exchange rates from external API are fetched and converted to `datetime64[ns]` for compatibility
- External datasets are used strictly for feature enrichment and scenario analysis; excluded from statistical inference

## Data Engineering (ETL)

A modular Python-based ETL pipeline is fully implemented and operational:

- Ingests multi-source CSV data: orders.csv, returns.csv, people.csv
- Fetches external data from APIs (exchange rates, synthetic competitor data)
- Standardizes schemas and case formatting for key columns
- Enforces data quality checks:
    - Required columns
    - Data types (including datetime handling for timestamps)
    - No nulls in critical columns
- Loads analytics-ready data into PostgreSQL tables:
    - orders (fact table)
    - returns (fact table)
    - customers (dimension)
    - leads (dimension)
    - exchange_rates (dimension)
- Idempotent and re-runnable, designed to prevent duplication and schema conflicts
- Primary keys are enforced at load-time using SQLAlchemy text statements

## Data Quality & Validation

- Required columns are checked for existence
- Data types validated (with flexibility for object/string and datetime handling)
- Null values prevented in critical columns
- API-derived datasets (e.g., exchange rates) now correctly standardized for datetime and float types
- Errors during ETL are logged, preventing corrupt data from loading into PostgreSQL

## Data Modeling

The analytics warehouse uses a **star schema** with:

- A central sales fact table  
- Customer, product, geography, and date dimensions  

PostgreSQL is used to demonstrate full-stack ownership and SQL proficiency. The design is directly transferable to cloud warehouses such as Snowflake.

## Analytics & Statistics

All analysis is **hypothesis-driven** and includes:

- Explicit assumption checks  
- Appropriate statistical tests  
- Effect sizes  
- Business interpretation of results  

### Initial Data Assessment (IDA) Summary

- Verified column types and data completeness.  
- Checked uniqueness of identifiers (Order ID, Customer ID).  
- Reviewed basic distributions and value counts for categorical features (e.g., Segment, Region, Returned).  
- Summarized numeric distributions (Sales, Profit, Quantity, etc.).  
- Ensured no data inconsistencies before proceeding to deeper EDA, feature engineering, and statistical tests.

### Initial Hypotheses

The project focuses on **four high-impact hypotheses** that combine multiple datasets, statistical tests, and business interpretation:

1. **The Discount Trap (Stats + Cross-Dataset)**  
   **Hypothesis:** Orders with discounts >20% have a statistically higher return rate than orders with <10% discounts.  
   **Business Value:** Evaluates the real cost of aggressive discounting, combining `orders.csv` and `returns.csv`, uncovering whether high discounts attract “low-quality” returns.

2. **Shipping Leakage (Unit Economics / Business Logic)**  
   **Hypothesis:** “Critical” priority orders shipped via First Class in the Consumer segment yield negative net profit margins ≥40% of the time.  
   **Business Value:** Identifies fulfillment inefficiencies and unit economics problems, highlighting where the company loses money on high-cost orders.

3. **Segment Value (Customer Modeling / Churn)**  
   **Hypothesis:** The Corporate segment has a ≥20% higher 12-month retention rate and lower return rate than the Consumer segment.  
   **Business Value:** Validates segment-level customer lifetime value (LTV) assumptions, informing marketing spend and acquisition strategy. Requires building a dim_customers table and retention metrics.

4. **Regional Efficiency (Logistics Analysis / ANOVA)**  
   **Hypothesis:** Product return rates are driven more by regional logistics (shipping mode, region) than by product category.  
   **Business Value:** Determines whether returns are operational vs. product-related, guiding inventory and shipping optimization strategies.

## Machine Learning

Machine learning is used selectively for:

- Customer churn prediction  
- Sales forecasting  

Model selection prioritizes **interpretability** and **business impact** over raw accuracy.

## Production & MLOps

Models are exposed via **FastAPI** and tracked using **MLflow**.  
Docker is used to ensure reproducibility across environments.

## Deployment

- The ETL pipeline fully automates ingestion, validation, and database loading
- Tables are created or replaced with enforced primary keys, avoiding duplication
- System is reproducible locally using Python, PostgreSQL, and Docker

## BI & Storytelling

Power BI dashboards communicate insights to executive and analytical audiences, maintaining a clear separation between operational systems and decision support.

## Reproducibility

The entire system can be reproduced locally using Docker with documented setup steps.

## Roadmap and Non-Goals

Future enhancements are documented explicitly while maintaining clear boundaries to avoid overengineering.

### Next Steps

- Perform detailed EDA and feature engineering for all internal CSVs.  
- Develop statistical analyses and hypothesis testing based on the **four high-impact hypotheses**.  
- Build interpretable machine learning models.
