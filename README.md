# GlobalRetail 360
An End-to-End Enterprise Analytics & ML System

## Project Overview
GlobalRetail 360 is a production-oriented analytics and machine learning system designed to demonstrate end-to-end data capabilities across data engineering, analytics, applied statistics, and deployable machine learning.

This project is intentionally scoped to reflect realistic enterprise workflows rather than overengineered solutions.

**Target roles**
- Data Analyst
- Analytics Engineer
- Applied Data Scientist
- Entry-to-mid Machine Learning Engineer

---

## System Architecture
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

---

## Data Sources

### Internal Data (Statistically Valid)
These datasets serve as the sole source for statistical testing and machine learning. All files are CSVs generated from the original Excel:

- `data/raw/global_superstore/orders.csv` — Order-level transaction data
- `data/raw/global_superstore/returns.csv` — Order return information
- `data/raw/global_superstore/customers.csv` — Customer demographic and account data

**Initial Data Assessment (IDA)** has been performed on all internal CSVs to ensure data quality and readiness for analysis:

- **orders.csv** – 51,290 rows, 24 columns. Clean dataset with only Postal Code partially missing. Ready for analysis.
- **returns.csv** – 2,033 rows, 3 columns. No missing values. Can serve as a target for return prediction tasks.
- **customers.csv** – 24 rows, 2 columns. Complete, no missing values, categorical only. Ready for enrichment and analysis.

### External Data (Enrichment Only)
External APIs are used strictly for feature enrichment and scenario analysis:
- Exchange rate data
- Synthetic competitor data

External data is **explicitly excluded** from statistical inference to preserve analytical validity.

---

## Data Engineering (ETL)
A modular Python-based ETL pipeline is used to:
- Ingest multi-source data
- Standardize schemas
- Enforce data quality checks
- Load analytics-ready data into PostgreSQL

The pipeline is designed to be idempotent and re-runnable.

---

## Data Modeling
The analytics warehouse uses a star schema with:
- A central sales fact table
- Customer, product, geography, and date dimensions

PostgreSQL is used to demonstrate full-stack ownership and SQL proficiency.  
The design is directly transferable to cloud warehouses such as Snowflake.

---

## Analytics & Statistics
All analysis is hypothesis-driven and includes:
- Explicit assumption checks
- Appropriate statistical tests
- Effect sizes
- Business interpretation of results

**Initial Data Assessment (IDA)** Summary:
- Verified column types and data completeness.
- Checked uniqueness of identifiers (Order ID, Customer ID).
- Reviewed basic distributions and value counts for categorical features (e.g., Segment, Region, Returned).
- Summarized numeric distributions (Sales, Profit, Quantity, etc.).
- Ensured no data inconsistencies before proceeding to deeper EDA, feature engineering, and statistical tests.

---

## Machine Learning
Machine learning is used selectively for:
- Customer churn prediction
- Sales forecasting

Model selection prioritizes interpretability and business impact over raw accuracy.

---

## Production & MLOps
Models are exposed via a FastAPI service and tracked using MLflow.  
Docker is used to ensure reproducibility across environments.

---

## Deployment
The system is designed to be deployed on lightweight cloud platforms with a public prediction endpoint.

---

## BI & Storytelling
Power BI dashboards are used to communicate insights to executive and analytical audiences, with clear separation between operational systems and decision support.

---

## Reproducibility
The entire system can be reproduced locally using Docker with documented setup steps.

---

## Roadmap and Non-Goals
Future enhancements are documented explicitly, while maintaining clear boundaries to avoid overengineering.  

**Next Steps:**
- Perform detailed EDA and feature engineering for all internal CSVs.
- Develop statistical analyses and hypothesis testing.
- Build interpretable machine learning models.
