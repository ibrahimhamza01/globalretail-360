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
These datasets serve as the sole source for statistical testing and machine learning:
- Global Superstore sales data
- Returns data
- Customer data

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
