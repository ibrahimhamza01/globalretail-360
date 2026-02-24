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
- `orders`
- `returns`
- `customers`
- `leads`
- `exchange_rates` (166 currencies, timestamped)

### External Data (Enrichment Only)

External APIs are used strictly for feature enrichment and scenario analysis; they are **excluded from statistical inference** to preserve analytical validity.

**Exchange Rates (API)**

- 166 currencies fetched relative to USD
- Columns: `currency` (string), `rate` (float), `timestamp` (datetime)
- No missing values detected
- Data standardized to correct `float64` and `datetime64[ns]` types for downstream ETL

**Fake Store Products (Synthetic API)**

- 20 products fetched with full details
- Columns: `id` (int), `title` (str), `price` (float), `description` (str), `category` (str), `image` (str), `rating` (object/dict)
- No missing values detected
- Basic statistics for price column:
    - Min: 7.95, Max: 999.99, Mean: 162.05, Median: 56.49
- Data ready for enrichment into products dimension or scenario modeling

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
The analytics warehouse has been built using PostgreSQL with a **star schema** design:

**Fact Table**
- `fact_sales` — contains order-level metrics:
    - order_id, customer_key, product_key, date_id, geography_key
    - sales, profit, discount, quantity, shipping_cost
    - return_flag, loss_flag

**Dimension Tables**
- `dim_customers` — surrogate key `customer_key`, customer demographics
- `dim_products` — surrogate key `product_key`, product details and average internal price
- `dim_fake_products` — synthetic product data for enrichment
- `dim_date` — timestamp and derived date features (year, quarter, month, weekday, weekend flag)
- `dim_geography` — unique locations (city, state, region, country)

**Notes**
- Surrogate keys were generated for all dimension tables.
- Fact table rows link to dimension tables using these surrogate keys.
- `return_flag` and `loss_flag` were computed from orders and returns tables.
- The design allows analytical queries to efficiently aggregate sales, profits, discounts, and returns across customers, products, dates, and geography.

### Advanced SQL Features in Warehouse

- All features are computed in SQL, not Pandas
- Scripts used:
    - `scripts/build_warehouse.sql` — builds star schema tables
    - `scripts/advanced_sql_features.sql` — computes rolling aggregates, CLV, and loss flags

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

## 1. The Discount Trap (Stats + Cross-Dataset)

**Hypothesis:** Orders with discounts >20% have a statistically higher return rate than orders with <10% discounts.  

**Business Value:** Evaluates the real cost of aggressive discounting, combining `orders.csv` and `returns.csv`, uncovering whether high discounts attract “low-quality” returns.

**Methodology:**
- Filtered data to focus on:
  - Low Discount (<10%)
  - High Discount (>20%)
- Excluded mid-range discounts (10–20%) to create a sharp comparison.
- Computed return rates:
  - High Discount: 13.34%
  - Low Discount: 11.28%
- Built contingency table of return counts by discount group.
- Statistical significance testing:
  - Chi-square test: χ² = 33.35, df = 1, p < 0.001
  - Effect size (Cramér’s V) = 0.028
- 95% confidence interval for difference in return rates: 2.06 percentage points (CI: 1.34% – 2.78%)

**Key Insights:**
- Difference in return rates is statistically significant, but effect size is small.
- Aggressive discounts slightly increase return rates, but practical impact is minor.
- Other operational or customer-related factors likely dominate returns.

**Takeaway for Decision-Makers:**
- Discounting alone is **not a major driver of returns**.
- Small adjustments in discount strategy could slightly optimize profitability.

## 2. Shipping Leakage (Unit Economics / Business Logic)

**Hypothesis:** “Critical” priority orders shipped via First Class in the Consumer segment yield negative net profit margins ≥40% of the time.  

**Business Value:** Identifies fulfillment inefficiencies and unit economics problems, highlighting where the company loses money on high-cost orders.

**Methodology:**  
- Joined data from:
  - `fact_sales` (profit, sales)
  - `orders` (`Order Priority`, `Ship Mode`)
  - `dim_customers` (`Segment`)
- Filtered for:
  - Customer Segment = Consumer
  - Order Priority = Critical
  - Shipping Mode = First Class
- Calculated **net profit margin per order**: `profit / sales`
- Computed proportion of orders with negative net margin.

**Results:**  
- **Proportion of negative net margin orders:** 25.94%  
- **Interpretation:** Hypothesis is **not supported**. Fewer than 40% of these high-priority orders are losing money, though a significant minority (~26%) still have negative margins.

**Takeaways for Decision-Makers:**  
- High-priority First Class shipping in the Consumer segment is **not as loss-inducing** as feared.  
- ~26% of orders with negative margins indicates some **opportunity to optimize fulfillment or pricing** for specific orders.  
- Efforts to reduce shipping costs or improve unit economics should target the **subset of orders with negative margins**.

## 3. Segment Value (Customer Modeling / Churn)

**Hypothesis:** The Corporate segment has a ≥20% higher 12-month retention rate and lower return rate than the Consumer segment.  

**Business Value:** Validates segment-level customer lifetime value (LTV) assumptions, informing marketing spend and acquisition strategy.

**Methodology:**
- Build `dim_customers` table with segment classification.
- Compute 12-month retention and return rates for each customer.
- Aggregate metrics at the segment level.
- Compare Corporate vs. Consumer segment using:
  - 12-month retention rate
  - Fraction of customers with at least one return
- Conduct Chi-square tests to check if differences are statistically significant.

**Key Insights & Takeaways:**
- **Retention:** Both segments have nearly identical 12-month retention — Consumer 43.7%, Corporate 43.6%. Chi-square test confirms no significant difference (p = 0.901).  
- **Returns:** Fraction of customers with at least one return is similar — Consumer 10.87%, Corporate 11.09%. Chi-square test confirms no significant difference (p = 0.708).  
- **Interpretation:** The hypothesis that Corporate customers have higher retention or lower return rates is **not supported** by the data. LTV assumptions based solely on retention and returns are not validated.  
- **Actionable Insight:** Marketing and retention strategies should not assume Corporate customers are inherently “stickier” or less likely to return products. Additional metrics (e.g., order frequency, average order value) should be considered for segment-specific targeting.

## 4. Regional Efficiency (Logistics Analysis / ANOVA)

**Hypothesis:** Product return rates are driven more by regional logistics (shipping mode, region) than by product category.  

**Business Value:** Determines whether returns are operational vs. product-related, guiding inventory and shipping optimization strategies.

**Methodology:**
- Analyze returns by:
  - Product category
  - Shipping mode
  - Region
- Aggregate returns data by region, shipping mode, and product category.
- Compute return rates per group.
- Perform ANOVA to assess contribution of region, shipping mode, and product category to return rates.

**Key Insights & Takeaways:**
- **Region:** Statistically significant effect on return rates (p ≈ 0.0046), indicating regional logistics are a key driver of returns.
- **Shipping Mode:** No significant effect on return rates (p ≈ 0.099), suggesting shipping method has limited influence.
- **Product Category:** No significant effect on return rates (p ≈ 0.813), implying returns are not product-driven.
- **Interpretation:** Returns are primarily driven by **regional logistics** rather than product type or shipping mode. Inventory and operational optimization should prioritize regional factors.

## Machine Learning – Customer Churn Prediction

Machine learning is used to predict which customers are likely to churn and to guide retention strategies. Models focus on **interpretability** and **business impact** rather than raw accuracy.

### Objective
- Identify customers at risk of churn within the next 12 months.
- Enable targeted retention campaigns to reduce churn costs and improve customer lifetime value.

### Modeling Approach
- **Problem Type:** Supervised binary classification (churn = 1, active = 0)  
- **Features:** Customer segment, region, total orders, total sales, average discount, total returns  
- **Evaluation Metrics:** Accuracy, ROC AUC, Recall per class (Non-Churn 0, Churn 1)  

### Models Trained
| Model | Handling Class Imbalance | Key Insight |
|-------|-------------------------|------------|
| Logistic Regression | None | High recall for churners (class 1); interpretable coefficients |
| Logistic Regression | SMOTE | Better recall for non-churners (class 0); slightly lower overall accuracy |
| Random Forest | None | Balanced recall (~0.45 for non-churners, ~0.71 for churners); top features: total_sales, total_orders |
| Random Forest | SMOTE | Improved non-churn recall, decreased overall performance; total_sales dominates importance |
| XGBoost | None | Similar to Random Forest; captures non-linear feature interactions; high churn recall |
| XGBoost | SMOTE | Slightly improved non-churn recall; overall ROC AUC slightly lower |

### Key Observations
- **Class Imbalance:** Original dataset (~56% churn, 44% non-churn) favors predicting churners; SMOTE balances classes to improve non-churn recall.  
- **Performance Trade-offs:** SMOTE improves recognition of non-churners but reduces overall accuracy and ROC AUC.  
- **Feature Importance & Business Insights:**  
  - High-risk churn drivers: low total_orders, low total_sales, high total_returns, certain regions (Caribbean, Central Africa).  
  - Protective factors: high engagement (total_orders), certain regions (Western US, Oceania), segment_Home Office.  
- **Recommendation:** Logistic Regression (no SMOTE) is a solid baseline for targeting likely churners. SMOTE-based models are useful when minimizing false positives for non-churners.

### Business Impact
- Retention campaigns can be prioritized for high-risk customers identified by the models.  
- Regional and segment-level patterns enable targeted marketing and loyalty programs.  
- Model interpretability allows stakeholders to understand key drivers of churn and make data-informed decisions.


## Production & MLOps

The GlobalRetail 360 churn prediction models are deployed as a **FastAPI** service, exposing endpoints for predictions and system health. This ensures reliable, scalable, and production-ready access to the models.

### Key Features

#### FastAPI API
- **`/predict`** — Accepts validated customer data and returns churn probabilities.
- **`/health`** — Returns `{"status": "ok"}` for health checks (used by Kubernetes, Docker, load balancers, and monitoring tools).
- Input validation via **Pydantic models** ensures only clean, structured data is processed.
- Automatic **HTTP 422 errors** for malformed or invalid input.
- Response schema (`PredictionResponse`) ensures consistent and well-documented JSON output.

#### Exception Handling & Validation
- Invalid categories or numeric anomalies trigger structured errors.
- Region and segment validation includes fallback options (`Other`) and prevents malicious or malformed input.

#### MLflow Tracking
- Model versioning, metrics, and artifacts tracked for reproducibility and experiment management.

#### Dockerized Deployment
- Ensures consistent environment across local, staging, and production systems.
- Simplifies CI/CD and cloud deployment.

#### Extensible Design
- Additional features or endpoints (e.g., new metrics, batch prediction) can be easily added.
- Pydantic models and response schemas improve maintainability and OpenAPI documentation.

### Example API Usage

**POST `/predict`**

```json
[
  {
    "segment": "Consumer",
    "region": "Central Us",
    "total_orders": 70,
    "total_sales": 330,
    "avg_discount": 0.5,
    "total_returns": 20
  }
]
```
**Response**
```json
[
    {
      "predictions": [0.0000468157438026401]
    }
]
```

## Deployment
- The ETL pipeline fully automates ingestion, validation, and database loading.
- Warehouse tables are built automatically at the end of the ETL.
- Run locally using: `python -m src.main`

## BI & Storytelling

Power BI dashboards communicate insights to executive and analytical audiences, maintaining a clear separation between operational systems and decision support.

## Reproducibility

The entire system can be reproduced locally using Docker with documented setup steps.

## Roadmap and Non-Goals

Future enhancements are documented explicitly while maintaining clear boundaries to avoid overengineering.

### Next Steps

- Perform detailed EDA and feature engineering for all internal CSVs and external enrichment datasets (exchange rates, Fake Store products)
- Conduct statistical analyses and hypothesis testing based on the four high-impact hypotheses:
    1. The Discount Trap
    2. Shipping Leakage
    3. Segment Value
    4. Regional Efficiency
- Build interpretable machine learning models for:
    - Customer churn prediction
    - Sales forecasting
- Extend BI dashboards to include insights from warehouse analytics and external enrichment features
- Deploy predictive models via FastAPI and monitor with MLflow
