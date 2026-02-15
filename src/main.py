from src.utils.logger import get_logger
from src.utils.config import ENV
from src.extract.csv_loader import load_orders, load_leads, load_customers, load_returns
from src.transform.case_standardizer import standardize_case
from src.transform.data_validation import (
    validate_required_columns,
    validate_column_types,
    validate_no_nulls
)
from src.load.postgres_loader import load_to_postgres
from datetime import datetime
from src.extract.api_loader import load_exchange_rates, load_fake_store_products
from sqlalchemy import String, Float, Integer, TIMESTAMP
import pandas as pd

logger = get_logger(__name__)


def etl_orders():
    """ETL pipeline for orders fact table (fact table only)."""
    logger.info("Starting ETL for Orders")

    # -----------------------
    # Extract
    # -----------------------
    orders = load_orders()

    # -----------------------
    # Transform
    # -----------------------
    # Standardize case for relevant columns
    orders = standardize_case(
        df=orders,
        columns=["City", "State", "Region"],  # Keep case standardization only for orders-relevant fields
        case_type="title"
    )

    # Drop customer dimension columns to avoid duplication
    customer_cols = ["Customer Name", "Segment", "City", "State", "Region", "Postal Code", "Country"]
    orders = orders.drop(columns=customer_cols, errors="ignore")

    # -----------------------
    # Validate
    # -----------------------
    validate_required_columns(
        orders,
        required_columns=["Order ID", "Sales", "Customer ID"]
    )

    expected_types = {
        "Order ID": "str",
        "Sales": "float64",
        "Quantity": "int64",
        "Profit": "float64",
        "Customer ID": "str"
    }

    validate_column_types(orders, expected_types)

    validate_no_nulls(
        orders,
        critical_columns=["Order ID", "Sales", "Customer ID"]
    )

    # -----------------------
    # Load
    # -----------------------
    load_to_postgres(
        df=orders,
        table_name="orders",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append"
    )

    logger.info("ETL for Orders completed successfully")


def etl_customers():
    """ETL pipeline for customers dimension table."""
    logger.info("Starting ETL for Customers")

    customers = load_customers()

    customers = standardize_case(
        df=customers,
        columns=["Customer Name", "City", "State", "Region"],
        case_type="title"
    )

    validate_required_columns(
        customers,
        required_columns=["Customer ID", "Customer Name"]
    )

    expected_types = {
        "Customer ID": "str",
        "Customer Name": "object",
        "Segment": "object",
        "City": "object",
        "State": "object",
        "Region": "object",
        "Postal Code": "object",
        "Country": "object"
    }
    validate_column_types(customers, expected_types)

    validate_no_nulls(
        customers,
        critical_columns=["Customer ID", "Customer Name"]
    )

    load_to_postgres(
        df=customers,
        table_name="customers",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append"
    )

    logger.info("ETL for Customers completed successfully")


def etl_leads():
    """ETL pipeline for leads from people.csv."""
    logger.info("Starting ETL for Leads")

    leads = load_leads()

    leads = standardize_case(
        df=leads,
        columns=["First Name", "Last Name"],
        case_type="title"
    )

    validate_required_columns(
        leads,
        required_columns=["Lead ID", "First Name", "Last Name"]
    )

    expected_types = {
        "Lead ID": "str",
        "First Name": "object",
        "Last Name": "object"
    }
    validate_column_types(leads, expected_types)

    load_to_postgres(
        df=leads,
        table_name="leads",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append"
    )

    logger.info("ETL for Leads completed successfully")

def etl_returns():
    """ETL pipeline for returns table."""
    from src.extract.csv_loader import load_returns

    logger.info("Starting ETL for Returns")

    # -----------------------
    # Extract
    # -----------------------
    returns = load_returns()

    # -----------------------
    # Validate
    # -----------------------
    validate_required_columns(
        returns,
        required_columns=["Order ID", "Returned"]
    )

    expected_types = {
        "Order ID": "str",
        "Returned": "object"
    }
    validate_column_types(returns, expected_types)

    validate_no_nulls(
        returns,
        critical_columns=["Order ID", "Returned"]
    )

    # -----------------------
    # Load
    # -----------------------
    load_to_postgres(
        df=returns,
        table_name="returns",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append"
    )

    logger.info("ETL for Returns completed successfully")

def etl_exchange_rates():
    """ETL pipeline for ExchangeRate API."""
    logger.info("Starting ETL for Exchange Rates")

    # -----------------------
    # Extract
    # -----------------------
    rates_df = load_exchange_rates()

    # -----------------------
    # Transform
    # -----------------------

    # Add timestamp
    rates_df["timestamp"] = pd.Timestamp.now(tz=None)

    # -----------------------
    # Validate
    # -----------------------
    validate_required_columns(rates_df, ["currency", "rate", "timestamp"])

    expected_types = {
        "currency": "object",
        "rate": "float64",
        "timestamp": "datetime64[ns]"
    }
    validate_column_types(rates_df, expected_types)
    validate_no_nulls(rates_df, ["currency", "rate"])

    # -----------------------
    # Load
    # -----------------------
    dtype_map = {
        "currency": String,
        "rate": Float,
        "timestamp": TIMESTAMP
    }
    load_to_postgres(
        df=rates_df,
        table_name="exchange_rates",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append",
        primary_key="currency",
        dtype_map=dtype_map
    )

    logger.info("ETL for Exchange Rates completed successfully")


def etl_fake_store_products():
    """ETL pipeline for Fake Store API products."""
    logger.info("Starting ETL for Fake Store Products")

    # -----------------------
    # Extract
    # -----------------------
    products_df = load_fake_store_products()

    # -----------------------
    # Transform
    # -----------------------
    # Separate rating dict into two columns: rating_rate, rating_count
    if "rating" in products_df.columns:
        products_df["rating_rate"] = products_df["rating"].apply(lambda x: x.get("rate") if isinstance(x, dict) else None)
        products_df["rating_count"] = products_df["rating"].apply(lambda x: x.get("count") if isinstance(x, dict) else None)
        products_df = products_df.drop(columns=["rating"])

    # -----------------------
    # Validate
    # -----------------------
    required_cols = ["id", "title", "price", "category", "rating_rate", "rating_count"]
    validate_required_columns(products_df, required_cols)

    expected_types = {
        "id": "int64",
        "title": "object",
        "price": "float64",
        "category": "object",
        "rating_rate": "float64",
        "rating_count": "int64"
    }
    validate_column_types(products_df, expected_types)
    validate_no_nulls(products_df, ["id", "title", "price"])

    # -----------------------
    # Load
    # -----------------------
    dtype_map = {
        "id": Integer,
        "title": String,
        "price": Float,
        "description": String,
        "category": String,
        "image": String,
        "rating_rate": Float,
        "rating_count": Integer
    }

    load_to_postgres(
        df=products_df,
        table_name="fake_store_products",
        schema="public",
        if_exists="replace" if ENV == "dev" else "append",
        primary_key="id",
        dtype_map=dtype_map
    )

    logger.info("ETL for Fake Store Products completed successfully")

def main():
    logger.info(f"Starting GlobalRetail 360 ETL pipeline | ENV={ENV}")

    etl_orders()
    etl_returns()
    etl_customers()
    etl_leads()

    etl_exchange_rates()
    etl_fake_store_products()

    logger.info("All ETL processes completed successfully")



if __name__ == "__main__":
    main()