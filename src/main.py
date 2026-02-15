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


def main():
    logger.info(f"Starting GlobalRetail 360 ETL pipeline | ENV={ENV}")

    etl_orders()
    etl_returns()
    etl_customers()
    etl_leads()

    logger.info("All ETL processes completed successfully")



if __name__ == "__main__":
    main()