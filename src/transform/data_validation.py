import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list[str]
) -> None:
    """
    Ensures all required columns exist in the DataFrame.
    Raises error if any are missing.
    """

    logger.info("Validating required columns")

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    logger.info("Required column validation passed")


def validate_column_types(df: pd.DataFrame, expected_types: dict) -> None:
    """
    Validate column data types.
    expected_types: dict {column_name: dtype_as_string}
    """
    for col, expected in expected_types.items():
        actual_dtype = str(df[col].dtype)
        if expected == "object" and actual_dtype in ["object", "string", "str"]:
            continue  # accept str/object as valid
        if actual_dtype != expected:
            logger.error(f"Column {col} has dtype {actual_dtype}, expected {expected}")
            raise TypeError(f"Column {col} has dtype {actual_dtype}, expected {expected}")
    logger.info("Column dtype validation passed")


def validate_no_nulls(
    df: pd.DataFrame,
    critical_columns: list[str]
) -> None:
    """
    Ensures critical columns contain no null values.
    """

    logger.info("Validating null values in critical columns")

    for col in critical_columns:

        if col not in df.columns:
            logger.warning(f"Column not found for null validation: {col}")
            continue

        null_count = df[col].isnull().sum()

        if null_count > 0:
            logger.error(
                f"Column {col} contains {null_count} null values"
            )
            raise ValueError(
                f"Column {col} contains {null_count} null values"
            )

    logger.info("Null validation passed")

# -------------------------------
# API-Specific Validation Wrappers
# -------------------------------

def validate_exchange_rates(df: pd.DataFrame) -> None:
    """
    Validate exchange rates table.
    """
    logger.info("Validating Exchange Rates table")
    required_cols = ["currency", "rate", "timestamp"]
    validate_required_columns(df, required_cols)

    expected_types = {
        "currency": "object",
        "rate": "float64",
        "timestamp": "object",  # timestamp as string (ISO format)
    }
    validate_column_types(df, expected_types)
    validate_no_nulls(df, critical_columns=["currency", "rate"])
    logger.info("Exchange Rates validation passed")


def validate_fake_store_products(df: pd.DataFrame) -> None:
    """
    Validate Fake Store products table.
    """
    logger.info("Validating Fake Store products table")
    required_cols = ["id", "title", "price", "category", "rating_rate", "rating_count"]
    validate_required_columns(df, required_cols)

    expected_types = {
        "id": "int64",
        "title": "object",
        "price": "float64",
        "category": "object",
        "rating_rate": "float64",
        "rating_count": "int64"
    }
    validate_column_types(df, expected_types)
    validate_no_nulls(df, critical_columns=["id", "title", "price"])
    logger.info("Fake Store products validation passed")