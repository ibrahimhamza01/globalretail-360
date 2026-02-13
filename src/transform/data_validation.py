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


def validate_column_types(
    df: pd.DataFrame,
    expected_dtypes: dict[str, str]
) -> None:
    """
    Validates that columns match expected pandas dtypes.

    Args:
        expected_dtypes: dict like {"Sales": "float64"}
    """

    logger.info("Validating column data types")

    for col, expected_type in expected_dtypes.items():

        if col not in df.columns:
            logger.warning(f"Column not found for dtype validation: {col}")
            continue

        actual_type = str(df[col].dtype)

        if actual_type != expected_type:
            logger.error(
                f"Column {col} has dtype {actual_type}, "
                f"expected {expected_type}"
            )
            raise TypeError(
                f"Column {col} has dtype {actual_type}, "
                f"expected {expected_type}"
            )

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
