import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def standardize_case(
    df: pd.DataFrame,
    columns: list[str],
    case_type: str = "title"
) -> pd.DataFrame:
    """
    Standardizes string casing for specified columns.

    Args:
        df (pd.DataFrame): Input DataFrame
        columns (list[str]): Columns to standardize
        case_type (str): One of ["lower", "upper", "title"]

    Returns:
        pd.DataFrame: DataFrame with standardized columns
    """

    logger.info(f"Starting case standardization | Case type: {case_type}")

    valid_case_types = {"lower", "upper", "title"}

    if case_type not in valid_case_types:
        logger.error(f"Invalid case_type: {case_type}")
        raise ValueError(
            f"case_type must be one of {valid_case_types}"
        )

    df_copy = df.copy()

    for col in columns:
        if col not in df_copy.columns:
            logger.warning(f"Column not found in DataFrame: {col}")
            continue

        if not pd.api.types.is_string_dtype(df_copy[col]):
            logger.warning(f"Column is not string type, skipping: {col}")
            continue

        logger.info(f"Standardizing column: {col}")

        df_copy[col] = (
            df_copy[col]
            .astype(str)
            .str.strip()
        )

        if case_type == "lower":
            df_copy[col] = df_copy[col].str.lower()
        elif case_type == "upper":
            df_copy[col] = df_copy[col].str.upper()
        elif case_type == "title":
            df_copy[col] = df_copy[col].str.title()

    logger.info("Case standardization completed")

    return df_copy
