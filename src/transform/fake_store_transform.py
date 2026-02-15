import pandas as pd
from src.utils.logger import get_logger
from src.transform.case_standardizer import standardize_case

logger = get_logger(__name__)


def transform_fake_store_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Fake Store API product catalog for PostgreSQL load.

    - Splits 'rating' dictionary into 'rating_rate' and 'rating_count'
    - Standardizes text columns
    - Enforces data types
    - Drops duplicates

    Args:
        df (pd.DataFrame): Raw DataFrame from Fake Store API

    Returns:
        pd.DataFrame: Cleaned and transformed DataFrame
    """
    logger.info("Transforming Fake Store products")

    # Split rating dict
    if "rating" in df.columns:
        df["rating_rate"] = df["rating"].apply(lambda x: x.get("rate") if isinstance(x, dict) else None)
        df["rating_count"] = df["rating"].apply(lambda x: x.get("count") if isinstance(x, dict) else None)
        df = df.drop(columns=["rating"])
    
    # Standardize string columns
    text_cols = ["title", "category"]
    df = standardize_case(df, columns=text_cols, case_type="title")

    # Enforce types
    df["id"] = df["id"].astype(int)
    df["price"] = df["price"].astype(float)
    df["rating_rate"] = df["rating_rate"].astype(float)
    df["rating_count"] = df["rating_count"].astype(int)

    # Drop duplicates by product id
    df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

    logger.info(f"Fake Store products transformed | Rows: {len(df)}")
    return df
