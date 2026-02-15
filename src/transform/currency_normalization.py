from datetime import datetime
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def normalize_exchange_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare exchange rates DataFrame for PostgreSQL.
    
    Adds a timestamp column to track when rates were fetched.
    
    Args:
        df (pd.DataFrame): Input DataFrame with columns ['currency', 'rate']
        
    Returns:
        pd.DataFrame: Normalized DataFrame with columns ['currency', 'rate', 'timestamp']
    """
    required_columns = {"currency", "rate"}
    if not required_columns.issubset(df.columns):
        logger.error(f"Exchange rates DataFrame missing required columns: {df.columns}")
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")
    
    logger.info("Normalizing exchange rates DataFrame")
    
    # Ensure currency codes are uppercase
    df["currency"] = df["currency"].str.upper()
    
    # Ensure rate is float
    df["rate"] = df["rate"].astype(float)
    
    # Add timestamp column
    df["timestamp"] = datetime.utcnow()
    
    # Remove duplicate currencies (keep the latest)
    df = df.drop_duplicates(subset=["currency"], keep="last")
    
    # Reset index
    df = df.reset_index(drop=True)
    
    logger.info(f"Exchange rates normalized | Rows: {len(df)}")
    
    return df
