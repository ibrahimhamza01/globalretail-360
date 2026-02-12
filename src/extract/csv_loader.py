import pandas as pd
from pathlib import Path

from src.utils.config import RAW_DATA_DIR
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_csv(file_path: Path, dataset_name: str) -> pd.DataFrame:
    """
    Internal helper to load a CSV file with logging and basic checks.

    Args:
        file_path (Path): Path to the CSV file
        dataset_name (str): Human-readable dataset name for logging

    Returns:
        pd.DataFrame
    """
    logger.info(f"Starting extraction for dataset: {dataset_name}")

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"Missing file: {file_path}")

    df = pd.read_csv(file_path)

    logger.info(
        f"Completed extraction for {dataset_name} | "
        f"Rows: {df.shape[0]} | Columns: {df.shape[1]}"
    )

    return df


def load_orders() -> pd.DataFrame:
    """
    Load orders dataset from raw CSV.
    """
    file_path = RAW_DATA_DIR / "orders.csv"
    return _load_csv(file_path, "Orders")


def load_returns() -> pd.DataFrame:
    """
    Load returns dataset from raw CSV.
    """
    file_path = RAW_DATA_DIR / "returns.csv"
    return _load_csv(file_path, "Returns")


def load_people() -> pd.DataFrame:
    """
    Load people dataset from raw CSV.
    """
    file_path = RAW_DATA_DIR / "people.csv"
    return _load_csv(file_path, "People")
