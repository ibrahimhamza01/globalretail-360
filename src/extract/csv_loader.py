import pandas as pd
from pathlib import Path

from src.utils.config import RAW_DATA_DIR
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_csv(file_path: Path, dataset_name: str) -> pd.DataFrame:
    """
    Internal helper to load a CSV file with logging and basic checks.
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
    """Load orders table with all order columns. Customer info remains as Customer ID only."""
    file_path = RAW_DATA_DIR / "orders.csv"
    df = _load_csv(file_path, "Orders")

    df["Order ID"] = df["Order ID"].astype(str)
    df["Customer ID"] = df["Customer ID"].astype(str)

    return df


def load_customers() -> pd.DataFrame:
    """Extract unique customers from orders.csv."""
    file_path = RAW_DATA_DIR / "orders.csv"
    df = _load_csv(file_path, "Orders")

    customers = df[
        ["Customer ID", "Customer Name", "Segment", "City", "State", "Region", "Postal Code", "Country"]
    ].drop_duplicates(subset=["Customer ID"])

    # Ensure Customer ID and Postal Code are strings
    customers["Customer ID"] = customers["Customer ID"].astype(str)
    customers["Postal Code"] = customers["Postal Code"].astype(str)

    return customers


def load_returns() -> pd.DataFrame:
    """Load returns dataset from returns.csv."""
    file_path = RAW_DATA_DIR / "returns.csv"
    df = _load_csv(file_path, "Returns")

    # Ensure correct types
    df["Order ID"] = df["Order ID"].astype(str)
    df["Returned"] = df["Returned"].astype(str)

    return df


def load_leads() -> pd.DataFrame:
    """
    Load leads dataset from people.csv.
    Splits 'Person' into first and last name robustly.
    """
    file_path = RAW_DATA_DIR / "people.csv"
    df = _load_csv(file_path, "People")

    if "Person" in df.columns:
        # Strip spaces and replace multiple spaces with single space
        df["Person"] = df["Person"].str.strip().str.replace(r"\s+", " ", regex=True)

        # Split by last space
        name_split = df["Person"].str.rsplit(" ", n=1, expand=True)
        df["First Name"] = name_split[0]
        df["Last Name"] = name_split[1].fillna("")
    else:
        df["First Name"] = ""
        df["Last Name"] = ""

    # Generate Lead ID
    df["Lead ID"] = [f"LEAD{i+1}" for i in range(len(df))]

    # Keep only needed columns
    columns_to_keep = ["Lead ID", "First Name", "Last Name"]
    if "Region" in df.columns:
        columns_to_keep.append("Region")

    return df[columns_to_keep]

