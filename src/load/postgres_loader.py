import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.utils.config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _create_engine() -> Engine:
    """
    Creates a SQLAlchemy engine for PostgreSQL.
    """

    logger.info("Creating PostgreSQL engine")

    connection_string = (
        f"postgresql://{POSTGRES_USER}:"
        f"{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/"
        f"{POSTGRES_DB}"
    )

    engine = create_engine(connection_string)

    logger.info("PostgreSQL engine created successfully")

    return engine


def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: str = "replace"
) -> None:
    """
    Loads a DataFrame into PostgreSQL.

    Args:
        df (pd.DataFrame): DataFrame to load
        table_name (str): Target table name
        schema (str): Target schema
        if_exists (str): 'replace', 'append', or 'fail'
    """

    logger.info(
        f"Starting load to PostgreSQL | "
        f"Table: {schema}.{table_name} | "
        f"Mode: {if_exists}"
    )

    engine = _create_engine()

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000
        )

        logger.info(
            f"Successfully loaded {df.shape[0]} rows into "
            f"{schema}.{table_name}"
        )

    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed")
