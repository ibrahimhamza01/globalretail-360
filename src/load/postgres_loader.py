import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy.engine import Engine
from src.utils.config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from src.utils.logger import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


def _create_engine() -> Engine:
    """
    Creates a SQLAlchemy engine for PostgreSQL.
    """
    logger.info("Creating PostgreSQL engine")
    connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(connection_string)
    logger.info("PostgreSQL engine created successfully")
    return engine


def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: str = "replace",
    primary_key: str = None,
    dtype_map: dict = None
) -> None:
    """
    Loads a DataFrame into PostgreSQL with optional primary key and type mapping.

    Args:
        df (pd.DataFrame): DataFrame to load
        table_name (str): Target table name
        schema (str): Target schema
        if_exists (str): 'replace', 'append', or 'fail'
        primary_key (str): Column to set as primary key (for new tables)
        dtype_map (dict): Optional dict {col_name: sqlalchemy_type} for type enforcement
    """
    logger.info(f"Starting load to PostgreSQL | Table: {schema}.{table_name} | Mode: {if_exists}")
    engine = _create_engine()

    try:
        # Apply SQLAlchemy types if provided
        sql_dtype = dtype_map if dtype_map else None

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
            dtype=sql_dtype
        )

        if primary_key and if_exists != "append":
            # Set primary key if table is created or replaced
            with engine.begin() as conn:
                logger.info(f"Setting primary key on {primary_key} for table {table_name}")
                conn.execute(text(f"""
                                  ALTER TABLE {schema}.{table_name}
                                  ADD PRIMARY KEY ({primary_key});
                                  """))

        logger.info(f"Successfully loaded {df.shape[0]} rows into {schema}.{table_name}")

    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise

    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed")