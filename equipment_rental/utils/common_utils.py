import pandas as pd
import sqlalchemy
from typing import Optional
from equipment_rental.logger.logger import get_logger

logger = get_logger()

def read_excel(file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Reads an Excel sheet into a pandas DataFrame.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file '{file_path}', sheet '{sheet_name}': {str(e)}")
        raise

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV file '{file_path}': {str(e)}")
        raise

def save_csv(df: pd.DataFrame, path: str):
    """
    Saves a DataFrame to CSV safely.
    """
    try:
        df.to_csv(path, index=False)
        logger.info(f"Saved CSV to '{path}'")
    except Exception as e:
        logger.error(f"Failed to save CSV to '{path}': {str(e)}")
        raise

def read_db_query(connection_str: str, query: str, **kwargs) -> pd.DataFrame:
    """
    Reads data from a database using a SQL query.
    connection_str: SQLAlchemy connection string
    query: SQL query to execute
    kwargs: optional pandas.read_sql parameters
    """
    try:
        engine = sqlalchemy.create_engine(connection_str)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, **kwargs)
        return df
    except Exception as e:
        logger.error(f"Failed to execute DB query: {str(e)}")
        raise