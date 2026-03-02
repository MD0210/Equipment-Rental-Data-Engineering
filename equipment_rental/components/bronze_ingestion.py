# equipment_rental/components/bronze_ingestion.py
from datetime import datetime
import os
from equipment_rental.utils.common_utils import read_excel, read_csv, read_db_query, save_csv
from equipment_rental.constants.constants import BRONZE_DIR
from equipment_rental.logger.logger import get_logger
from pathlib import Path

logger = get_logger()

class BronzeIngestion:
    """
    Handles ingestion of data into the Bronze layer.
    Supports Excel sheets, CSVs, and database query sources.
    """

    def ingest_excel(self, file_path: str, sheet_name: str, pipeline_run_id: str = None):
        """
        Ingest data from an Excel sheet into Bronze.
        """
        try:
            df = read_excel(file_path, sheet_name=sheet_name)
            df["load_timestamp"] = datetime.now()
            df["source_file"] = os.path.basename(file_path)
            df["pipeline_run_id"] = pipeline_run_id

            output_name = f"{sheet_name}.csv"
            output_path = os.path.join(BRONZE_DIR, output_name)
            save_csv(df, output_path)

            logger.info(f"Bronze ingestion completed for Excel sheet: {sheet_name} | rows: {len(df)}")
            return df, output_path

        except Exception as e:
            logger.error(f"Bronze ingestion failed for Excel sheet '{sheet_name}': {str(e)}")
            raise

    def ingest_csv(self, file_path: str, pipeline_run_id: str = None):
        """
        Ingest data from a CSV file into Bronze.
        """
        try:
            df = read_csv(file_path)
            df["load_timestamp"] = datetime.now()
            df["source_file"] = os.path.basename(file_path)
            df["pipeline_run_id"] = pipeline_run_id

            output_name = os.path.basename(file_path)
            output_path = os.path.join(BRONZE_DIR, output_name)
            save_csv(df, output_path)

            logger.info(f"Bronze ingestion completed for CSV file: {output_name} | rows: {len(df)}")
            return df, output_path

        except Exception as e:
            logger.error(f"Bronze ingestion failed for CSV file '{file_path}': {str(e)}")
            raise

    def ingest_db(self, connection_str: str, query: str, table_name: str, pipeline_run_id: str = None):
        """
        Ingest data from a database query into Bronze.
        """
        try:
            df = read_db_query(connection_str, query)
            df["load_timestamp"] = datetime.now()
            df["source_query"] = query
            df["pipeline_run_id"] = pipeline_run_id

            output_name = f"{table_name}.csv"
            output_path = os.path.join(BRONZE_DIR, output_name)
            save_csv(df, output_path)

            logger.info(f"Bronze ingestion completed for database table: {table_name} | rows: {len(df)}")
            return df, output_path

        except Exception as e:
            logger.error(f"Bronze ingestion failed for database table '{table_name}': {str(e)}")
            raise