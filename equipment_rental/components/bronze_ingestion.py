from datetime import datetime
from pathlib import Path
import os

from equipment_rental.utils.common_utils import read_excel, read_csv, read_db_query, save_csv
from equipment_rental.configuration.configuration import Config
from equipment_rental.logger.logger import get_logger
from equipment_rental.entity.artifact_entity import BronzeArtifact

logger = get_logger()

class BronzeIngestion:
    """
    Handles ingestion of data into the Bronze layer.
    Supports Excel sheets, CSVs, and database query sources.
    """

    def __init__(self, bronze_dir: str = Config.BRONZE_DIR):
        self.bronze_dir = Path(bronze_dir)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.ingested_files = {}

    def ingest_excel(self, file_path: str, sheet_name: str, pipeline_run_id: str = None) -> BronzeArtifact:
        """
        Ingest data from an Excel sheet into Bronze layer.
        Returns a BronzeArtifact with the file path.
        """
        try:
            df = read_excel(file_path, sheet_name=sheet_name)
            df["load_timestamp"] = datetime.now()
            df["source_file"] = Path(file_path).name
            df["pipeline_run_id"] = pipeline_run_id

            output_path = self.bronze_dir / f"{sheet_name}.csv"
            save_csv(df, output_path)

            self.ingested_files[sheet_name] = str(output_path)
            logger.info(f"Bronze ingestion completed for Excel sheet '{sheet_name}' | rows: {len(df)}")
            return BronzeArtifact(ingested_files=self.ingested_files.copy())

        except Exception as e:
            logger.error(f"Bronze ingestion failed for Excel sheet '{sheet_name}': {str(e)}")
            raise

    def ingest_csv(self, file_path: str, pipeline_run_id: str = None) -> BronzeArtifact:
        """
        Ingest data from a CSV file into Bronze layer.
        Returns a BronzeArtifact with the file path.
        """
        try:
            df = read_csv(file_path)
            df["load_timestamp"] = datetime.now()
            df["source_file"] = Path(file_path).name
            df["pipeline_run_id"] = pipeline_run_id

            output_path = self.bronze_dir / Path(file_path).name
            save_csv(df, output_path)

            self.ingested_files[Path(file_path).stem] = str(output_path)
            logger.info(f"Bronze ingestion completed for CSV file '{file_path}' | rows: {len(df)}")
            return BronzeArtifact(ingested_files=self.ingested_files.copy())

        except Exception as e:
            logger.error(f"Bronze ingestion failed for CSV file '{file_path}': {str(e)}")
            raise

    def ingest_db(self, connection_str: str, query: str, table_name: str, pipeline_run_id: str = None) -> BronzeArtifact:
        """
        Ingest data from a database query into Bronze layer.
        Returns a BronzeArtifact with the file path.
        """
        try:
            df = read_db_query(connection_str, query)
            df["load_timestamp"] = datetime.now()
            df["source_query"] = query
            df["pipeline_run_id"] = pipeline_run_id

            output_path = self.bronze_dir / f"{table_name}.csv"
            save_csv(df, output_path)

            self.ingested_files[table_name] = str(output_path)
            logger.info(f"Bronze ingestion completed for DB table '{table_name}' | rows: {len(df)}")
            return BronzeArtifact(ingested_files=self.ingested_files.copy())

        except Exception as e:
            logger.error(f"Bronze ingestion failed for DB table '{table_name}': {str(e)}")
            raise