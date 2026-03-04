# equipment_rental/pipeline/medallion_pipeline.py
import os
import pandas as pd
from datetime import datetime
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException
from equipment_rental.constants.constants import BRONZE_DIR, SILVER_DIR, GOLD_DIR
from equipment_rental.utils.common_utils import save_csv

logger = get_logger()


class MedallionPipeline:

    def __init__(self):
        self.bronze = BronzeIngestion()
        self.silver_validator = SilverValidation()
        self.silver_transformer = SilverTransformation()
        self.gold = GoldAggregation()
        self.pipeline_manager = PipelineManager()

        # Ensure directories exist
        os.makedirs(BRONZE_DIR, exist_ok=True)
        os.makedirs(SILVER_DIR, exist_ok=True)
        os.makedirs(GOLD_DIR, exist_ok=True)

        # Pre-register Bronze, Silver, Gold folder sources
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Bronze",
            source_type="folder",
            connection_text=BRONZE_DIR
        )
        self.silver_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Silver",
            source_type="folder",
            connection_text=SILVER_DIR
        )
        self.gold_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Gold",
            source_type="folder",
            connection_text=GOLD_DIR
        )

    def run(
        self,
        source_name,
        source_type,
        table_name,
        stage,
        file_path=None,
        db_query=None,
        pipeline_run_id=None
    ):
        task_id = None
        try:
            logger.info(f"Pipeline stage started | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id}")

            # --------------------
            # Bronze Stage
            # --------------------
            if stage == "bronze":
                source_id = self.pipeline_manager.add_or_get_source(
                    source_name=source_name,
                    source_type=source_type,
                    connection_text=file_path or (db_query["connection_str"] if db_query else None)
                )
                task_id = self.pipeline_manager.start_task(
                    source_id, self.bronze_folder_id, "bronze", table_name, pipeline_run_id
                )

                # Ingest data
                if source_type == "db" and db_query:
                    bronze_df, _ = self.bronze.ingest_db(
                        db_query["connection_str"], db_query["query"], table_name, pipeline_run_id
                    )
                elif source_type == "excel" and file_path:
                    bronze_df, _ = self.bronze.ingest_excel(
                        file_path, sheet_name=table_name, pipeline_run_id=pipeline_run_id
                    )
                elif source_type == "csv" and file_path:
                    bronze_df, _ = self.bronze.ingest_csv(file_path=file_path, pipeline_run_id=pipeline_run_id)
                else:
                    raise ValueError("Invalid source configuration for Bronze stage")

                self.pipeline_manager.complete_task(task_id)
                return bronze_df

            # --------------------
            # Silver Stage
            # --------------------
            elif stage == "silver":
                task_id = self.pipeline_manager.start_task(
                    self.bronze_folder_id, self.silver_folder_id, "silver", table_name, pipeline_run_id
                )

                bronze_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")
                if not os.path.exists(bronze_path):
                    raise FileNotFoundError(f"Bronze data not found for table '{table_name}' in {BRONZE_DIR}")
                bronze_df = pd.read_csv(bronze_path)

                # Validate & transform
                validated = self.silver_validator.validate(
                    bronze_df, table_name, source_file=file_path, pipeline_run_id=pipeline_run_id
                )
                transformed = self.silver_transformer.transform(
                    validated, table_name, pipeline_run_id=pipeline_run_id
                )

                # Save only desired outputs
                allowed_keys = []
                if table_name.lower() in ["customer_master", "equipment_master"]:
                    allowed_keys = ["clean"]
                elif table_name.lower() == "rental_transactions":
                    allowed_keys = ["all", "active", "completed", "cancelled"]  # exclude equipment_utilisation and quarantine

                for key, df in transformed.items():
                    if key not in allowed_keys:
                        continue
                    save_path = os.path.join(SILVER_DIR, f"{table_name.lower()}_{key}.csv")
                    df.to_csv(save_path, index=False)

            # --------------------
            # Gold Stage
            # --------------------
            elif stage == "gold":
                gold_table_name = table_name  # <-- dynamic name for logging
                task_id = self.pipeline_manager.start_task(
                    self.silver_folder_id, self.gold_folder_id, "gold", gold_table_name, pipeline_run_id
                )

                # Load required Silver CSVs
                rental_df = pd.read_csv(os.path.join(SILVER_DIR, "rental_transactions_all.csv"))
                customer_df = pd.read_csv(os.path.join(SILVER_DIR, "customer_master_clean.csv"))
                equipment_df = pd.read_csv(os.path.join(SILVER_DIR, "equipment_master_clean.csv"))

                # Run Gold aggregation
                self.gold.aggregate(
                    rental_df=rental_df,
                    customer_df=customer_df,
                    equipment_df=equipment_df,
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)

            else:
                raise ValueError(f"Invalid stage: {stage}")

            logger.info(f"Pipeline stage completed | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(f"Pipeline stage failed | table: {table_name} | stage: {stage} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")