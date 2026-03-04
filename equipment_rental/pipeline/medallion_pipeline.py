# equipment_rental/pipeline/medallion_pipeline.py
import os
import pandas as pd
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException
from equipment_rental.constants.constants import BRONZE_DIR, SILVER_DIR, GOLD_DIR

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

        # Pre-register folder sources
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Bronze", source_type="folder", connection_text=BRONZE_DIR
        )
        self.silver_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Silver", source_type="folder", connection_text=SILVER_DIR
        )
        self.gold_folder_id = self.pipeline_manager.add_or_get_source(
            source_name="Gold", source_type="folder", connection_text=GOLD_DIR
        )

    def run(
        self,
        source_name,
        source_type,
        table_name,
        stage,
        file_path=None,
        db_query=None,
        pipeline_run_id=None,
        schedule_id=None,
        batch_id=None
    ):
        task_id = None
        try:
            logger.info(
                f"Pipeline stage started | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id} | batch_id={batch_id}"
            )

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
                    source_id=source_id,
                    target_id=self.bronze_folder_id,
                    stage="bronze",
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )

                # Ingest data
                if source_type.lower() == "db" and db_query:
                    bronze_df, _ = self.bronze.ingest_db(
                        db_query["connection_str"], db_query["query"], table_name, pipeline_run_id
                    )
                elif source_type.lower() == "excel" and file_path:
                    bronze_df, _ = self.bronze.ingest_excel(
                        file_path, sheet_name=table_name, pipeline_run_id=pipeline_run_id
                    )
                elif source_type.lower() == "csv" and file_path:
                    bronze_df, _ = self.bronze.ingest_csv(file_path=file_path, pipeline_run_id=pipeline_run_id)
                else:
                    raise ValueError("Invalid source configuration for Bronze stage")

                self.pipeline_manager.complete_task(task_id)
                return bronze_df

            # --------------------
            # Silver Stage
            # --------------------
            elif stage == "silver":
                bronze_source_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")
                if not os.path.exists(bronze_source_path):
                    raise FileNotFoundError(f"Bronze data not found for table '{table_name}' in {BRONZE_DIR}")

                bronze_source_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"{table_name}_bronze",
                    source_type="folder",
                    connection_text=bronze_source_path
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=bronze_source_id,
                    target_id=self.silver_folder_id,
                    stage="silver",
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )

                bronze_df = pd.read_csv(bronze_source_path)

                # Validate & transform
                validated = self.silver_validator.validate(
                    bronze_df, table_name, source_file=file_path, pipeline_run_id=pipeline_run_id
                )
                transformed = self.silver_transformer.transform(
                    validated, table_name, pipeline_run_id=pipeline_run_id
                )

                # Map table names to standardized filenames
                filename_map = {
                    "Customer_Master": "customer_master",
                    "Equipment_Master": "equipment_master",
                    "Rental_Transactions": "rental_transactions"
                }
                save_name = filename_map.get(table_name, table_name.lower())

                # Define allowed outputs per table
                allowed_keys = []
                if table_name.lower() in ["customer_master", "equipment_master"]:
                    allowed_keys = ["clean"]
                elif table_name.lower() == "rental_transactions":
                    allowed_keys = ["all", "active", "completed", "cancelled"]

                # Save CSVs
                for key, df in transformed.items():
                    if key not in allowed_keys:
                        continue
                    save_path = os.path.join(SILVER_DIR, f"{save_name}_{key}.csv")
                    df.to_csv(save_path, index=False)

                self.pipeline_manager.complete_task(task_id)
                return transformed

            # --------------------
            # Gold Stage
            # --------------------
            elif stage == "gold":
                silver_source_id = self.pipeline_manager.add_or_get_source(
                    source_name="Silver",
                    source_type="folder",
                    connection_text=SILVER_DIR
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=silver_source_id,
                    target_id=self.gold_folder_id,
                    stage="gold",
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )

                # Automatically detect all Silver master files
                customer_files = [f for f in os.listdir(SILVER_DIR) if f.startswith("customer_master") and f.endswith(".csv")]
                equipment_files = [f for f in os.listdir(SILVER_DIR) if f.startswith("equipment_master") and f.endswith(".csv")]
                rental_files = [f for f in os.listdir(SILVER_DIR) if f.startswith("rental_transactions") and f.endswith(".csv")]

                if not customer_files or not equipment_files or not rental_files:
                    raise FileNotFoundError("Required Silver master files missing in SILVER_DIR")

                # Load the first master files (can be extended to merge if multiple files exist)
                customer_df = pd.read_csv(os.path.join(SILVER_DIR, customer_files[0]))
                equipment_df = pd.read_csv(os.path.join(SILVER_DIR, equipment_files[0]))
                rental_df = pd.read_csv(os.path.join(SILVER_DIR, rental_files[0]))

                self.gold.aggregate(
                    rental_df=rental_df,
                    customer_df=customer_df,
                    equipment_df=equipment_df,
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)
                return True

            else:
                raise ValueError(f"Invalid stage: {stage}")

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(
                f"Pipeline stage failed | table: {table_name} | stage: {stage} | batch_id={batch_id} | error: {str(e)}"
            )
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")