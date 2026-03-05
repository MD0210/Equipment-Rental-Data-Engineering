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

        os.makedirs(BRONZE_DIR, exist_ok=True)
        os.makedirs(SILVER_DIR, exist_ok=True)
        os.makedirs(GOLD_DIR, exist_ok=True)

        # Register folders
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source(
            "Bronze", "folder", BRONZE_DIR
        )

        self.silver_folder_id = self.pipeline_manager.add_or_get_source(
            "Silver", "folder", SILVER_DIR
        )

        self.gold_folder_id = self.pipeline_manager.add_or_get_source(
            "Gold", "folder", GOLD_DIR
        )

    # Detect source type
    def _detect_source_type(self, connection_text: str):

        if not connection_text or "." not in connection_text:
            return "folder"

        ext = connection_text.split(".")[-1].lower()

        if ext == "csv":
            return "csv"
        elif ext in ["xlsx", "xls"]:
            return "excel"
        else:
            return ext

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
                f"Pipeline stage started | table={table_name} | stage={stage} | pipeline_run_id={pipeline_run_id}"
            )

            # ============================================================
            # BRONZE
            # ============================================================

            if stage == "bronze":

                connection = file_path or (db_query["connection_str"] if db_query else None)

                detected_type = self._detect_source_type(connection)

                source_id = self.pipeline_manager.add_or_get_source(
                    source_name,
                    detected_type,
                    connection
                )

                task_id = self.pipeline_manager.start_task(
                    source_id,
                    self.bronze_folder_id,
                    "bronze",
                    pipeline_run_id,
                    schedule_id,
                    batch_id
                )

                # Ingest
                if detected_type == "excel":

                    bronze_df, _ = self.bronze.ingest_excel(
                        file_path,
                        sheet_name=table_name,
                        pipeline_run_id=pipeline_run_id
                    )

                elif detected_type == "csv":

                    bronze_df, _ = self.bronze.ingest_csv(
                        file_path=file_path,
                        pipeline_run_id=pipeline_run_id
                    )

                elif detected_type == "db" and db_query:

                    bronze_df, _ = self.bronze.ingest_db(
                        db_query["connection_str"],
                        db_query["query"],
                        table_name,
                        pipeline_run_id
                    )

                else:
                    raise ValueError("Invalid Bronze source configuration")

                self.pipeline_manager.complete_task(task_id)

                return bronze_df

            # ============================================================
            # SILVER
            # ============================================================

            elif stage == "silver":

                bronze_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")

                if not os.path.exists(bronze_path):
                    raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

                detected_type = self._detect_source_type(bronze_path)

                bronze_source_id = self.pipeline_manager.add_or_get_source(
                    f"{table_name}_bronze",
                    detected_type,
                    bronze_path
                )

                task_id = self.pipeline_manager.start_task(
                    bronze_source_id,
                    self.silver_folder_id,
                    "silver",
                    pipeline_run_id,
                    schedule_id,
                    batch_id
                )

                bronze_df = pd.read_csv(bronze_path)

                validated = self.silver_validator.validate(
                    bronze_df,
                    table_name,
                    source_file=file_path,
                    pipeline_run_id=pipeline_run_id
                )

                transformed = self.silver_transformer.transform(
                    validated,
                    table_name,
                    pipeline_run_id=pipeline_run_id
                )

                filename_map = {
                    "Customer_Master": "customer_master",
                    "Equipment_Master": "equipment_master",
                    "Rental_Transactions": "rental_transactions"
                }

                save_name = filename_map.get(table_name, table_name.lower())

                allowed_keys = []

                if table_name.lower() in ["customer_master", "equipment_master"]:
                    allowed_keys = ["clean"]

                elif table_name.lower() == "rental_transactions":
                    allowed_keys = ["all", "active", "completed", "cancelled"]

                for key, df in transformed.items():

                    if key not in allowed_keys:
                        continue

                    save_path = os.path.join(SILVER_DIR, f"{save_name}_{key}.csv")

                    df.to_csv(save_path, index=False)

                    detected_type = self._detect_source_type(save_path)

                    self.pipeline_manager.add_or_get_source(
                        f"{save_name}_{key}_silver",
                        detected_type,
                        save_path
                    )

                # ✅ FIX: COMPLETE SILVER TASK
                self.pipeline_manager.complete_task(task_id)

                return True

            # ============================================================
            # GOLD
            # ============================================================

            elif stage == "gold":

                # Only run Gold for fact table
                if table_name.lower() != "rental_transactions":
                    logger.info(f"Skipping Gold stage for table: {table_name}")
                    return True

                # Get all Silver sources for Rental_Transactions
                rental_files = [
                    f for f in os.listdir(SILVER_DIR)
                    if f.startswith("rental_transactions") and f.endswith(".csv")
                ]

                if not rental_files:
                    raise FileNotFoundError("No rental transaction Silver files found")

                # Also ensure master tables exist
                required_masters = [
                    os.path.join(SILVER_DIR, "customer_master_clean.csv"),
                    os.path.join(SILVER_DIR, "equipment_master_clean.csv")
                ]
                for path in required_masters:
                    if not os.path.exists(path):
                        raise FileNotFoundError(f"Missing Silver file: {path}")

                # Read master tables once
                customer_df = pd.read_csv(os.path.join(SILVER_DIR, "customer_master_clean.csv"))
                equipment_df = pd.read_csv(os.path.join(SILVER_DIR, "equipment_master_clean.csv"))

                # Loop through each Silver rental file and create a task for it
                for rental_file in rental_files:

                    rental_path = os.path.join(SILVER_DIR, rental_file)
                    detected_type = self._detect_source_type(rental_path)

                    silver_source_id = self.pipeline_manager.add_or_get_source(
                        f"{rental_file.replace('.csv', '')}_silver",
                        detected_type,
                        rental_path
                    )

                    task_id = self.pipeline_manager.start_task(
                        source_id=silver_source_id,
                        target_id=self.gold_folder_id,
                        stage="gold",
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=schedule_id,
                        batch_id=batch_id
                    )

                    # Read the rental CSV
                    rental_df = pd.read_csv(rental_path)

                    # Run Gold aggregation
                    self.gold.aggregate(
                        rental_df=rental_df,
                        customer_df=customer_df,
                        equipment_df=equipment_df,
                        pipeline_run_id=pipeline_run_id
                    )

                    # Register the Gold outputs
                    for file in os.listdir(GOLD_DIR):
                        if not file.endswith(".csv"):
                            continue

                        file_path = os.path.join(GOLD_DIR, file)
                        detected_type = self._detect_source_type(file_path)

                        self.pipeline_manager.add_or_get_source(
                            file.replace(".csv", "_gold"),
                            detected_type,
                            file_path
                        )

                    self.pipeline_manager.complete_task(task_id)

                return True

            else:
                raise ValueError(f"Invalid stage: {stage}")

        except Exception as e:

            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))

            logger.error(
                f"Pipeline stage failed | table={table_name} | stage={stage} | error={str(e)}"
            )

            raise PipelineManagerException(
                f"Medallion pipeline execution failed: {str(e)}"
            )