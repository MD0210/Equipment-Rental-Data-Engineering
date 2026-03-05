# equipment_rental/pipeline/medallion_pipeline.py
import os
import time
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

# SLA thresholds (in seconds)
STAGE_SLA = {
    "bronze": 30,
    "silver": 60,
    "gold": 120
}


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
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source("Bronze", "folder", BRONZE_DIR)
        self.silver_folder_id = self.pipeline_manager.add_or_get_source("Silver", "folder", SILVER_DIR)
        self.gold_folder_id = self.pipeline_manager.add_or_get_source("Gold", "folder", GOLD_DIR)

    def _detect_source_type(self, connection_text: str):
        if not connection_text or "." not in connection_text:
            return "folder"
        ext = connection_text.split(".")[-1].lower()
        if ext in ["csv"]:
            return "csv"
        elif ext in ["xlsx", "xls"]:
            return "excel"
        else:
            return "db"

    def run(self,
            source_name,
            source_type,
            table_name,
            stage,
            batch_type="full",
            file_path=None,
            db_query=None,
            pipeline_run_id=None,
            schedule_id=None,
            batch_id=None):

        task_id = None
        start_time = time.time()

        try:
            logger.info(f"Pipeline stage started | table={table_name} | stage={stage} | pipeline_run_id={pipeline_run_id} | batch_type={batch_type}")

            # ============================
            # BRONZE STAGE
            # ============================
            if stage == "bronze":
                # Determine connection
                connection = file_path or (db_query.get("connection_str") if db_query else None)
                detected_type = self._detect_source_type(connection)
                source_id = self.pipeline_manager.add_or_get_source(source_name, detected_type, connection)

                # Start task
                task_id = self.pipeline_manager.start_task(
                    source_id, self.bronze_folder_id, "bronze",
                    pipeline_run_id, schedule_id, batch_id
                )

                last_watermark = self.pipeline_manager.get_last_watermark(source_id, "bronze")

                bronze_outputs = {}
                if detected_type == "excel":
                    sheets = pd.ExcelFile(connection).sheet_names
                    for sheet_name in sheets:
                        df, save_path = self.bronze.ingest_excel(connection, sheet_name, pipeline_run_id)
                        # Incremental filtering
                        if batch_type == "incremental" and last_watermark is not None and "LastUpdated" in df.columns:
                            df = df[df["LastUpdated"] > last_watermark]
                            if os.path.exists(save_path):
                                existing_df = pd.read_csv(save_path)
                                df = pd.concat([existing_df, df]).drop_duplicates()
                            save_csv(df, save_path)
                        bronze_outputs[sheet_name] = df
                        self.pipeline_manager.add_or_get_source(f"{sheet_name}_bronze", "csv", save_path)

                elif detected_type == "csv":
                    df, save_path = self.bronze.ingest_csv(file_path, pipeline_run_id)
                    if batch_type == "incremental" and last_watermark is not None and "LastUpdated" in df.columns:
                        df = df[df["LastUpdated"] > last_watermark]
                        if os.path.exists(save_path):
                            existing_df = pd.read_csv(save_path)
                            df = pd.concat([existing_df, df]).drop_duplicates()
                        save_csv(df, save_path)
                    bronze_outputs[table_name] = df
                    self.pipeline_manager.add_or_get_source(f"{table_name}_bronze", "csv", save_path)

                elif detected_type == "db" and db_query:
                    query = db_query.get("query")
                    if batch_type == "incremental" and last_watermark is not None:
                        query += f" WHERE LastUpdated > '{last_watermark}'"
                    df, save_path = self.bronze.ingest_db(db_query["connection_str"], query, table_name, pipeline_run_id)
                    if batch_type == "incremental" and os.path.exists(save_path):
                        existing_df = pd.read_csv(save_path)
                        df = pd.concat([existing_df, df]).drop_duplicates()
                        save_csv(df, save_path)
                    bronze_outputs[table_name] = df
                    self.pipeline_manager.add_or_get_source(f"{table_name}_bronze", "csv", save_path)

                # Update watermark
                if batch_type == "incremental" and bronze_outputs:
                    for df in bronze_outputs.values():
                        if "LastUpdated" in df.columns:
                            max_ts = df["LastUpdated"].max()
                            self.pipeline_manager.update_watermark(source_id, "bronze", max_ts)

                self.pipeline_manager.complete_task(task_id)
                return bronze_outputs

            # ============================
            # SILVER STAGE
            # ============================
            elif stage == "silver":
                bronze_files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".csv")]
                if not bronze_files:
                    raise FileNotFoundError("No Bronze files found")

                for bronze_file in bronze_files:
                    table_name = bronze_file.replace(".csv", "")
                    bronze_path = os.path.join(BRONZE_DIR, bronze_file)
                    bronze_source_id = self.pipeline_manager.add_or_get_source(f"{table_name}_bronze", "csv", bronze_path)

                    task_id = self.pipeline_manager.start_task(
                        bronze_source_id, self.silver_folder_id, "silver",
                        pipeline_run_id, schedule_id, batch_id
                    )

                    bronze_df = pd.read_csv(bronze_path)
                    last_watermark = self.pipeline_manager.get_last_watermark(bronze_source_id, "silver")
                    if batch_type == "incremental" and last_watermark is not None and "LastUpdated" in bronze_df.columns:
                        bronze_df = bronze_df[bronze_df["LastUpdated"] > last_watermark]

                    validated = self.silver_validator.validate(bronze_df, table_name, source_file=bronze_path, pipeline_run_id=pipeline_run_id)
                    transformed = self.silver_transformer.transform(validated, table_name, pipeline_run_id=pipeline_run_id)
                    if table_name.lower() == "rental_transactions" and "quarantine" and "equipment_utilisation" in transformed:
                       transformed.pop("quarantine", "equipment_utilisation")

                    for key, df in transformed.items():
                        save_name = table_name.lower()
                        if table_name.lower() in ["customer_master", "equipment_master"]:
                            # Only save _clean version, no _all
                            silver_path = os.path.join(SILVER_DIR, f"{save_name}_clean.csv")
                            if batch_type == "incremental" and os.path.exists(silver_path):
                                existing_df = pd.read_csv(silver_path)
                                df = pd.concat([existing_df, df]).drop_duplicates()
                            save_csv(df, silver_path)
                            self.pipeline_manager.add_or_get_source(f"{save_name}_clean_silver", "csv", silver_path)
                        elif table_name.lower() == "rental_transactions":
                            # Keep all outputs for rental_transactions
                            if key == "equipment_utilisation":
                                silver_path = os.path.join(SILVER_DIR, "equipment_utilisation.csv")
                            else:
                                silver_path = os.path.join(SILVER_DIR, f"{save_name}_{key}.csv")
                            if batch_type == "incremental" and os.path.exists(silver_path):
                                existing_df = pd.read_csv(silver_path)
                                df = pd.concat([existing_df, df]).drop_duplicates()
                            save_csv(df, silver_path)
                            self.pipeline_manager.add_or_get_source(f"{save_name}_{key}_silver", "csv", silver_path)

                    if batch_type == "incremental" and not bronze_df.empty:
                        max_ts = bronze_df["LastUpdated"].max()
                        self.pipeline_manager.update_watermark(bronze_source_id, "silver", max_ts)

                    self.pipeline_manager.complete_task(task_id)

            # ============================
            # GOLD STAGE
            # ============================
            elif stage == "gold":
                silver_files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".csv")]
                if not silver_files:
                    raise FileNotFoundError("No Silver files found")

                silver_dfs = {}
                for file in silver_files:
                    table_name = file.replace(".csv", "")
                    path = os.path.join(SILVER_DIR, file)
                    silver_source_id = self.pipeline_manager.add_or_get_source(f"{table_name}_silver", "csv", path)
                    silver_dfs[table_name] = pd.read_csv(path)

                rental_df = silver_dfs.get("rental_transactions_all")
                customer_df = silver_dfs.get("customer_master_clean")
                equipment_df = silver_dfs.get("equipment_master_clean")
                if rental_df is None or customer_df is None or equipment_df is None:
                    raise ValueError("Missing required Silver datasets")

                gold_task_id = self.pipeline_manager.start_task(
                    source_id=self.pipeline_manager.get_source_id_by_name("rental_transactions_all_silver"),
                    target_id=self.gold_folder_id,
                    stage="gold",
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )

                self.gold.aggregate(
                    rental_df=rental_df,
                    customer_df=customer_df,
                    equipment_df=equipment_df,
                    pipeline_run_id=pipeline_run_id
                )

                # Register Gold outputs
                for file in os.listdir(GOLD_DIR):
                    if file.endswith(".csv"):
                        file_path = os.path.join(GOLD_DIR, file)
                        source_name = file.replace(".csv", "_gold")
                        if not self.pipeline_manager.get_source_id_by_name(source_name):
                            self.pipeline_manager.add_or_get_source(source_name, "csv", file_path)

                self.pipeline_manager.complete_task(gold_task_id)

            else:
                raise ValueError(f"Invalid stage: {stage}")

            # ============================
            # SLA Check
            # ============================
            end_time = time.time()
            duration = end_time - start_time
            max_allowed = STAGE_SLA.get(stage)
            if max_allowed and duration > max_allowed:
                logger.warning(f"SLA breached for stage {stage}: took {duration:.2f}s, allowed {max_allowed:.2f}s | pipeline_run_id={pipeline_run_id}")

            return True

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(f"Pipeline stage failed | table={table_name} | stage={stage} | error={str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")