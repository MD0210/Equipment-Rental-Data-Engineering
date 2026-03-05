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
from equipment_rental.configuration.configuration import Config
from equipment_rental.entity.artifact_entity import BronzeArtifact, SilverArtifact, GoldArtifact

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

        # Ensure directories exist
        for dir_path in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
            os.makedirs(dir_path, exist_ok=True)

        # Register folders
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source("Bronze", "folder", BRONZE_DIR)
        self.silver_folder_id = self.pipeline_manager.add_or_get_source("Silver", "folder", SILVER_DIR)
        self.gold_folder_id = self.pipeline_manager.add_or_get_source("Gold", "folder", GOLD_DIR)

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

    def run(self,
            source_name: str,
            stage: str,
            batch_type: str = "full",
            file_path: str = None,
            db_query: dict = None,
            pipeline_run_id: str = None,
            schedule_id: str = None,
            batch_id: str = None):

        task_id = None
        start_time = time.time()

        try:
            logger.info(f"Pipeline stage started | stage={stage} | pipeline_run_id={pipeline_run_id} | batch_type={batch_type}")

            # ============================
            # BRONZE STAGE
            # ============================
            if stage == "bronze":
                artifact = self._run_bronze(source_name, batch_type, file_path, db_query, pipeline_run_id, schedule_id, batch_id)
                return artifact

            # ============================
            # SILVER STAGE
            # ============================
            elif stage == "silver":
                artifact = self._run_silver(batch_type, pipeline_run_id, schedule_id, batch_id)
                return artifact

            # ============================
            # GOLD STAGE
            # ============================
            elif stage == "gold":
                artifact = self._run_gold(batch_type, pipeline_run_id, schedule_id, batch_id)
                return artifact

            else:
                raise ValueError(f"Invalid stage: {stage}")

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(f"Pipeline stage failed | stage={stage} | error={str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")

        finally:
            end_time = time.time()
            duration = end_time - start_time
            max_allowed = STAGE_SLA.get(stage)
            if max_allowed and duration > max_allowed:
                logger.warning(f"SLA breached for stage {stage}: took {duration:.2f}s, allowed {max_allowed:.2f}s")

    # ---------------------------
    # Bronze Stage Helper
    # ---------------------------
    def _run_bronze(self, source_name, batch_type, file_path, db_query, pipeline_run_id, schedule_id, batch_id):
        connection = file_path or (db_query.get("connection_str") if db_query else None)
        detected_type = self._detect_source_type(connection)
        source_id = self.pipeline_manager.add_or_get_source(source_name, detected_type, connection)

        task_id = self.pipeline_manager.start_task(
            source_id, self.bronze_folder_id, "bronze", pipeline_run_id, schedule_id, batch_id
        )

        last_watermark = self.pipeline_manager.get_last_watermark(source_id, "bronze")
        bronze_outputs = {}

        # Excel ingestion
        if detected_type == "excel":
            sheets = pd.read_excel(connection, sheet_name=None)
            for sheet_name, df in sheets.items():
                save_path = os.path.join(BRONZE_DIR, f"{sheet_name}.csv")
                df = self._apply_incremental(df, last_watermark, save_path, batch_type)
                df.to_csv(save_path, index=False)
                bronze_outputs[sheet_name] = df
                self.pipeline_manager.add_or_get_source(f"{sheet_name}_bronze", "csv", save_path)

        # CSV ingestion
        elif detected_type == "csv":
            df, _ = self.bronze.ingest_csv(file_path=file_path, pipeline_run_id=pipeline_run_id)
            save_path = file_path
            df = self._apply_incremental(df, last_watermark, save_path, batch_type)
            df.to_csv(save_path, index=False)
            bronze_outputs[source_name] = df
            self.pipeline_manager.add_or_get_source(f"{source_name}_bronze", "csv", save_path)

        # Database ingestion
        elif detected_type == "db" and db_query:
            query = db_query["query"]
            if batch_type == "incremental" and last_watermark is not None:
                query += f" WHERE LastUpdated > '{last_watermark}'"
            df, _ = self.bronze.ingest_db(db_query["connection_str"], query, source_name, pipeline_run_id)
            save_path = os.path.join(BRONZE_DIR, f"{source_name}.csv")
            df = self._apply_incremental(df, last_watermark, save_path, batch_type)
            df.to_csv(save_path, index=False)
            self.pipeline_manager.add_or_get_source(f"{source_name}_bronze", "csv", save_path)

        # Update watermark
        if batch_type == "incremental" and bronze_outputs:
            for df in bronze_outputs.values():
                if not df.empty:
                    max_timestamp = df["LastUpdated"].max()
                    self.pipeline_manager.update_watermark(source_id, "bronze", max_timestamp)

        self.pipeline_manager.complete_task(task_id)
        return BronzeArtifact(ingested_files={k: os.path.join(BRONZE_DIR, f"{k}.csv") for k in bronze_outputs})

    # ---------------------------
    # Silver Stage Helper
    # ---------------------------
    def _run_silver(self, batch_type, pipeline_run_id, schedule_id, batch_id):
        silver_outputs = {}
        bronze_files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".csv")]
        if not bronze_files:
            raise FileNotFoundError("No Bronze files found")

        for bronze_file in bronze_files:
            table_name = bronze_file.replace(".csv", "")
            bronze_path = os.path.join(BRONZE_DIR, bronze_file)
            bronze_source_id = self.pipeline_manager.add_or_get_source(f"{table_name}_bronze", "csv", bronze_path)

            task_id = self.pipeline_manager.start_task(
                bronze_source_id, self.silver_folder_id, "silver", pipeline_run_id, schedule_id, batch_id
            )

            df = pd.read_csv(bronze_path)
            last_watermark = self.pipeline_manager.get_last_watermark(bronze_source_id, "silver")
            df = df[df["LastUpdated"] > last_watermark] if batch_type == "incremental" and last_watermark else df

            validated = self.silver_validator.validate(df, table_name, bronze_path, pipeline_run_id)
            transformed = self.silver_transformer.transform(validated, table_name, pipeline_run_id)

            # Save transformed files
            for key, df_transformed in transformed.items():
                save_path = os.path.join(SILVER_DIR, f"{table_name.lower()}_{key}.csv")
                if batch_type == "incremental" and os.path.exists(save_path):
                    existing_df = pd.read_csv(save_path)
                    df_transformed = pd.concat([existing_df, df_transformed]).drop_duplicates()
                df_transformed.to_csv(save_path, index=False)
                silver_outputs[f"{table_name}_{key}"] = save_path
                self.pipeline_manager.add_or_get_source(f"{table_name}_{key}_silver", "csv", save_path)

            # Update watermark
            if batch_type == "incremental" and not df.empty:
                max_timestamp = df["LastUpdated"].max()
                self.pipeline_manager.update_watermark(bronze_source_id, "silver", max_timestamp)

            self.pipeline_manager.complete_task(task_id)

        return SilverArtifact(
            active_file=silver_outputs.get("rental_transactions_active"),
            completed_file=silver_outputs.get("rental_transactions_completed"),
            cancelled_file=silver_outputs.get("rental_transactions_cancelled"),
            all_file=silver_outputs.get("rental_transactions_all"),
            quarantine_file=os.path.join(SILVER_DIR, "quarantine/invalid_rows.csv")
        )

    # ---------------------------
    # Gold Stage Helper
    # ---------------------------
    def _run_gold(self, batch_type, pipeline_run_id, schedule_id, batch_id):
        silver_files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".csv")]
        if not silver_files:
            raise FileNotFoundError("No Silver files found")

        silver_dfs = {f.replace(".csv", ""): pd.read_csv(os.path.join(SILVER_DIR, f)) for f in silver_files}

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

        gold_files = [os.path.join(GOLD_DIR, f) for f in os.listdir(GOLD_DIR) if f.endswith(".csv")]
        for file_path in gold_files:
            source_name = os.path.basename(file_path).replace(".csv", "_gold")
            if not self.pipeline_manager.get_source_id_by_name(source_name):
                self.pipeline_manager.add_or_get_source(source_name, "csv", file_path)

        self.pipeline_manager.complete_task(gold_task_id)

        return GoldArtifact(
            equipment_agg_file=os.path.join(GOLD_DIR, Config.GOLD_AGGREGATION_FILENAME),
            customer_agg_file=os.path.join(GOLD_DIR, Config.GOLD_CUSTOMER_FILENAME),
            monthly_agg_file=os.path.join(GOLD_DIR, Config.GOLD_MONTHLY_FILENAME)
        )

    # ---------------------------
    # Utility: Apply incremental logic
    # ---------------------------
    def _apply_incremental(self, df: pd.DataFrame, last_watermark, save_path, batch_type):
        if batch_type == "incremental" and last_watermark is not None:
            df = df[df["LastUpdated"] > last_watermark]
            if os.path.exists(save_path):
                existing_df = pd.read_csv(save_path)
                df = pd.concat([existing_df, df]).drop_duplicates()
        return df