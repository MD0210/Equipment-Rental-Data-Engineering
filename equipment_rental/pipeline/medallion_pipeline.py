import os
import time
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

# SLA thresholds (seconds)
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
        self.bronze_folder_id = self.pipeline_manager.add_or_get_source(
            "Bronze", "folder", BRONZE_DIR
        )

        self.silver_folder_id = self.pipeline_manager.add_or_get_source(
            "Silver", "folder", SILVER_DIR
        )

        self.gold_folder_id = self.pipeline_manager.add_or_get_source(
            "Gold", "folder", GOLD_DIR
        )

    # -----------------------------------------
    # Detect source type
    # -----------------------------------------
    def _detect_source_type(self, connection_text):

        if not connection_text or "." not in connection_text:
            return "folder"

        ext = connection_text.split(".")[-1].lower()

        if ext == "csv":
            return "csv"

        elif ext in ["xlsx", "xls"]:
            return "excel"

        return ext

    # -----------------------------------------
    # Run Pipeline
    # -----------------------------------------
    def run(
        self,
        source_name,
        source_type,
        table_name=None,
        stage=None,
        file_path=None,
        db_query=None,
        pipeline_run_id=None,
        schedule_id=None,
        batch_id=None
    ):

        task_id = None
        start_time = time.time()

        try:

            logger.info(
                f"Pipeline stage started | stage={stage} | pipeline_run_id={pipeline_run_id}"
            )

            # =====================================
            # BRONZE
            # =====================================
            if stage == "bronze":

                connection = file_path or (
                    db_query["connection_str"] if db_query else None
                )

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

                # -------------------------------
                # Excel Source (Read ALL Sheets)
                # -------------------------------
                if detected_type == "excel":

                    sheets = pd.read_excel(file_path, sheet_name=None)

                    for sheet_name, df in sheets.items():

                        bronze_path = os.path.join(
                            BRONZE_DIR,
                            f"{sheet_name}.csv"
                        )

                        df.to_csv(bronze_path, index=False)

                        logger.info(f"Bronze extracted sheet: {sheet_name}")

                        # Register bronze table
                        self.pipeline_manager.add_or_get_source(
                            sheet_name,
                            "csv",
                            bronze_path
                        )

                # -------------------------------
                # CSV Source
                # -------------------------------
                elif detected_type == "csv":

                    bronze_df = pd.read_csv(file_path)

                    bronze_path = os.path.join(
                        BRONZE_DIR,
                        f"{source_name}.csv"
                    )

                    bronze_df.to_csv(bronze_path, index=False)

                    self.pipeline_manager.add_or_get_source(
                        source_name,
                        "csv",
                        bronze_path
                    )

                # -------------------------------
                # Database Source
                # -------------------------------
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

            # =====================================
            # SILVER
            # =====================================
            elif stage == "silver":

                bronze_files = [
                    f for f in os.listdir(BRONZE_DIR) if f.endswith(".csv")
                ]

                for file in bronze_files:

                    table = file.replace(".csv", "")
                    bronze_path = os.path.join(BRONZE_DIR, file)

                    source_id = self.pipeline_manager.add_or_get_source(
                        f"{table}_bronze",
                        "csv",
                        bronze_path
                    )

                    task_id = self.pipeline_manager.start_task(
                        source_id,
                        self.silver_folder_id,
                        "silver",
                        pipeline_run_id,
                        schedule_id,
                        batch_id
                    )

                    bronze_df = pd.read_csv(bronze_path)

                    validated = self.silver_validator.validate(
                        bronze_df,
                        table,
                        source_file=bronze_path,
                        pipeline_run_id=pipeline_run_id
                    )

                    transformed = self.silver_transformer.transform(
                        validated,
                        table,
                        pipeline_run_id=pipeline_run_id
                    )

                    for key, df in transformed.items():

                        silver_path = os.path.join(
                            SILVER_DIR,
                            f"{key}.csv"
                        )

                        df.to_csv(silver_path, index=False)

                        self.pipeline_manager.add_or_get_source(
                            key,
                            "csv",
                            silver_path
                        )

                    self.pipeline_manager.complete_task(task_id)

            # =====================================
            # GOLD
            # =====================================
            elif stage == "gold":

                rental_path = os.path.join(
                    SILVER_DIR,
                    "rental_transactions_all.csv"
                )

                customer_path = os.path.join(
                    SILVER_DIR,
                    "customer_master_clean.csv"
                )

                equipment_path = os.path.join(
                    SILVER_DIR,
                    "equipment_master_clean.csv"
                )

                rental_df = pd.read_csv(rental_path)
                customer_df = pd.read_csv(customer_path)
                equipment_df = pd.read_csv(equipment_path)

                source_id = self.pipeline_manager.add_or_get_source(
                    "rental_transactions_all_silver",
                    "csv",
                    rental_path
                )

                task_id = self.pipeline_manager.start_task(
                    source_id,
                    self.gold_folder_id,
                    "gold",
                    pipeline_run_id,
                    schedule_id,
                    batch_id
                )

                self.gold.aggregate(
                    rental_df,
                    customer_df,
                    equipment_df,
                    pipeline_run_id
                )

                for file in os.listdir(GOLD_DIR):

                    if file.endswith(".csv"):

                        path = os.path.join(GOLD_DIR, file)

                        name = file.replace(".csv", "_gold")

                        self.pipeline_manager.add_or_get_source(
                            name,
                            "csv",
                            path
                        )

                self.pipeline_manager.complete_task(task_id)

            else:
                raise ValueError(f"Invalid stage: {stage}")

            # =====================================
            # SLA Check
            # =====================================
            end_time = time.time()

            duration = end_time - start_time

            max_allowed = STAGE_SLA.get(stage)

            if max_allowed and duration > max_allowed:

                logger.warning(
                    f"SLA breached for stage {stage}: "
                    f"{duration:.2f}s > {max_allowed}s"
                )

            return True

        except Exception as e:

            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))

            logger.error(
                f"Pipeline stage failed | stage={stage} | error={str(e)}"
            )

            raise PipelineManagerException(
                f"Medallion pipeline execution failed: {str(e)}"
            )