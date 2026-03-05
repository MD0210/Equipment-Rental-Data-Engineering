import os
import pandas as pd
import sqlite3
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

            # ============================
            # BRONZE
            # ============================
            if stage == "bronze":
                connection = file_path or (db_query["connection_str"] if db_query else None)
                detected_type = self._detect_source_type(connection)

                source_id = self.pipeline_manager.add_or_get_source(
                    source_name, detected_type, connection
                )

                task_id = self.pipeline_manager.start_task(
                    source_id, self.bronze_folder_id, "bronze",
                    pipeline_run_id, schedule_id, batch_id
                )

                if detected_type == "excel":
                    bronze_df, _ = self.bronze.ingest_excel(
                        file_path, sheet_name=table_name, pipeline_run_id=pipeline_run_id
                    )
                elif detected_type == "csv":
                    bronze_df, _ = self.bronze.ingest_csv(
                        file_path=file_path, pipeline_run_id=pipeline_run_id
                    )
                elif detected_type == "db" and db_query:
                    bronze_df, _ = self.bronze.ingest_db(
                        db_query["connection_str"], db_query["query"],
                        table_name, pipeline_run_id
                    )
                else:
                    raise ValueError("Invalid Bronze source configuration")

                self.pipeline_manager.complete_task(task_id)
                return bronze_df

            # ============================
            # SILVER
            # ============================
            elif stage == "silver":
                bronze_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")
                if not os.path.exists(bronze_path):
                    raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

                detected_type = self._detect_source_type(bronze_path)
                bronze_source_id = self.pipeline_manager.add_or_get_source(
                    f"{table_name}_bronze", detected_type, bronze_path
                )

                # Start Silver task for all tables
                task_id = self.pipeline_manager.start_task(
                    bronze_source_id, self.silver_folder_id, "silver",
                    pipeline_run_id, schedule_id, batch_id
                )

                bronze_df = pd.read_csv(bronze_path)
                validated = self.silver_validator.validate(
                    bronze_df, table_name, source_file=file_path, pipeline_run_id=pipeline_run_id
                )
                transformed = self.silver_transformer.transform(
                    validated, table_name, pipeline_run_id=pipeline_run_id
                )

                filename_map = {
                    "Customer_Master": "customer_master",
                    "Equipment_Master": "equipment_master",
                    "Rental_Transactions": "rental_transactions"
                }
                save_name = filename_map.get(table_name, table_name.lower())

                for key, df in transformed.items():
                    if table_name.lower() in ["customer_master", "equipment_master"]:
                        silver_path = os.path.join(SILVER_DIR, f"{save_name}_clean.csv")
                        df.to_csv(silver_path, index=False)

                        # ✅ Register as a Silver task
                        master_task_id = self.pipeline_manager.start_task(
                            bronze_source_id,
                            self.silver_folder_id,
                            "silver",
                            pipeline_run_id,
                            schedule_id,
                            batch_id
                        )
                        self.pipeline_manager.add_or_get_source(
                            f"{save_name}_clean_silver",
                            self._detect_source_type(silver_path),
                            silver_path
                        )
                        self.pipeline_manager.complete_task(master_task_id)

                    elif table_name.lower() == "rental_transactions":
                        allowed_keys = ["all", "active", "completed", "cancelled"]
                        if key not in allowed_keys:
                            continue
                        save_path = os.path.join(SILVER_DIR, f"{save_name}_{key}.csv")
                        df.to_csv(save_path, index=False)
                        self.pipeline_manager.add_or_get_source(
                            f"{save_name}_{key}_silver",
                            self._detect_source_type(save_path),
                            save_path
                        )

                self.pipeline_manager.complete_task(task_id)
                return True

            # ============================
            # GOLD
            # ============================
            elif stage == "gold":
                master_paths = [
                    ("customer_master_clean.csv", "Customer_Master"),
                    ("equipment_master_clean.csv", "Equipment_Master")
                ]
                master_dfs = {}
                for filename, table in master_paths:
                    path = os.path.join(SILVER_DIR, filename)
                    if not os.path.exists(path):
                        raise FileNotFoundError(f"Missing Silver file: {path}")
                    master_dfs[table] = pd.read_csv(path)

                    # ✅ Start and complete Gold task for master tables
                    master_task_id = self.pipeline_manager.start_task(
                        source_id=self.pipeline_manager.get_source_id_by_name(f"{table.lower()}_clean_silver"),
                        target_id=self.gold_folder_id,
                        stage="gold",
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=schedule_id,
                        batch_id=batch_id
                    )
                    self.pipeline_manager.complete_task(master_task_id)

                # Process rental_transactions Gold
                rental_files = sorted(
                    f for f in os.listdir(SILVER_DIR)
                    if f.startswith("rental_transactions") and f.endswith(".csv")
                )
                if not rental_files:
                    raise FileNotFoundError("No rental transaction Silver files found")

                for rental_file in rental_files:
                    rental_path = os.path.join(SILVER_DIR, rental_file)
                    detected_type = self._detect_source_type(rental_path)

                    silver_source_name = rental_file.replace(".csv", "") + "_silver"
                    silver_source_id = self.pipeline_manager.get_source_id_by_name(silver_source_name)
                    if not silver_source_id:
                        silver_source_id = self.pipeline_manager.add_or_get_source(
                            silver_source_name,
                            detected_type,
                            rental_path
                        )

                    with sqlite3.connect(self.pipeline_manager.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT schedule_id, batch_id
                            FROM task
                            WHERE source_id=? AND stage='silver' AND status='success'
                            ORDER BY task_id DESC
                            LIMIT 1
                        """, (silver_source_id,))
                        row = cursor.fetchone()
                        gold_schedule_id, gold_batch_id = (schedule_id, batch_id) if not row else row

                    gold_task_id = self.pipeline_manager.start_task(
                        source_id=silver_source_id,
                        target_id=self.gold_folder_id,
                        stage="gold",
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=gold_schedule_id,
                        batch_id=gold_batch_id
                    )

                    rental_df = pd.read_csv(rental_path)
                    self.gold.aggregate(
                        rental_df=rental_df,
                        customer_df=master_dfs["Customer_Master"],
                        equipment_df=master_dfs["Equipment_Master"],
                        pipeline_run_id=pipeline_run_id
                    )

                    # Register Gold outputs
                    for file in os.listdir(GOLD_DIR):
                        if not file.endswith(".csv"):
                            continue
                        file_path = os.path.join(GOLD_DIR, file)
                        gold_source_name = file.replace(".csv", "_gold")
                        gold_source_id = self.pipeline_manager.add_or_get_source(
                            gold_source_name,
                            self._detect_source_type(file_path),
                            file_path
                        )

                        # ✅ Start and complete Gold task for each output
                        gold_output_task_id = self.pipeline_manager.start_task(
                            source_id=silver_source_id,
                            target_id=self.gold_folder_id,
                            stage="gold",
                            pipeline_run_id=pipeline_run_id,
                            schedule_id=schedule_id,
                            batch_id=batch_id
                        )
                        self.pipeline_manager.complete_task(gold_output_task_id)

                    # Equipment utilisation explicitly
                    util_path = os.path.join(GOLD_DIR, "equipment_utilisation.csv")
                    if os.path.exists(util_path):
                        util_source_id = self.pipeline_manager.add_or_get_source(
                            "equipment_utilisation_gold",
                            self._detect_source_type(util_path),
                            util_path
                        )
                        util_task_id = self.pipeline_manager.start_task(
                            source_id=silver_source_id,
                            target_id=self.gold_folder_id,
                            stage="gold",
                            pipeline_run_id=pipeline_run_id,
                            schedule_id=schedule_id,
                            batch_id=batch_id
                        )
                        self.pipeline_manager.complete_task(util_task_id)

                    self.pipeline_manager.complete_task(gold_task_id)

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