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

    def _detect_source_type(self, path):

        if not path or "." not in path:
            return "folder"

        ext = path.split(".")[-1].lower()

        if ext in ["xlsx", "xls"]:
            return "excel"
        if ext == "csv":
            return "csv"

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

        try:

            logger.info(
                f"Pipeline stage started | table={table_name} | stage={stage} | pipeline_run_id={pipeline_run_id}"
            )

            # ==================================================
            # BRONZE
            # ==================================================
            if stage == "bronze":

                connection = file_path or (db_query["connection_str"] if db_query else None)

                source_id = self.pipeline_manager.add_or_get_source(
                    source_name,
                    source_type,
                    connection
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=source_id,
                    target_id=self.bronze_folder_id,
                    stage="bronze",
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )

                if source_type == "excel":

                    bronze_df, bronze_path = self.bronze.ingest_excel(
                        file_path=file_path,
                        sheet_name=table_name,
                        pipeline_run_id=pipeline_run_id
                    )

                elif source_type == "csv":

                    bronze_df, bronze_path = self.bronze.ingest_csv(
                        file_path=file_path,
                        pipeline_run_id=pipeline_run_id
                    )

                elif source_type == "db":

                    bronze_df, bronze_path = self.bronze.ingest_db(
                        db_query["connection_str"],
                        db_query["query"],
                        table_name,
                        pipeline_run_id
                    )

                else:
                    raise ValueError("Unsupported source type")

                # register artifact
                artifact_id = self.pipeline_manager.add_or_get_source(
                    f"{table_name}_bronze",
                    "csv",
                    bronze_path
                )

                self.pipeline_manager.complete_task(task_id)

                return bronze_df

            # ==================================================
            # SILVER
            # ==================================================
            elif stage == "silver":

                bronze_file = os.path.join(BRONZE_DIR, f"{table_name}.csv")

                if not os.path.exists(bronze_file):
                    raise FileNotFoundError(bronze_file)

                bronze_df = pd.read_csv(bronze_file)

                bronze_source_id = self.pipeline_manager.add_or_get_source(
                    f"{table_name}_bronze",
                    "csv",
                    bronze_file
                )

                validated = self.silver_validator.validate(
                    bronze_df,
                    table_name,
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

                outputs = []

                for key, df in transformed.items():

                    filename = f"{save_name}_{key}.csv"
                    save_path = os.path.join(SILVER_DIR, filename)

                    df.to_csv(save_path, index=False)

                    artifact_id = self.pipeline_manager.add_or_get_source(
                        filename.replace(".csv", "_silver"),
                        "csv",
                        save_path
                    )

                    task_id = self.pipeline_manager.start_task(
                        source_id=bronze_source_id,
                        target_id=artifact_id,
                        stage="silver",
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=schedule_id,
                        batch_id=batch_id
                    )

                    self.pipeline_manager.complete_task(task_id)

                    outputs.append(save_path)

                return outputs

            # ==================================================
            # GOLD
            # ==================================================
            elif stage == "gold":

                rental_files = [
                    f for f in os.listdir(SILVER_DIR)
                    if f.startswith("rental_transactions") and f.endswith(".csv")
                ]

                if not rental_files:
                    raise FileNotFoundError("No rental_transactions silver files")

                rental_df = pd.concat(
                    [pd.read_csv(os.path.join(SILVER_DIR, f)) for f in rental_files]
                )

                customer_df = pd.read_csv(
                    os.path.join(SILVER_DIR, "customer_master_clean.csv")
                )

                equipment_df = pd.read_csv(
                    os.path.join(SILVER_DIR, "equipment_master_clean.csv")
                )

                gold_outputs = self.gold.aggregate(
                    rental_df=rental_df,
                    customer_df=customer_df,
                    equipment_df=equipment_df,
                    pipeline_run_id=pipeline_run_id
                )

                for name, path in gold_outputs.items():

                    artifact_id = self.pipeline_manager.add_or_get_source(
                        f"{name}_gold",
                        "csv",
                        path
                    )

                    task_id = self.pipeline_manager.start_task(
                        source_id=self.silver_folder_id,
                        target_id=artifact_id,
                        stage="gold",
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=schedule_id,
                        batch_id=batch_id
                    )

                    self.pipeline_manager.complete_task(task_id)

                return True

            else:
                raise ValueError(f"Invalid stage {stage}")

        except Exception as e:

            logger.error(
                f"Pipeline failed | table={table_name} | stage={stage} | error={str(e)}"
            )

            raise PipelineManagerException(
                f"Medallion pipeline execution failed: {str(e)}"
            )