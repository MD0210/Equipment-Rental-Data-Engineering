# equipment_rental/pipeline/medallion_pipeline.py
from datetime import datetime
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException
import os
import pandas as pd
from equipment_rental.constants.constants import SILVER_DIR

logger = get_logger()


class MedallionPipeline:

    def __init__(self):
        self.bronze = BronzeIngestion()
        self.silver_validator = SilverValidation()
        self.silver_transformer = SilverTransformation()
        self.gold = GoldAggregation()
        self.pipeline_manager = PipelineManager()

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
        """
        Runs one stage of the Medallion pipeline for a given table.
        Stage must be one of: bronze, silver, gold.
        """
        task_id = None

        try:
            logger.info(
                f"Pipeline stage started | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id}"
            )

            # =========================
            # SOURCE
            # =========================
            data_source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                source_type=source_type,
                connection_text=file_path or (
                    db_query["connection_str"] if db_query else None
                )
            )

            if stage == "bronze":
                # Bronze target folder
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name="Bronze",
                    source_type="folder",
                    connection_text=f"artifacts/bronze/{table_name}"
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=data_source_id,
                    target_id=target_id,
                    stage="bronze",
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )

                # Ingest data
                if source_type == "db" and db_query:
                    bronze_df, _ = self.bronze.ingest_db(
                        connection_str=db_query["connection_str"],
                        query=db_query["query"],
                        table_name=table_name,
                        pipeline_run_id=pipeline_run_id
                    )
                elif source_type == "excel" and file_path:
                    bronze_df, _ = self.bronze.ingest_excel(
                        file_path=file_path,
                        sheet_name=table_name,
                        pipeline_run_id=pipeline_run_id
                    )
                elif source_type == "csv" and file_path:
                    bronze_df, _ = self.bronze.ingest_csv(
                        file_path=file_path,
                        pipeline_run_id=pipeline_run_id
                    )
                else:
                    raise ValueError("Invalid source configuration")

                self.pipeline_manager.complete_task(task_id)
                return bronze_df  # needed for silver

            elif stage == "silver":
                # Silver target folder
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name="Silver",
                    source_type="folder",
                    connection_text=f"artifacts/silver/{table_name}"
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=data_source_id,
                    target_id=target_id,
                    stage="silver",
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )

                # Load bronze_df from SILVER_DIR if not passed
                bronze_df = None
                bronze_path = f"{SILVER_DIR}/{table_name}.csv"
                if os.path.exists(bronze_path):
                    bronze_df = pd.read_csv(bronze_path)

                if bronze_df is None:
                    raise ValueError("Bronze data not found for silver stage")

                validated_tables = self.silver_validator.validate(
                    df=bronze_df,
                    table_name=table_name,
                    source_file=file_path,
                    pipeline_run_id=pipeline_run_id
                )

                transformed_tables = self.silver_transformer.transform(
                    validated_tables=validated_tables,
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)
                return transformed_tables  # needed for gold

            elif stage == "gold":
                # Gold target folder
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name="Gold",
                    source_type="folder",
                    connection_text=f"artifacts/gold/{table_name}"
                )

                task_id = self.pipeline_manager.start_task(
                    source_id=data_source_id,
                    target_id=target_id,
                    stage="gold",
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )

                # Load silver data if not passed
                transformed_tables = {}
                if os.path.exists(f"{SILVER_DIR}/rental_transactions_clean.csv"):
                    transformed_tables["all"] = pd.read_csv(f"{SILVER_DIR}/rental_transactions_clean.csv")
                if os.path.exists(f"{SILVER_DIR}/customer_master_clean.csv"):
                    transformed_tables["customer_master_clean"] = pd.read_csv(f"{SILVER_DIR}/customer_master_clean.csv")
                if os.path.exists(f"{SILVER_DIR}/equipment_master_clean.csv"):
                    transformed_tables["equipment_master_clean"] = pd.read_csv(f"{SILVER_DIR}/equipment_master_clean.csv")

                rental_df = transformed_tables.get("all") if table_name.lower() == "rental_transactions" else None
                customer_df = transformed_tables.get("customer_master_clean")
                equipment_df = transformed_tables.get("equipment_master_clean")

                self.gold.aggregate(
                    rental_df=rental_df,
                    customer_df=customer_df,
                    equipment_df=equipment_df,
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)

            else:
                raise ValueError(f"Invalid stage: {stage}")

            logger.info(
                f"Pipeline stage completed successfully | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id}"
            )

        except Exception as e:
            logger.error(f"Pipeline stage failed | table: {table_name} | stage: {stage} | error: {str(e)}")
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            raise PipelineManagerException(
                f"Medallion pipeline execution failed: {str(e)}"
            )