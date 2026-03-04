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
        task_id = None
        try:
            logger.info(f"Pipeline stage started | table: {table_name} | stage: {stage} | pipeline_run_id={pipeline_run_id}")

            # --------------------
            # Source registration
            # --------------------
            data_source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                source_type=source_type,
                connection_text=file_path or (db_query["connection_str"] if db_query else None)
            )

            # --------------------
            # Bronze Stage
            # --------------------
            if stage == "bronze":
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
                    bronze_df, _ = self.bronze.ingest_db(db_query["connection_str"], db_query["query"], table_name, pipeline_run_id)
                elif source_type == "excel" and file_path:
                    bronze_df, _ = self.bronze.ingest_excel(file_path, sheet_name=table_name, pipeline_run_id=pipeline_run_id)
                elif source_type == "csv" and file_path:
                    bronze_df, _ = self.bronze.ingest_csv(file_path=file_path, pipeline_run_id=pipeline_run_id)
                else:
                    raise ValueError("Invalid source configuration")

                self.pipeline_manager.complete_task(task_id)
                return bronze_df

            # --------------------
            # Silver Stage
            # --------------------
            elif stage == "silver":
                # Check bronze exists
                bronze_path = f"{SILVER_DIR}/{table_name}.csv"
                if not os.path.exists(bronze_path):
                    raise ValueError("Bronze data not found for silver stage")

                target_id = self.pipeline_manager.add_or_get_source(
                    source_name="Silver",
                    source_type="folder",
                    connection_text=f"artifacts/silver/{table_name}"
                )
                task_id = self.pipeline_manager.start_task(data_source_id, target_id, "silver", table_name, pipeline_run_id)

                bronze_df = pd.read_csv(bronze_path)
                validated = self.silver_validator.validate(bronze_df, table_name, source_file=file_path, pipeline_run_id=pipeline_run_id)
                transformed = self.silver_transformer.transform(validated, table_name, pipeline_run_id=pipeline_run_id)
                self.pipeline_manager.complete_task(task_id)
                return transformed

            # --------------------
            # Gold Stage
            # --------------------
            elif stage == "gold":
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name="Gold",
                    source_type="folder",
                    connection_text=f"artifacts/gold/{table_name}"
                )
                task_id = self.pipeline_manager.start_task(data_source_id, target_id, "gold", table_name, pipeline_run_id)

                transformed_tables = {}
                for f in ["rental_transactions_clean.csv","customer_master_clean.csv","equipment_master_clean.csv"]:
                    path = f"{SILVER_DIR}/{f}"
                    if os.path.exists(path):
                        key = f.replace(".csv","")
                        transformed_tables[key] = pd.read_csv(path)

                self.gold.aggregate(
                    rental_df=transformed_tables.get("rental_transactions_clean") if table_name.lower()=="rental_transactions" else None,
                    customer_df=transformed_tables.get("customer_master_clean"),
                    equipment_df=transformed_tables.get("equipment_master_clean"),
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