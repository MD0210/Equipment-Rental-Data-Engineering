# equipment_rental/pipeline/medallion_pipeline.py
from datetime import datetime
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException

logger = get_logger()


class MedallionPipeline:
    def __init__(self):
        self.bronze = BronzeIngestion()
        self.silver_validator = SilverValidation()
        self.silver_transformer = SilverTransformation()
        self.gold = GoldAggregation()
        self.pipeline_manager = PipelineManager()

    def run(self, source_name, source_type, table_name, file_path=None, db_query=None,
            batch_type="full", schedule=None):
        """
        Execute full medallion pipeline for one table
        """
        try:
            # --- Register source ---
            source_id = self.pipeline_manager.add_or_get_source(
                source_name=f"{source_name}_{table_name}_bronze",
                source_type="artifact",
                connection_text=file_path or (db_query or {}).get("connection_str")
            )
            target_id = self.pipeline_manager.add_or_get_source(
                source_name=f"{source_name}_{table_name}_silver",
                source_type="artifact",
                connection_text=f"{table_name}_silver"
            )

            # --- Schedule ---
            schedule_id = None
            if schedule:
                schedule_id = self.pipeline_manager.add_schedule(
                    source_id=source_id,
                    schedule_name=schedule,
                    frequency="daily"
                )

            # --- Batch ---
            batch_id = self.pipeline_manager.add_batch(
                schedule_id=schedule_id,
                batch_name=table_name,
                batch_type=batch_type
            )

            logger.info(f"Pipeline started | table: {table_name}")

            # ---------------- Bronze ----------------
            task_name = f"data_to_bronze_{table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=source_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )

            if source_name == "excel" and file_path:
                bronze_df, source_file = self.bronze.ingest_excel(file_path=file_path, sheet_name=table_name)
            elif source_name == "db" and db_query:
                bronze_df, source_file = self.bronze.ingest_db(
                    connection_str=db_query["connection_str"],
                    query=db_query["query"],
                    table_name=table_name
                )
            else:
                raise ValueError("Invalid source")

            self.pipeline_manager.complete_task(run_id)

            # ---------------- Silver ----------------
            task_name = f"bronze_to_silver_{table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=target_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )

            validated_tables = self.silver_validator.validate(
                df=bronze_df,
                table_name=table_name,
                source_file=source_file,
                pipeline_run_id=run_id
            )

            transformed_tables = self.silver_transformer.transform(
                validated_tables=validated_tables,
                table_name=table_name,
                pipeline_run_id=run_id
            )

            self.pipeline_manager.complete_task(run_id)

            # ---------------- Gold ----------------
            gold_target_id = self.pipeline_manager.add_or_get_source(
                source_name=f"{source_name}_{table_name}_gold",
                source_type="artifact",
                connection_text=f"{table_name}_gold"
            )

            for tname, df in transformed_tables.items():
                task_name = f"silver_to_gold_{table_name}_{tname}"
                run_id = self.pipeline_manager.start_task(
                    source_id=target_id,
                    target_id=gold_target_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id,
                    task_name=task_name
                )
                self.gold.aggregate(df)
                self.pipeline_manager.complete_task(run_id)

            logger.info(f"Pipeline completed successfully | table: {table_name}")

        except Exception as e:
            if 'run_id' in locals():
                self.pipeline_manager.fail_task(run_id, str(e))
            logger.error(f"Pipeline failed | table: {table_name} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")