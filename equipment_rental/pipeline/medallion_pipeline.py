# equipment_rental/pipeline/medallion_pipeline.py
from datetime import datetime
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException
from equipment_rental.components.quarantine_handler import QuarantineHandler

logger = get_logger()


class MedallionPipeline:
    def __init__(self):
        self.bronze = BronzeIngestion()
        self.silver_validator = SilverValidation()
        self.silver_transformer = SilverTransformation()
        self.gold = GoldAggregation()
        self.quarantine_handler = QuarantineHandler()
        self.pipeline_manager = PipelineManager()

    def run(self, source_name, source_type, table_name, file_path=None, db_query=None, batch_type="full", schedule_name=None, frequency=None):
        """
        Runs the full medallion pipeline with process-level task tracking.
        Each process step is a separate task: data->bronze, bronze->silver, silver->gold
        """
        try:
            logger.info(f"Pipeline started | table: {table_name}")

            # ---------------- Source ----------------
            connection_text = file_path if source_type=="excel" else db_query.get("connection_str", "")
            source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                connection_text=connection_text
            )

            # ---------------- Schedule ----------------
            schedule_id = self.pipeline_manager.add_schedule(
                source_id=source_id,
                schedule_name=schedule_name or f"{table_name}_schedule",
                frequency=frequency or "manual"
            )

            # ---------------- Batch ----------------
            batch_id = self.pipeline_manager.add_batch(
                schedule_id=schedule_id,
                batch_name=table_name,
                batch_type=batch_type
            )

            # ---------------- Step 1: Data -> Bronze ----------------
            task_name = f"Data to Bronze {table_name}"
            task_run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=source_id,  # Bronze stored under same source_id for simplicity
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )
            # Bronze ingestion
            if source_type == "excel":
                bronze_df, source_file = self.bronze.ingest_excel(file_path=file_path, sheet_name=table_name)
            else:
                bronze_df, source_file = self.bronze.ingest_db(
                    connection_str=db_query["connection_str"],
                    query=db_query["query"],
                    table_name=db_query["table_name"]
                )
            self.pipeline_manager.complete_task(task_run_id)

            # ---------------- Step 2: Bronze -> Silver ----------------
            task_name = f"Bronze to Silver {table_name}"
            task_run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=source_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )
            validated_tables = self.silver_validator.validate(
                df=bronze_df,
                table_name=table_name,
                source_file=source_file,
                pipeline_run_id=task_run_id
            )
            # Handle quarantine if needed
            quarantine_df = validated_tables.get("quarantine")
            if quarantine_df is not None and not quarantine_df.empty:
                self.quarantine_handler.save_quarantine(quarantine_df, table_name=table_name, pipeline_run_id=task_run_id)
            transformed_tables = self.silver_transformer.transform(
                validated_tables=validated_tables,
                table_name=table_name,
                pipeline_run_id=task_run_id
            )
            self.pipeline_manager.complete_task(task_run_id)

            # ---------------- Step 3: Silver -> Gold ----------------
            task_name = f"Silver to Gold {table_name}"
            task_run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=source_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )
            for tname, df in transformed_tables.items():
                self.gold.aggregate(df)
            self.pipeline_manager.complete_task(task_run_id)

            logger.info(f"Pipeline completed successfully | table: {table_name}")

        except Exception as e:
            self.pipeline_manager.fail_task(task_run_id, str(e))
            logger.error(f"Pipeline failed | table: {table_name} | error: {str(e)}")
            raise PipelineManagerException(f"Pipeline execution failed: {str(e)}")