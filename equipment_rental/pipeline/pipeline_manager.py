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

    def run(self, source_name, source_type, table_name, file_path=None, db_query=None, batch_type="full", schedule_name=None, frequency=None):
        """
        Run full medallion pipeline with process-level task tracking.
        """
        try:
            # -------- Source --------
            source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                source_type=source_type,
                connection_text=file_path or (db_query["connection_str"] if db_query else None)
            )

            # -------- Schedule --------
            schedule_id = self.pipeline_manager.add_schedule(
                source_id=source_id,
                schedule_name=schedule_name or f"manual_{table_name}",
                frequency=frequency or "manual"
            )

            # -------- Batch --------
            batch_id = self.pipeline_manager.add_batch(
                schedule_id=schedule_id,
                batch_name=table_name,
                batch_type=batch_type
            )

            # -------- Bronze Ingestion --------
            artifact_bronze_id = self.pipeline_manager.add_or_get_source(
                source_name=f"bronze_{table_name}",
                source_type="artifact",
                connection_text=f"artifacts/bronze/{table_name}.parquet"
            )

            task_name = f"data_to_bronze_{table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=source_id,
                target_id=artifact_bronze_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )

            if source_type == "db" and db_query:
                bronze_df, _ = self.bronze.ingest_db(**db_query)
            elif source_type == "excel" and file_path:
                bronze_df, _ = self.bronze.ingest_excel(file_path, table_name)
            else:
                raise ValueError("Invalid source type or missing file path/db query")

            self.pipeline_manager.complete_task(run_id)

            # -------- Silver Validation --------
            validated_tables = self.silver_validator.validate(
                df=bronze_df,
                table_name=table_name,
                source_file=file_path
            )

            # -------- Silver Transformation --------
            artifact_silver_id = self.pipeline_manager.add_or_get_source(
                source_name=f"silver_{table_name}",
                source_type="artifact",
                connection_text=f"artifacts/silver/{table_name}.parquet"
            )
            task_name = f"bronze_to_silver_{table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=artifact_bronze_id,
                target_id=artifact_silver_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )

            transformed_tables = self.silver_transformer.transform(
                validated_tables=validated_tables,
                table_name=table_name
            )
            self.pipeline_manager.complete_task(run_id)

            # -------- Gold Aggregation --------
            artifact_gold_id = self.pipeline_manager.add_or_get_source(
                source_name=f"gold_{table_name}",
                source_type="artifact",
                connection_text=f"artifacts/gold/{table_name}.parquet"
            )
            task_name = f"silver_to_gold_{table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=artifact_silver_id,
                target_id=artifact_gold_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name
            )

            for tname, df in transformed_tables.items():
                self.gold.aggregate(df)
            self.pipeline_manager.complete_task(run_id)

        except Exception as e:
            self.pipeline_manager.fail_task(run_id, str(e))
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")