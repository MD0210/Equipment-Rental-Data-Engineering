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

    def run(self, source_name: str, source_type: str, table_name: str,
            file_path: str = None, db_query: dict = None, batch_type: str = "full", rerun_id: int = None):
        """
        Run the full Medallion pipeline for a single table/sheet and track via PipelineManager.
        Supports Excel and DB sources.
        """

        # Use rerun_id if provided
        if rerun_id:
            pipeline_run_id = rerun_id
            logger.info(f"Rerunning failed pipeline tasks | run_id: {pipeline_run_id}")
        else:
            pipeline_run_id = self.pipeline_manager.start_task(
                source=source_name, batch_type=batch_type, task_name=table_name
            )

        try:
            logger.info(f"Pipeline started | run_id: {pipeline_run_id}")

            # -------- Bronze Ingestion --------
            if source_type == "db" and db_query:
                bronze_df, source_file = self.bronze.ingest_db(
                    connection_str=db_query["connection_str"],
                    query=db_query["query"],
                    table_name=db_query["table_name"]
                )
            elif source_type == "excel" and file_path:
                bronze_df, source_file = self.bronze.ingest_excel(
                    file_path=file_path,
                    sheet_name=table_name
                )
            else:
                raise ValueError("Invalid source_type or missing file_path/db_query")

            logger.info(f"Bronze ingestion complete | rows: {len(bronze_df)} | table: {table_name}")

            # -------- Silver Validation --------
            validated_tables = self.silver_validator.validate(
                df=bronze_df,
                table_name=table_name,
                source_file=source_file,
                pipeline_run_id=pipeline_run_id
            )

            # Use appropriate key depending on table type
            if table_name.lower() == "rental_transactions":
                validated_df = validated_tables["all"]
            else:
                validated_df = validated_tables["clean"]  # For master tables

            logger.info(f"Silver validation complete | rows: {len(validated_df)} | table: {table_name}")

            # -------- Silver Transformation --------
            transformed_df = self.silver_transformer.transform_rentals(
                validated_df, source_file
            )
            logger.info(f"Silver transformation complete | rows: {len(transformed_df)} | table: {table_name}")

            # -------- Gold Aggregation (only for Rental_Transactions) --------
            if table_name.lower() == "rental_transactions":
                self.gold.aggregate(transformed_df)
                logger.info(f"Gold aggregation complete | table: {table_name}")

            # -------- Complete Task --------
            self.pipeline_manager.complete_task(pipeline_run_id)
            logger.info(f"Pipeline completed successfully | run_id: {pipeline_run_id}")

        except Exception as e:
            self.pipeline_manager.fail_task(pipeline_run_id, str(e))
            logger.error(f"Pipeline failed | run_id: {pipeline_run_id} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")