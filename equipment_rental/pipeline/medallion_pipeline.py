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
        self.pipeline_manager = PipelineManager()
        self.quarantine_handler = QuarantineHandler()

    def run(
        self,
        source_name: str,
        source_type: str,
        table_name: str,
        file_path: str = None,
        db_query: dict = None,
        batch_type: str = "full",
        rerun_id: str = None,
        schedule: str = None
    ):
        """
        Run Medallion pipeline for a single table/sheet.

        Parameters:
        - source_name: str
        - source_type: str -> "excel" or "db"
        - table_name: str
        - file_path: str
        - db_query: dict
        - batch_type: str -> "full" or "incremental"
        - rerun_id: str -> optional run_id to rerun
        - schedule: str -> optional cron expression
        """

        if rerun_id:
            run_id = rerun_id
            logger.info(f"Rerunning failed pipeline | run_id: {run_id}")
        else:
            run_id = self.pipeline_manager.start_task(
                source=source_name,
                batch_type=batch_type,
                task_name=table_name
            )

        try:
            logger.info(f"Pipeline started | run_id: {run_id} | table: {table_name}")

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
                bronze_df, table_name=table_name, source_file=source_file, pipeline_run_id=run_id
            )
            logger.info(f"Silver validation complete | table: {table_name}")

            # Handle quarantine rows
            if table_name.lower() == "rental_transactions":
                quarantine_df = validated_tables.get("quarantine")
                if quarantine_df is not None and not quarantine_df.empty:
                    self.quarantine_handler.save_quarantine(
                        df=quarantine_df,
                        table_name=table_name,
                        pipeline_run_id=run_id
                    )
                    logger.warning(f"{len(quarantine_df)} rows quarantined | table: {table_name}")

            # -------- Silver Transformation --------
            transformed_df = self.silver_transformer.transform(
                validated_tables, table_name, source_file, pipeline_run_id=run_id
            )
            logger.info(f"Silver transformation complete | table: {table_name}")

            # -------- Gold Aggregation --------
            self.gold.aggregate(transformed_df)
            logger.info(f"Gold aggregation complete | table: {table_name}")

            # -------- Complete Task --------
            self.pipeline_manager.complete_task(run_id)
            logger.info(f"Pipeline completed successfully | run_id: {run_id}")

        except Exception as e:
            self.pipeline_manager.fail_task(run_id, str(e))
            logger.error(f"Pipeline failed | run_id: {run_id} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")