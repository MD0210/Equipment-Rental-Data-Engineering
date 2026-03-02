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

    def run(self, source_name: str, file_path: str, sheet_name: str = None, db_query: dict = None):
        """
        Run the full Medallion pipeline and track via PipelineManager.
        
        Parameters:
        - source_name: str, name of the data source
        - file_path: str, path to Excel file (if using Excel)
        - sheet_name: str, optional, Excel sheet name
        - db_query: dict, optional, {"connection_str":..., "query":..., "table_name":...} if source is DB
        """
        pipeline_run_id = self.pipeline_manager.start_task(
            source=source_name, batch_type="full", task_name="medallion_pipeline"
        )

        try:
            logger.info(f"Pipeline started | run_id: {pipeline_run_id}")

            # -------- Bronze Ingestion --------
            if db_query:
                bronze_df, source_file = self.bronze.ingest_db(
                    connection_str=db_query["connection_str"],
                    query=db_query["query"],
                    table_name=db_query["table_name"]
                )
            else:
                bronze_df, source_file = self.bronze.ingest_excel(
                    file_path=file_path,
                    sheet_name=sheet_name
                )
            logger.info(f"Bronze ingestion complete | rows: {len(bronze_df)}")

            # -------- Silver Validation --------
            validated_tables = self.silver_validator.validate_rentals(bronze_df, source_file)
            validated_df = validated_tables["all"]
            logger.info(f"Silver validation complete | rows: {len(validated_df)}")

            # -------- Silver Transformation --------
            transformed_df = self.silver_transformer.transform_rentals(validated_df, source_file)
            logger.info(f"Silver transformation complete | rows: {len(transformed_df)}")

            # -------- Gold Aggregation --------
            self.gold.aggregate(transformed_df)
            logger.info("Gold aggregation complete")

            # -------- Complete Task --------
            self.pipeline_manager.complete_task(pipeline_run_id)
            logger.info(f"Pipeline completed successfully | run_id: {pipeline_run_id}")

        except Exception as e:
            # Log failure in PipelineManager and raise custom exception
            self.pipeline_manager.fail_task(pipeline_run_id, str(e))
            logger.error(f"Pipeline failed | run_id: {pipeline_run_id} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")