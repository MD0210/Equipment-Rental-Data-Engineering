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

    def run(
        self,
        source_name,
        source_type,
        table_name,
        file_path=None,
        db_query=None,
        batch_type="full",
        schedule_id=None,
        batch_id=None,
        frequency="manual",
        run_ts=None,
        timezone=None,
        priority_nbr=1,
        active_flag=1,
        pipeline_run_id=None
    ):
        """
        Runs the Medallion pipeline for a given table.
        This version does NOT insert schedules or batches.
        Assumes schedule_id and batch_id are already provided.
        """
        run_id = None

        try:
            logger.info(f"Pipeline started | table: {table_name} | pipeline_run_id={pipeline_run_id}")

            # =========================
            # SOURCE
            # =========================
            data_source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                source_type=source_type,
                connection_text=file_path or (db_query["connection_str"] if db_query else None)
            )

            # =========================
            #  BRONZE INGESTION
            # =========================
            stage = "bronze"
            bronze_id = self.pipeline_manager.add_or_get_source(
                source_name="Bronze",
                source_type="folder",
                connection_text=f"artifacts/bronze/{table_name}"
            )

            run_id = self.pipeline_manager.start_task(
                source_id=bronze_id,
                target_id=silver_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=f"{table_name}_{stage}",   # <-- pass task_name
                pipeline_run_id=pipeline_run_id,
                table_name=table_name,
                stage=stage
            )
            # Ingest data (db/excel/csv)
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

            self.pipeline_manager.complete_task(run_id)

            # =========================
            # SILVER VALIDATION & TRANSFORMATION
            # =========================
            stage = "silver"
            silver_id = self.pipeline_manager.add_or_get_source(
                source_name="Silver",
                source_type="folder",
                connection_text=f"artifacts/silver/{table_name}"
            )

            run_id = self.pipeline_manager.start_task(
                source_id=bronze_id,
                target_id=silver_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                table_name=table_name,
                stage=stage,
                pipeline_run_id=pipeline_run_id
            )

            # validate + transform
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

            self.pipeline_manager.complete_task(run_id)

            # =========================
            # GOLD AGGREGATION
            stage = "gold"
            gold_id = self.pipeline_manager.add_or_get_source(
                source_name="Gold",
                source_type="folder",
                connection_text=f"artifacts/gold/{table_name}"
            )

            run_id = self.pipeline_manager.start_task(
                source_id=silver_id,
                target_id=gold_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                table_name=table_name,
                stage=stage,
                pipeline_run_id=pipeline_run_id
            )

            # aggregation
            if table_name.lower() == "rental_transactions":
                self.gold.aggregate(
                    rental_df=transformed_tables.get("all"),
                    customer_df=transformed_tables.get("customer_master_clean"),
                    equipment_df=transformed_tables.get("equipment_master_clean"),
                    pipeline_run_id=pipeline_run_id
                )

            self.pipeline_manager.complete_task(run_id)
            logger.info(f"Pipeline completed successfully | table: {table_name} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Pipeline failed | table: {table_name} | error: {str(e)}")
            if run_id:
                self.pipeline_manager.fail_task(run_id, str(e))
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")