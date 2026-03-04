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
        schedule_name=None,
        frequency="manual",
        run_ts=None,
        timezone=None,
        priority_nbr=1,
        active_flag=1,
        pipeline_run_id=None
    ):

        run_id = None

        try:
            logger.info(f"Pipeline started | table: {table_name} | pipeline_run_id={pipeline_run_id}")

            # ======================================================
            # 1️⃣ SOURCE
            # ======================================================
            data_source_id = self.pipeline_manager.add_or_get_source(
                source_name=source_name,
                source_type=source_type,
                connection_text=file_path or (db_query["connection_str"] if db_query else None)
            )

            # ======================================================
            # 2️⃣ SCHEDULE
            # ======================================================
            schedule_id = self.pipeline_manager.add_or_get_schedule(
                source_id=data_source_id,
                schedule_name=schedule_name or f"manual_{table_name}",
                frequency=frequency,
                run_ts=run_ts,
                timezone=timezone,
                priority_nbr=priority_nbr,
                active_flag=active_flag
            )

            # ======================================================
            # 3️⃣ BATCH
            # ======================================================
            batch_id = self.pipeline_manager.add_batch(
                schedule_id=schedule_id,
                batch_name=table_name,
                batch_type=batch_type,
                priority_nbr=priority_nbr,
                active_flag=active_flag
            )

            # ======================================================
            # 4️⃣ BRONZE
            # ======================================================
            bronze_id = self.pipeline_manager.add_or_get_source(
                source_name="Bronze",
                source_type="folder",
                connection_text=f"artifacts/bronze/{table_name}"
            )

            task_name = f"ingesting {table_name} to bronze"
            run_id = self.pipeline_manager.start_task(
                source_id=data_source_id,
                target_id=bronze_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name,
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

            self.pipeline_manager.complete_task(run_id)

            # ======================================================
            # 5️⃣ SILVER
            # ======================================================
            silver_id = self.pipeline_manager.add_or_get_source(
                source_name="Silver",
                source_type="folder",
                connection_text=f"artifacts/silver/{table_name}"
            )

            task_name = f"bronze to silver {table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=bronze_id,
                target_id=silver_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name,
                pipeline_run_id=pipeline_run_id
            )

            # Validate Bronze -> Silver
            validated_tables = self.silver_validator.validate(
                df=bronze_df,
                table_name=table_name,
                source_file=file_path,
                pipeline_run_id=pipeline_run_id
            )

            # Transform Silver
            transformed_tables = self.silver_transformer.transform(
                validated_tables=validated_tables,
                table_name=table_name,
                pipeline_run_id=pipeline_run_id
            )

            self.pipeline_manager.complete_task(run_id)

            # ======================================================
            # 6️⃣ GOLD
            # ======================================================
            gold_id = self.pipeline_manager.add_or_get_source(
                source_name="Gold",
                source_type="folder",
                connection_text=f"artifacts/gold/{table_name}"
            )

            task_name = f"silver to gold {table_name}"
            run_id = self.pipeline_manager.start_task(
                source_id=silver_id,
                target_id=gold_id,
                schedule_id=schedule_id,
                batch_id=batch_id,
                task_name=task_name,
                pipeline_run_id=pipeline_run_id
            )

            # Aggregate Gold using all three silver tables
            self.gold.aggregate(
                rental_df=transformed_tables.get("all"),                   # rental_transaction_all
                customer_df=transformed_tables.get("customer_clean"),       # customer_master_clean
                equipment_df=transformed_tables.get("equipment_clean"),     # equipment_master_clean
                pipeline_run_id=pipeline_run_id
            )

            self.pipeline_manager.complete_task(run_id)

            logger.info(f"Pipeline completed successfully | table: {table_name} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Pipeline failed | table: {table_name} | error: {str(e)}")

            if run_id:
                self.pipeline_manager.fail_task(run_id, str(e))

            raise PipelineManagerException(
                f"Medallion pipeline execution failed: {str(e)}"
            )