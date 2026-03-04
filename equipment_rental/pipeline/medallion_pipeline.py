# equipment_rental/pipeline/medallion_pipeline.py
import os
import pandas as pd
from equipment_rental.components.bronze_ingestion import BronzeIngestion
from equipment_rental.components.silver_validation import SilverValidation
from equipment_rental.components.silver_transformation import SilverTransformation
from equipment_rental.components.gold_aggregation import GoldAggregation
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import PipelineManagerException
from equipment_rental.constants.constants import BRONZE_DIR, SILVER_DIR, GOLD_DIR

logger = get_logger()

class MedallionPipeline:

    def __init__(self):
        self.bronze = BronzeIngestion()
        self.silver_validator = SilverValidation()
        self.silver_transformer = SilverTransformation()
        self.gold = GoldAggregation()
        self.pipeline_manager = PipelineManager()

        os.makedirs(BRONZE_DIR, exist_ok=True)
        os.makedirs(SILVER_DIR, exist_ok=True)
        os.makedirs(GOLD_DIR, exist_ok=True)

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
            logger.info(f"Pipeline stage started | table={table_name} | stage={stage} | pipeline_run_id={pipeline_run_id}")

            # --------------------
            # Stage Sources
            # --------------------
            if stage == "bronze":
                data_source_id = self.pipeline_manager.add_or_get_source(
                    source_name=source_name,
                    source_type=source_type,
                    connection_text=file_path or (db_query["connection_str"] if db_query else None)
                )
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"Bronze_{table_name}",
                    source_type="folder",
                    connection_text=os.path.join(BRONZE_DIR, f"{table_name}.csv")
                )

            elif stage == "silver":
                data_source_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"Bronze_{table_name}",
                    source_type="folder",
                    connection_text=os.path.join(BRONZE_DIR, f"{table_name}.csv")
                )
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"Silver_{table_name}",
                    source_type="folder",
                    connection_text=os.path.join(SILVER_DIR, f"{table_name}_clean.csv")
                )

            elif stage == "gold":
                data_source_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"Silver_{table_name}",
                    source_type="folder",
                    connection_text=os.path.join(SILVER_DIR, f"{table_name}_clean.csv")
                )
                target_id = self.pipeline_manager.add_or_get_source(
                    source_name=f"Gold_{table_name}",
                    source_type="folder",
                    connection_text=os.path.join(GOLD_DIR, f"{table_name}_aggregated.csv")
                )
            else:
                raise ValueError(f"Invalid stage: {stage}")

            # --------------------
            # Start Task
            # --------------------
            task_id = self.pipeline_manager.start_task(
                source_id=data_source_id,
                target_id=target_id,
                stage=stage,
                table_name=table_name,
                pipeline_run_id=pipeline_run_id
            )

            # --------------------
            # Execute Stage
            # --------------------
            if stage == "bronze":
                # Ingest Data
                if source_type == "db" and db_query:
                    bronze_df, _ = self.bronze.ingest_db(db_query["connection_str"], db_query["query"], table_name, pipeline_run_id)
                elif source_type == "excel" and file_path:
                    bronze_df, _ = self.bronze.ingest_excel(file_path, sheet_name=table_name, pipeline_run_id=pipeline_run_id)
                elif source_type == "csv" and file_path:
                    bronze_df, _ = self.bronze.ingest_csv(file_path=file_path, pipeline_run_id=pipeline_run_id)
                else:
                    raise ValueError("Invalid source configuration for Bronze stage")

                # Ensure output is a DataFrame
                if not isinstance(bronze_df, pd.DataFrame):
                    bronze_df = pd.DataFrame(bronze_df)

                # Save Bronze CSV
                bronze_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")
                bronze_df.to_csv(bronze_path, index=False)
                self.pipeline_manager.complete_task(task_id)
                return bronze_df

            elif stage == "silver":
                bronze_path = os.path.join(BRONZE_DIR, f"{table_name}.csv")
                if not os.path.exists(bronze_path):
                    raise FileNotFoundError(f"Bronze data not found for table {table_name}")

                bronze_df = pd.read_csv(bronze_path)

                # Validate
                validated = self.silver_validator.validate(
                    bronze_df, table_name, source_file=file_path, pipeline_run_id=pipeline_run_id
                )

                # Transform
                transformed = self.silver_transformer.transform(
                    validated, table_name, pipeline_run_id=pipeline_run_id
                )

                # Flatten any nested dicts to DataFrames
                flat_transformed = {}
                for key, df in transformed.items():
                    if not isinstance(df, pd.DataFrame):
                        df = pd.DataFrame(df)
                    flat_transformed[key] = df
                    save_path = os.path.join(SILVER_DIR, f"{table_name.lower()}_{key}.csv")
                    df.to_csv(save_path, index=False)

                self.pipeline_manager.complete_task(task_id)
                return flat_transformed

            elif stage == "gold":
                # Load all silver CSVs
                transformed_tables = {}
                for f in os.listdir(SILVER_DIR):
                    if f.endswith(".csv"):
                        key = f.replace(".csv", "")
                        transformed_tables[key] = pd.read_csv(os.path.join(SILVER_DIR, f))

                self.gold.aggregate(
                    rental_df=transformed_tables.get("rental_transactions_all") if table_name.lower()=="rental_transactions" else None,
                    customer_df=transformed_tables.get("customer_master_clean"),
                    equipment_df=transformed_tables.get("equipment_master_clean"),
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)

            logger.info(f"Pipeline stage completed | table={table_name} | stage={stage} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(f"Pipeline stage failed | table={table_name} | stage={stage} | error={str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")