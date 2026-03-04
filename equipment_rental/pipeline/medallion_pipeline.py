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

    def run(self, file_path, stage, pipeline_run_id=None):
        """Run pipeline stage for a file"""
        task_id = None
        try:
            logger.info(f"Pipeline stage started | file: {file_path} | stage: {stage} | pipeline_run_id={pipeline_run_id}")

            # --------------------
            # Source registration
            # --------------------
            source_id = self.pipeline_manager.add_or_get_source(file_path)

            # --------------------
            # Bronze Stage
            # --------------------
            if stage == "bronze":
                target_path = os.path.join(BRONZE_DIR, os.path.basename(file_path))
                target_id = self.pipeline_manager.add_or_get_source(target_path)

                task_id = self.pipeline_manager.start_task(source_id, target_id, stage, pipeline_run_id)

                # Ingest data
                ext = os.path.splitext(file_path)[1].lower()
                if ext == ".csv":
                    df, _ = self.bronze.ingest_csv(file_path, pipeline_run_id=pipeline_run_id)
                elif ext in [".xlsx", ".xls"]:
                    df, _ = self.bronze.ingest_excel(file_path, sheet_name=None, pipeline_run_id=pipeline_run_id)
                else:
                    raise ValueError(f"Unsupported file type: {ext}")

                self.pipeline_manager.complete_task(task_id)
                return df

            # --------------------
            # Silver Stage
            # --------------------
            elif stage == "silver":
                bronze_file = os.path.join(BRONZE_DIR, os.path.basename(file_path))
                if not os.path.exists(bronze_file):
                    raise ValueError(f"Bronze file not found: {bronze_file}")

                target_path = os.path.join(SILVER_DIR, os.path.basename(file_path))
                target_id = self.pipeline_manager.add_or_get_source(target_path)
                task_id = self.pipeline_manager.start_task(source_id, target_id, stage, pipeline_run_id)

                df = pd.read_csv(bronze_file)
                validated = self.silver_validator.validate(df, os.path.basename(file_path), pipeline_run_id=pipeline_run_id)
                transformed = self.silver_transformer.transform(validated, os.path.basename(file_path), pipeline_run_id=pipeline_run_id)

                # Save transformed tables
                for key, df_t in transformed.items():
                    save_path = os.path.join(SILVER_DIR, f"{os.path.splitext(os.path.basename(file_path))[0]}_{key}.csv")
                    df_t.to_csv(save_path, index=False)

                self.pipeline_manager.complete_task(task_id)
                return transformed

            # --------------------
            # Gold Stage
            # --------------------
            elif stage == "gold":
                target_path = os.path.join(GOLD_DIR, os.path.basename(file_path))
                target_id = self.pipeline_manager.add_or_get_source(target_path)
                task_id = self.pipeline_manager.start_task(source_id, target_id, stage, pipeline_run_id)

                # Load all silver files
                silver_files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".csv")]
                transformed_tables = {}
                for f in silver_files:
                    key = os.path.splitext(f)[0]
                    transformed_tables[key] = pd.read_csv(os.path.join(SILVER_DIR, f))

                self.gold.aggregate(
                    rental_df=transformed_tables.get("Rental_Transactions_all") if "Rental_Transactions" in file_path else None,
                    customer_df=transformed_tables.get("Customer_Master_clean"),
                    equipment_df=transformed_tables.get("Equipment_Master_clean"),
                    pipeline_run_id=pipeline_run_id
                )

                self.pipeline_manager.complete_task(task_id)

            else:
                raise ValueError(f"Invalid stage: {stage}")

            logger.info(f"Pipeline stage completed | file: {file_path} | stage: {stage} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            if task_id:
                self.pipeline_manager.fail_task(task_id, str(e))
            logger.error(f"Pipeline stage failed | file: {file_path} | stage: {stage} | error: {str(e)}")
            raise PipelineManagerException(f"Medallion pipeline execution failed: {str(e)}")