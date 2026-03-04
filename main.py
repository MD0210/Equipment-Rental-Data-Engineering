# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
from datetime import datetime

logger = get_logger()


def run_pipeline_from_db():
    pm = PipelineManager()  # DB is initialized automatically
    pipeline = MedallionPipeline()

    # Example pipelines to run per source
    default_tables = ["Rental_Transactions", "Customer_Master", "Equipment_Master"]

    # Example data: you could read this from a config CSV/JSON if needed
    pipelines_to_run = [
        {
            "source_name": "Equipment_Hire_Dataset",
            "source_type": "excel",
            "connection_text": "data/Equipment_Hire_Dataset.xlsx",
            "schedule_name": "equip_sched",
            "run_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "frequency": "daily",
            "batch_name": "daily_batch",
            "batch_type": "full",
        }
    ]

    # Generate ONE pipeline_run_id for this execution
    pipeline_run_id = pm.create_pipeline_run()

    for config in pipelines_to_run:
        # -----------------------------
        # 1️⃣ Ensure Source exists
        # -----------------------------
        source_id = pm.add_or_get_source(
            config["source_name"],
            config["source_type"],
            config["connection_text"]
        )

        # -----------------------------
        # 2️⃣ Ensure Schedule exists
        # -----------------------------
        schedule_id = pm.add_or_get_schedule(
            source_id=source_id,
            schedule_name=config["schedule_name"],
            frequency=config.get("frequency"),
            run_ts=config.get("run_ts"),
            timezone=config.get("timezone"),
            priority_nbr=1,
            active_flag=1
        )

        # -----------------------------
        # 3️⃣ Ensure Batch exists
        # -----------------------------
        batch_id = pm.add_batch(
            schedule_id=schedule_id,
            batch_name=config["batch_name"],
            batch_type=config.get("batch_type", "full"),
            priority_nbr=1,
            active_flag=1
        )

        logger.info(f"Running pipeline | source: {config['source_name']} | schedule: {config['schedule_name']} | batch: {config['batch_name']}")

        try:
            for table_name in default_tables:
                logger.info(f"Running pipeline for table: {table_name}")
                pipeline.run(
                    source_name=config["source_name"],
                    source_type=config["source_type"],
                    table_name=table_name,
                    file_path=config["connection_text"],
                    schedule_name=config["schedule_name"],
                    run_ts=config["run_ts"],
                    timezone=config.get("timezone"),
                    frequency=config.get("frequency"),
                    priority_nbr=1,
                    active_flag=1,
                    batch_type=config.get("batch_type", "full"),
                    pipeline_run_id=pipeline_run_id
                )

        except Exception as e:
            logger.error(
                f"Pipeline failed | source: {config['source_name']} | "
                f"schedule: {config['schedule_name']} | batch: {config['batch_name']} | error: {str(e)}"
            )
            continue

    logger.info(f"All pipelines completed | pipeline_run_id={pipeline_run_id}")


if __name__ == "__main__":
    run_pipeline_from_db()