from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def run_interactive_pipeline():

    tables_input = input("Enter tables (comma-separated): ").strip()
    tables = [t.strip() for t in tables_input.split(",")]

    source_name = input("Enter source name (e.g., Equipment_Hire_Dataset): ").strip()
    source_type = input("Enter source type (excel/db/api): ").strip().lower()
    file_path = input("Enter file path or connection string: ").strip()
    schedule_name = input("Enter schedule name: ").strip()
    run_ts = input("Enter run timestamp (YYYY-MM-DD HH:MM:SS) or leave blank: ").strip() or None
    timezone = input("Enter timezone (e.g., UTC) or leave blank: ").strip() or None
    frequency = input("Enter frequency (daily, weekly, manual) or leave blank: ").strip() or "manual"
    priority_nbr = int(input("Enter priority number (default 1): ").strip() or 1)
    active_flag = int(input("Enter active flag (1=active, 0=inactive): ").strip() or 1)
    batch_type = input("Enter batch type (full/incremental, default full): ").strip() or "full"

    pipeline = MedallionPipeline()
    pm = PipelineManager()

    # ---------------------------------------------------
    # 🔥 Generate ONE pipeline_run_id for entire batch
    # ---------------------------------------------------
    pipeline_run_id = pm.generate_pipeline_run_id()

    logger.info(f"Starting pipeline batch | pipeline_run_id={pipeline_run_id}")

    for table_name in tables:
        logger.info(f"Running pipeline for table: {table_name}")

        pipeline.run(
            source_name=source_name,
            source_type=source_type,
            table_name=table_name,
            file_path=file_path,
            schedule_name=schedule_name,
            run_ts=run_ts,
            timezone=timezone,
            frequency=frequency,
            priority_nbr=priority_nbr,
            active_flag=active_flag,
            batch_type=batch_type,
            pipeline_run_id=pipeline_run_id   # 🔥 PASS IT HERE
        )

    logger.info(f"Pipeline batch completed | pipeline_run_id={pipeline_run_id}")


if __name__ == "__main__":
    run_interactive_pipeline()