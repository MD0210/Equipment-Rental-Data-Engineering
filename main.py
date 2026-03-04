# main.py
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger
from datetime import datetime

logger = get_logger()


def run_pipeline_from_db():
    pm = PipelineManager()
    pm.init_db()  # ensure DB exists

    # 1️⃣ Get all active pipeline configurations
    active_batches = pm.conn.execute("""
        SELECT b.id AS batch_id,
               s.id AS schedule_id,
               src.id AS source_id,
               src.source_name,
               src.source_type,
               src.connection_text,
               b.table_name,
               s.schedule_name,
               s.frequency,
               s.timezone,
               s.active_flag
        FROM batches b
        JOIN sources src ON b.source_id = src.id
        JOIN schedules s ON b.schedule_id = s.id
        WHERE s.active_flag = 1
    """).fetchall()

    if not active_batches:
        logger.info("No active batches found in pipeline_manager.db")
        return

    # 2️⃣ Initialize Medallion pipeline
    pipeline = MedallionPipeline()

    # 3️⃣ Loop through each active batch
    for batch in active_batches:
        batch_id = batch["batch_id"]
        schedule_id = batch["schedule_id"]
        source_id = batch["source_id"]
        source_name = batch["source_name"]
        source_type = batch["source_type"]
        file_path = batch["connection_text"]
        table_name = batch["table_name"]
        frequency = batch["frequency"]
        timezone = batch["timezone"]
        schedule_name = batch["schedule_name"]

        # Generate a single pipeline_run_id per batch execution
        pipeline_run_id = pipeline.pipeline_manager.create_pipeline_run()

        logger.info(f"Starting pipeline | table: {table_name} | pipeline_run_id={pipeline_run_id}")

        try:
            pipeline.run(
                source_name=source_name,
                source_type=source_type,
                table_name=table_name,
                file_path=file_path,
                schedule_name=schedule_name,
                run_ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                timezone=timezone,
                frequency=frequency,
                priority_nbr=1,
                active_flag=1,
                batch_type="full",
                pipeline_run_id=pipeline_run_id
            )

            logger.info(f"Pipeline completed successfully | table: {table_name} | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Pipeline failed | table: {table_name} | pipeline_run_id={pipeline_run_id} | error: {e}")


if __name__ == "__main__":
    run_pipeline_from_db()