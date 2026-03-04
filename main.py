# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
from datetime import datetime

logger = get_logger()


def run_pipeline_from_db():
    pm = PipelineManager()  # DB is initialized automatically
    pipeline = MedallionPipeline()

    import sqlite3

    # Connect to DB and fetch active sources, schedules, batches
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.schedule_id, s.schedule_name, s.frequency, s.run_ts, s.timezone, 
                   src.source_name, src.source_type, src.connection_text,
                   b.batch_id, b.batch_name, b.batch_type
            FROM schedule s
            JOIN source src ON s.source_id = src.source_id
            JOIN batch b ON s.schedule_id = b.schedule_id
            WHERE s.active_flag=1 AND b.active_flag=1
        """)
        rows = cursor.fetchall()

    if not rows:
        logger.info("No active schedules/batches found. Exiting.")
        return

    # Generate ONE pipeline_run_id for this full execution
    pipeline_run_id = pm.create_pipeline_run()

    for row in rows:
        schedule_id, schedule_name, frequency, run_ts, timezone, \
        source_name, source_type, connection_text, \
        batch_id, batch_name, batch_type = row

        logger.info(f"Running pipeline | source: {source_name} | schedule: {schedule_name} | batch: {batch_name}")

        try:
            # Here we call MedallionPipeline.run for each source/table
            # You may want to customize which tables are included per source
            # For now, we'll assume all tables are ingested from the source
            tables = ["Rental_Transactions", "Customer_Master", "Equipment_Master"]  # or fetch dynamically

            for table_name in tables:
                logger.info(f"Running pipeline for table: {table_name}")
                pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    file_path=connection_text,
                    schedule_name=schedule_name,
                    run_ts=run_ts,
                    timezone=timezone,
                    frequency=frequency,
                    priority_nbr=1,  # default, can fetch from DB if needed
                    active_flag=1,   # default
                    batch_type=batch_type,
                    pipeline_run_id=pipeline_run_id
                )

        except Exception as e:
            logger.error(f"Pipeline failed | source: {source_name} | schedule: {schedule_name} | batch: {batch_name} | error: {str(e)}")
            continue

    logger.info(f"All pipelines completed | pipeline_run_id={pipeline_run_id}")


if __name__ == "__main__":
    run_pipeline_from_db()