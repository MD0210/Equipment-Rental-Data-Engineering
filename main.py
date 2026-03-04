# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3

logger = get_logger()


def run_pipeline_from_db():
    pm = PipelineManager()
    pipeline = MedallionPipeline()

    # Fetch active schedules & batches
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.frequency, s.run_ts, s.timezone,
                   src.source_name, src.source_type, src.connection_text,
                   b.batch_name
            FROM schedule s
            JOIN source src ON s.source_id = src.source_id
            JOIN batch b ON s.schedule_id = b.schedule_id
            WHERE s.active_flag=1 AND b.active_flag=1
        """)
        rows = cursor.fetchall()

    if not rows:
        logger.info("No active schedules/batches found. Exiting.")
        return

    # ONE pipeline_run_id for entire execution
    pipeline_run_id = pm.create_pipeline_run()

    for row in rows:
        frequency, run_ts, timezone, \
        source_name, source_type, connection_text, \
        batch_name = row

        logger.info(f"Running pipeline | source: {source_name} | batch: {batch_name}")

        try:
            tables = ["Rental_Transactions", "Customer_Master", "Equipment_Master"]

            for table_name in tables:
                logger.info(f"Processing table: {table_name}")

                # ---------- BRONZE ----------
                bronze_df = pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage="bronze",
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id
                )

                # ---------- SILVER ----------
                transformed_tables = pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage="silver",
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id
                )

                # ---------- GOLD ----------
                pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage="gold",
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id
                )

        except Exception as e:
            logger.error(
                f"Pipeline failed | source: {source_name} | batch: {batch_name} | error: {str(e)}"
            )
            continue

    logger.info(f"All pipelines completed | pipeline_run_id={pipeline_run_id}")


if __name__ == "__main__":
    run_pipeline_from_db()