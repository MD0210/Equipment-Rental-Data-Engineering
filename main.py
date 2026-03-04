from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3

logger = get_logger()
tables = ["Rental_Transactions","Customer_Master","Equipment_Master"]

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

    pipeline_run_id = pm.create_pipeline_run()
    completed = {}

    try:
        for row in rows:
            frequency, run_ts, timezone, source_name, source_type, connection_text, batch_name = row
            logger.info(f"Running pipeline | source: {source_name} | batch: {batch_name}")

            for stage in ["bronze","silver","gold"]:
                for table_name in tables:
                    if stage == "silver" and completed.get((table_name,"bronze")) != "success":
                        continue
                    if stage == "gold" and any(completed.get((t,"silver")) != "success" for t in tables):
                        continue

                    try:
                        pipeline.run(
                            source_name=source_name,
                            source_type=source_type,
                            table_name=table_name,
                            stage=stage,
                            file_path=connection_text,
                            pipeline_run_id=pipeline_run_id
                        )
                        completed[(table_name, stage)] = "success"
                    except Exception as e:
                        logger.error(f"Pipeline failed | table={table_name} | stage={stage} | error={str(e)}")
                        completed[(table_name, stage)] = "failed"

        # Retry failed tasks
        failed_tasks = pm.get_failed_tasks(pipeline_run_id)
        for stage, table_name in failed_tasks:
            if stage == "silver" and completed.get((table_name,"bronze")) != "success":
                continue
            if stage == "gold" and any(completed.get((t,"silver")) != "success" for t in tables):
                continue
            try:
                pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage=stage,
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id
                )
                completed[(table_name, stage)] = "success"
            except Exception as e:
                logger.error(f"Failed again | table={table_name} | stage={stage} | error={str(e)}")

        pm.complete_pipeline_run(pipeline_run_id)

    except Exception as e:
        pm.fail_pipeline_run(pipeline_run_id)
        logger.error(f"Pipeline run failed: {str(e)}")

if __name__ == "__main__":
    run_pipeline_from_db()