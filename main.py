# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3

logger = get_logger()
tables = ["Rental_Transactions","Customer_Master","Equipment_Master"]

# DAG: bronze -> silver -> gold
dag = {"bronze": [], "silver": ["bronze"], "gold": ["silver"]}

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

    # Keep track of completed tasks to respect DAG
    completed = {}

    for row in rows:
        frequency, run_ts, timezone, source_name, source_type, connection_text, batch_name = row
        logger.info(f"Running pipeline | source: {source_name} | batch: {batch_name}")

        for stage in ["bronze","silver","gold"]:
            for table_name in tables:
                # Skip stage if DAG dependencies failed
                if stage == "silver" and completed.get((table_name,"bronze")) != "success":
                    logger.info(f"Skipping silver | table: {table_name} | bronze not completed")
                    continue
                if stage == "gold" and any(completed.get((t,"silver")) != "success" for t in tables):
                    logger.info("Skipping gold | some silver tables not completed")
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
                    logger.error(f"Pipeline failed | source: {source_name} | batch: {batch_name} | table: {table_name} | stage: {stage} | error: {str(e)}")
                    completed[(table_name, stage)] = "failed"

    # --------------------
    # Rerun failed tasks (optional)
    # --------------------
    failed_tasks = pm.get_failed_tasks(pipeline_run_id)
    if failed_tasks:
        logger.info(f"Retrying failed tasks for pipeline_run_id={pipeline_run_id}")
        for stage, table_name in failed_tasks:
            # same DAG check as above
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
                logger.error(f"Failed again | table: {table_name} | stage: {stage} | error: {str(e)}")

    logger.info(f"Pipeline run completed | pipeline_run_id={pipeline_run_id}")


if __name__ == "__main__":
    run_pipeline_from_db()