# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3

logger = get_logger()

def run_pipeline_from_db():
    pm = PipelineManager()
    pipeline = MedallionPipeline()

    # Fetch active schedules & sources
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.frequency, s.run_ts, s.timezone,
                   src.source_name, src.source_type, src.connection_text
            FROM schedule s
            JOIN source src ON s.source_id = src.source_id
            WHERE s.active_flag=1
        """)
        rows = cursor.fetchall()

    if not rows:
        logger.info("No active schedules found. Exiting.")
        return

    pipeline_run_id = pm.create_pipeline_run()
    completed = {}

    try:
        for row in rows:
            frequency, run_ts, timezone, source_name, source_type, connection_text = row
            table_name = source_name  # Use source_name as table_name
            logger.info(f"Starting pipeline for table/source: {table_name}")

            # Run stages sequentially: bronze → silver → gold
            for stage in ["bronze", "silver", "gold"]:
                # Skip stages if previous stage failed
                if stage == "silver" and completed.get((table_name, "bronze")) != "success":
                    logger.warning(f"Skipping Silver stage for {table_name} because Bronze failed")
                    continue
                if stage == "gold" and completed.get((table_name, "silver")) != "success":
                    logger.warning(f"Skipping Gold stage for {table_name} because Silver failed")
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
                    logger.info(f"Stage completed | table={table_name} | stage={stage}")

                except Exception as e:
                    completed[(table_name, stage)] = "failed"
                    logger.error(f"Pipeline stage failed | table={table_name} | stage={stage} | error={str(e)}")

        pm.complete_pipeline_run(pipeline_run_id)
        logger.info(f"Pipeline run completed | pipeline_run_id={pipeline_run_id}")

    except Exception as e:
        pm.fail_pipeline_run(pipeline_run_id)
        logger.error(f"Pipeline run failed | pipeline_run_id={pipeline_run_id} | error={str(e)}")


if __name__ == "__main__":
    run_pipeline_from_db()