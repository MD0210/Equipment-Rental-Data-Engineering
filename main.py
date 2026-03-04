# main.py
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3

logger = get_logger()


def run_pipeline_from_db():
    pm = PipelineManager()
    pipeline = MedallionPipeline()

    # Fetch active schedules & batches with priority
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.schedule_id, s.priority_nbr AS schedule_priority, s.frequency, s.run_ts, s.timezone,
                   src.source_name, src.source_type, src.connection_text,
                   b.batch_id, b.batch_name, b.priority_nbr AS batch_priority
            FROM schedule s
            JOIN source src ON s.source_id = src.source_id
            JOIN batch b ON s.schedule_id = b.schedule_id
            WHERE s.active_flag=1 AND b.active_flag=1
        """)
        rows = cursor.fetchall()

    if not rows:
        logger.info("No active schedules found. Exiting.")
        return

    # Sort first by schedule priority, then by batch priority
    rows_sorted = sorted(rows, key=lambda x: (x[1], x[10]))  # x[1]=schedule_priority, x[10]=batch_priority

    pipeline_run_id = pm.create_pipeline_run()
    completed = {}

    try:
        for row in rows_sorted:
            schedule_id, schedule_prio, frequency, run_ts, timezone, source_name, source_type, connection_text, batch_id, batch_name, batch_prio = row

            # Determine table_name
            if source_type.lower() == "excel":
                table_name = batch_name  # sheet_name must match batch name
            else:
                table_name = source_name

            # Normalize table_name for CSVs
            table_name = table_name.strip().replace(" ", "_")

            logger.info(f"Starting pipeline | table: {table_name} | batch_id: {batch_id} | schedule_prio: {schedule_prio} | batch_prio: {batch_prio}")

            for stage in ["bronze", "silver", "gold"]:
                # Skip stages if prior stage failed
                if stage == "silver" and completed.get((batch_id, "bronze")) != "success":
                    logger.warning(f"Skipping Silver stage for batch_id={batch_id} because Bronze failed")
                    continue
                if stage == "gold" and completed.get((batch_id, "silver")) != "success":
                    logger.warning(f"Skipping Gold stage for batch_id={batch_id} because Silver failed")
                    continue

                try:
                    pipeline.run(
                        source_name=source_name,
                        source_type=source_type,
                        table_name=table_name,
                        stage=stage,
                        file_path=connection_text,
                        pipeline_run_id=pipeline_run_id,
                        schedule_id=schedule_id,
                        batch_id=batch_id
                    )
                    completed[(batch_id, stage)] = "success"
                    logger.info(f"Stage completed | batch_id={batch_id} | stage={stage}")

                except Exception as e:
                    completed[(batch_id, stage)] = "failed"
                    logger.error(f"Pipeline stage failed | batch_id={batch_id} | stage={stage} | error={str(e)}")

        pm.complete_pipeline_run(pipeline_run_id)
        logger.info(f"Pipeline run completed | pipeline_run_id={pipeline_run_id}")

    except Exception as e:
        pm.fail_pipeline_run(pipeline_run_id)
        logger.error(f"Pipeline run failed | pipeline_run_id={pipeline_run_id} | error={str(e)}")


if __name__ == "__main__":
    run_pipeline_from_db()