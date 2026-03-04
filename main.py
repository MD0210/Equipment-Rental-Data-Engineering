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

    # Sort by schedule priority, then batch priority
    rows_sorted = sorted(rows, key=lambda x: (x[1], x[10]))  # x[1]=schedule_priority, x[10]=batch_priority

    pipeline_run_id = pm.create_pipeline_run()
    completed = {}

    try:
        # ----------------------
        # Phase 1: Bronze
        # ----------------------
        for row in rows_sorted:
            schedule_id, schedule_prio, frequency, run_ts, timezone, source_name, source_type, connection_text, batch_id, batch_name, batch_prio = row

            table_name = batch_name if source_type.lower() == "excel" else source_name
            table_name = table_name.strip().replace(" ", "_")

            logger.info(f"[Bronze] Starting pipeline | table: {table_name} | batch_id: {batch_id}")

            try:
                pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage="bronze",
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )
                completed[(batch_id, "bronze")] = "success"
                logger.info(f"[Bronze] Stage completed | batch_id={batch_id}")
            except Exception as e:
                completed[(batch_id, "bronze")] = "failed"
                logger.error(f"[Bronze] Stage failed | batch_id={batch_id} | error={str(e)}")

        # ----------------------
        # Phase 2: Silver
        # ----------------------
        for row in rows_sorted:
            schedule_id, schedule_prio, frequency, run_ts, timezone, source_name, source_type, connection_text, batch_id, batch_name, batch_prio = row

            # Skip Silver if Bronze failed
            if completed.get((batch_id, "bronze")) != "success":
                logger.warning(f"[Silver] Skipping batch_id={batch_id} because Bronze failed")
                continue

            table_name = batch_name if source_type.lower() == "excel" else source_name
            table_name = table_name.strip().replace(" ", "_")

            logger.info(f"[Silver] Starting pipeline | table: {table_name} | batch_id: {batch_id}")

            try:
                pipeline.run(
                    source_name=source_name,
                    source_type=source_type,
                    table_name=table_name,
                    stage="silver",
                    file_path=connection_text,
                    pipeline_run_id=pipeline_run_id,
                    schedule_id=schedule_id,
                    batch_id=batch_id
                )
                completed[(batch_id, "silver")] = "success"
                logger.info(f"[Silver] Stage completed | batch_id={batch_id}")
            except Exception as e:
                completed[(batch_id, "silver")] = "failed"
                logger.error(f"[Silver] Stage failed | batch_id={batch_id} | error={str(e)}")

        # ----------------------
        # Phase 3: Gold
        # ----------------------
        logger.info("[Gold] Starting pipeline for all batches")
        try:
            pipeline.run(
                source_name="Gold",
                source_type="folder",
                table_name="rental_transactions",
                stage="gold",
                pipeline_run_id=pipeline_run_id
            )
            logger.info("[Gold] Stage completed successfully")
        except Exception as e:
            logger.error(f"[Gold] Stage failed | error={str(e)}")

        pm.complete_pipeline_run(pipeline_run_id)
        logger.info(f"Pipeline run completed | pipeline_run_id={pipeline_run_id}")

    except Exception as e:
        pm.fail_pipeline_run(pipeline_run_id)
        logger.error(f"Pipeline run failed | pipeline_run_id={pipeline_run_id} | error={str(e)}")


if __name__ == "__main__":
    run_pipeline_from_db()