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
        logger.info("No active schedules found. Exiting.")
        return

    # Organize rows into a dict: table_name -> row
    schedule_map = {row[3]: row for row in rows}  # key = source_name

    pipeline_run_id = pm.create_pipeline_run()
    completed = {}

    try:
        # ---- Step 1: Process master tables first ----
        for master_table in ["Customer_Master", "Equipment_Master"]:
            if master_table not in schedule_map:
                logger.warning(f"No schedule found for master table: {master_table}")
                continue

            row = schedule_map[master_table]
            frequency, run_ts, timezone, source_name, source_type, connection_text, batch_name = row

            # Use batch_name only for Excel
            table_name = batch_name if source_type.lower() == "excel" else source_name

            for stage in ["bronze", "silver"]:
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
                    break  # skip remaining stages for this table if failed

        # ---- Step 2: Process Rental_Transactions ----
        rental_table = "Rental_Transactions"
        if rental_table in schedule_map:
            row = schedule_map[rental_table]
            frequency, run_ts, timezone, source_name, source_type, connection_text, batch_name = row
            table_name = batch_name if source_type.lower() == "excel" else source_name

            for stage in ["bronze", "silver", "gold"]:
                # Skip Silver if Bronze failed
                if stage == "silver" and completed.get((table_name, "bronze")) != "success":
                    logger.warning(f"Skipping Silver stage for {table_name} because Bronze failed")
                    continue
                # Skip Gold if Silver failed
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