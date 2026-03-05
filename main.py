from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger
import sqlite3
from collections import defaultdict

logger = get_logger()


def run_pipeline_from_db(rerun_failed=False):
    """
    Executes the medallion pipeline based on metadata stored in pipeline_manager.db
    No user input required.
    """

    pm = PipelineManager()
    pipeline = MedallionPipeline()

    # -------------------------------------------------------
    # Fetch active schedules + batches
    # -------------------------------------------------------
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                s.schedule_id,
                s.priority_nbr AS schedule_priority,
                s.frequency,
                s.run_ts,
                s.timezone,
                src.source_name,
                src.source_type,
                src.connection_text,
                b.batch_id,
                b.batch_name,
                b.priority_nbr AS batch_priority
            FROM schedule s
            JOIN source src
                ON s.source_id = src.source_id
            JOIN batch b
                ON s.schedule_id = b.schedule_id
            WHERE s.active_flag = 1
              AND b.active_flag = 1
        """)

        rows = cursor.fetchall()

    if not rows:
        logger.info("No active schedules found.")
        return

    # -------------------------------------------------------
    # Sort schedules and batches
    # -------------------------------------------------------
    rows_sorted = sorted(rows, key=lambda x: (x[1], x[10]))

    # -------------------------------------------------------
    # Define Medallion stages
    # -------------------------------------------------------
    stages = ["bronze", "silver", "gold"]

    stage_batches = defaultdict(list)

    for row in rows_sorted:
        (
            schedule_id,
            schedule_priority,
            frequency,
            run_ts,
            timezone,
            source_name,
            source_type,
            connection_text,
            batch_id,
            batch_name,
            batch_priority
        ) = row

        # Determine table name
        if source_type.lower() == "excel":
            table_name = batch_name
        else:
            table_name = source_name

        table_name = table_name.strip().replace(" ", "_")

        for stage in stages:
            stage_batches[stage].append(
                (
                    batch_id,
                    table_name,
                    source_name,
                    source_type,
                    connection_text,
                    schedule_id
                )
            )

    # -------------------------------------------------------
    # Create pipeline run
    # -------------------------------------------------------
    pipeline_run_id = pm.create_pipeline_run()

    completed = {}

    try:

        # -------------------------------------------------------
        # Run Medallion Stages
        # -------------------------------------------------------
        for stage in stages:

            logger.info(f"========== STARTING {stage.upper()} STAGE ==========")

            for batch in stage_batches[stage]:

                (
                    batch_id,
                    table_name,
                    source_name,
                    source_type,
                    connection_text,
                    schedule_id
                ) = batch

                # -------------------------------
                # DAG Dependency Check
                # -------------------------------
                if stage == "silver":
                    if completed.get((batch_id, "bronze")) != "success":
                        logger.warning(
                            f"Skipping Silver for batch {batch_id} (Bronze failed)"
                        )
                        continue

                if stage == "gold":
                    if completed.get((batch_id, "silver")) != "success":
                        logger.warning(
                            f"Skipping Gold for batch {batch_id} (Silver failed)"
                        )
                        continue

                # -------------------------------
                # Execute pipeline stage
                # -------------------------------
                try:

                    logger.info(
                        f"Running stage={stage} | batch_id={batch_id} | table={table_name}"
                    )

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

                    logger.info(
                        f"Stage success | batch_id={batch_id} | stage={stage}"
                    )

                except Exception as e:

                    completed[(batch_id, stage)] = "failed"

                    logger.error(
                        f"Stage failed | batch_id={batch_id} | stage={stage} | error={str(e)}"
                    )

                    # if rerun mode disabled stop pipeline
                    if not rerun_failed:
                        raise e

        # -------------------------------------------------------
        # Complete pipeline run
        # -------------------------------------------------------
        pm.complete_pipeline_run(pipeline_run_id)

        logger.info(
            f"Pipeline run completed successfully | pipeline_run_id={pipeline_run_id}"
        )

    except Exception as e:

        pm.fail_pipeline_run(pipeline_run_id)

        logger.error(
            f"Pipeline run failed | pipeline_run_id={pipeline_run_id} | error={str(e)}"
        )


if __name__ == "__main__":

    """
    Production Execution
    Used by scheduler / CI-CD
    """

    run_pipeline_from_db()