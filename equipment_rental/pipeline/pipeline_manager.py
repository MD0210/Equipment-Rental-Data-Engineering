# equipment_rental/pipeline/pipeline_manager.py
import os
import sqlite3
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.constants.constants import PIPELINE_DIR
from equipment_rental.exception.exception import PipelineManagerException  # <- import custom exception

logger = get_logger()
os.makedirs(PIPELINE_DIR, exist_ok=True)
DB_PATH = os.path.join(PIPELINE_DIR, "pipeline_manager.db")


class PipelineManager:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        try:
            self._init_db()
        except Exception as e:
            logger.error(f"Failed to initialize PipelineManager DB: {str(e)}")
            raise PipelineManagerException("DB initialization failed", errors=e)

    # ==========================================================
    # INIT DATABASE
    # ==========================================================
    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Source table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS source (
                    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT UNIQUE,
                    source_type TEXT,
                    connection_text TEXT,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT
                )
                """)

                # Schedule table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    schedule_name TEXT UNIQUE,
                    frequency TEXT,
                    run_ts TEXT,
                    next_run_ts TEXT,
                    timezone TEXT,
                    priority_nbr INTEGER,
                    active_flag INTEGER,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT
                )
                """)

                # Batch table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS batch (
                    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    schedule_id INTEGER,
                    batch_name TEXT,
                    batch_type TEXT,
                    priority_nbr INTEGER,
                    active_flag INTEGER,
                    run_date TEXT,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT
                )
                """)

                # Pipeline run table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_run (
                    pipeline_run_id TEXT PRIMARY KEY,
                    start_ts TEXT,
                    end_ts TEXT,
                    status TEXT,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT
                )
                """)

                # Task table
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS task (
                    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline_run_id TEXT,
                    schedule_id INTEGER,
                    batch_id INTEGER,
                    source_id INTEGER,
                    target_id INTEGER,
                    stage TEXT,
                    status TEXT,
                    start_ts TEXT,
                    end_ts TEXT,
                    duration_sec REAL,
                    error_msg TEXT,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT
                )
                """)

                # Watermark table for incremental loads
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_watermark (
                    watermark_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    stage TEXT,
                    last_watermark TEXT,
                    insert_ts TEXT,
                    insert_user TEXT,
                    update_ts TEXT,
                    update_user TEXT,
                    UNIQUE(source_id, stage)
                )
                """)

                conn.commit()
            logger.info(f"Pipeline Manager DB initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Error creating DB tables: {str(e)}")
            raise PipelineManagerException("Failed to initialize DB tables", errors=e)

    # ==========================================================
    # PIPELINE RUN
    # ==========================================================
    def create_pipeline_run(self):
        import uuid
        pipeline_run_id = str(uuid.uuid4())
        start_ts = datetime.now()
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO pipeline_run (pipeline_run_id, start_ts, status, insert_ts, insert_user)
                    VALUES (?, ?, ?, ?, ?)
                """, (pipeline_run_id, start_ts, "running", start_ts, "system"))
                conn.commit()
            logger.info(f"Pipeline run started: {pipeline_run_id}")
            return pipeline_run_id
        except Exception as e:
            logger.error(f"Failed to create pipeline run: {str(e)}")
            raise PipelineManagerException("Failed to create pipeline run", errors=e)

    def complete_pipeline_run(self, pipeline_run_id):
        try:
            end_ts = datetime.now()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pipeline_run
                    SET status='success', end_ts=?, update_ts=?, update_user=?
                    WHERE pipeline_run_id=?
                """, (end_ts, datetime.now(), "system", pipeline_run_id))
                conn.commit()
            logger.info(f"Pipeline run completed: {pipeline_run_id}")
        except Exception as e:
            logger.error(f"Failed to complete pipeline run: {str(e)}")
            raise PipelineManagerException(f"Failed to complete pipeline run {pipeline_run_id}", errors=e)

    def fail_pipeline_run(self, pipeline_run_id):
        try:
            end_ts = datetime.now()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pipeline_run
                    SET status='failed', end_ts=?, update_ts=?, update_user=?
                    WHERE pipeline_run_id=?
                """, (end_ts, datetime.now(), "system", pipeline_run_id))
                conn.commit()
            logger.error(f"Pipeline run failed: {pipeline_run_id}")
        except Exception as e:
            logger.error(f"Failed to mark pipeline run as failed: {str(e)}")
            raise PipelineManagerException(f"Failed to fail pipeline run {pipeline_run_id}", errors=e)