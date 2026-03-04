import os
import sqlite3
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.constants.constants import PIPELINE_DIR

logger = get_logger()

os.makedirs(PIPELINE_DIR, exist_ok=True)
DB_PATH = os.path.join(PIPELINE_DIR, "pipeline_manager.db")


class PipelineManager:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    # ==========================================================
    # INIT DATABASE
    # ==========================================================
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # SOURCE
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

            # SCHEDULE
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

            # BATCH
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

            # TASK
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT,
                source_id INTEGER,
                target_id INTEGER,
                stage TEXT,
                table_name TEXT,
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

    # ==========================================================
    # PIPELINE RUN
    # ==========================================================
    def create_pipeline_run(self):
        import uuid
        pipeline_run_id = str(uuid.uuid4())
        logger.info(f"Pipeline run created: {pipeline_run_id}")
        return pipeline_run_id

    # ==========================================================
    # SOURCE
    # ==========================================================
    def add_or_get_source(self, source_name, source_type, connection_text):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_id FROM source WHERE source_name=?", (source_name,))
            row = cursor.fetchone()
            if row:
                return row[0]
            cursor.execute("""
                INSERT INTO source (source_name, source_type, connection_text, insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?)
            """, (source_name, source_type, connection_text, datetime.now(), "system"))
            conn.commit()
            return cursor.lastrowid

    # ==========================================================
    # TASK 
    # ==========================================================
    def start_task(self, source_id, target_id,
                stage, table_name, pipeline_run_id):
        """Start a task for a given stage (bronze/silver/gold)."""
        start_ts = datetime.now()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO task (
                    pipeline_run_id, source_id, target_id,
                    stage, table_name, status,
                    start_ts, insert_ts, insert_user
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pipeline_run_id, source_id, target_id,
                stage, table_name, "running",
                start_ts, start_ts, "system"
            ))
            conn.commit()
            task_id = cursor.lastrowid

        logger.info(f"Task started | pipeline_run_id={pipeline_run_id} | stage={stage}")
        return task_id
    
    def complete_task(self, task_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT start_ts FROM task WHERE task_id=?", (task_id,))
            row = cursor.fetchone()
            if not row:
                logger.error(f"No task found for task_id={task_id}")
                return
            start_dt = datetime.fromisoformat(row[0])
            end_dt = datetime.now()
            duration = (end_dt - start_dt).total_seconds()
            cursor.execute("""
                UPDATE task
                SET status='success', end_ts=?, duration_sec=?,
                    update_ts=?, update_user=?
                WHERE task_id=?
            """, (end_dt, duration, datetime.now(), "system", task_id))
            conn.commit()
        logger.info(f"Task completed | task_id={task_id} | duration={duration} sec")

    def fail_task(self, task_id, error_msg):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            end_dt = datetime.now()
            cursor.execute("""
                UPDATE task
                SET status='failed', end_ts=?, error_msg=?,
                    update_ts=?, update_user=?
                WHERE task_id=?
            """, (end_dt, error_msg, datetime.now(), "system", task_id))
            conn.commit()
        logger.error(f"Task failed | task_id={task_id} | error={error_msg}")