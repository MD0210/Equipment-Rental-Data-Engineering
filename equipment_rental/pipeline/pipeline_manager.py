# pipeline_manager.py (updated)
import os
import sqlite3
from datetime import datetime
from equipment_rental.constants.constants import PIPELINE_DIR
from equipment_rental.logger.logger import get_logger

logger = get_logger()
os.makedirs(PIPELINE_DIR, exist_ok=True)
DB_PATH = os.path.join(PIPELINE_DIR, "pipeline_manager.db")


class PipelineManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    # -------------------------------
    # INIT DATABASE
    # -------------------------------
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
            )""")
            
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
            )""")
            
            # BATCH
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS batch (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                batch_name TEXT,  -- the table to run
                batch_type TEXT,
                priority_nbr INTEGER,
                active_flag INTEGER,
                run_date TEXT,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT
            )""")
            
            # PIPELINE RUN
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
            )""")
            
            # TASK (stage only)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT,
                source_id INTEGER,
                target_id INTEGER,
                stage TEXT,           -- bronze / silver / gold
                table_name TEXT,      -- batch_name
                status TEXT,
                start_ts TEXT,
                end_ts TEXT,
                duration_sec REAL,
                error_msg TEXT,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT
            )""")
            
            conn.commit()
        logger.info(f"Pipeline Manager DB initialized at {self.db_path}")

    # -------------------------------
    # PIPELINE RUN
    # -------------------------------
    def create_pipeline_run(self):
        import uuid
        pipeline_run_id = str(uuid.uuid4())
        start_ts = datetime.now()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pipeline_run (pipeline_run_id, start_ts, status, insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?)
            """, (pipeline_run_id, start_ts, "running", start_ts, "system"))
            conn.commit()
        logger.info(f"Pipeline run created: {pipeline_run_id}")
        return pipeline_run_id

    def complete_pipeline_run(self, pipeline_run_id, status="success"):
        end_ts = datetime.now()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_run
                SET end_ts=?, status=?, update_ts=?, update_user=?
                WHERE pipeline_run_id=?
            """, (end_ts, status, datetime.now(), "system", pipeline_run_id))
            conn.commit()

    # -------------------------------
    # TASK
    # -------------------------------
    def start_task(self, source_id, target_id, stage, table_name, pipeline_run_id):
        start_ts = datetime.now()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO task (
                    pipeline_run_id, source_id, target_id, stage, table_name, status,
                    start_ts, insert_ts, insert_user
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (pipeline_run_id, source_id, target_id, stage, table_name, "running",
                  start_ts, start_ts, "system"))
            conn.commit()
            task_id = cursor.lastrowid
        logger.info(f"Task started | pipeline_run_id={pipeline_run_id} | stage={stage} | table={table_name}")
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