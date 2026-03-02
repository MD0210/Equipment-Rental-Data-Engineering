# equipment_rental/pipeline/pipeline_manager.py
import os
import sqlite3
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.constants.constants import PIPELINE_DIR  # import the pipeline directory

logger = get_logger()

# Ensure the pipeline directory exists
os.makedirs(PIPELINE_DIR, exist_ok=True)

# Define DB path inside PIPELINE_DIR
DB_PATH = os.path.join(PIPELINE_DIR, "pipeline_manager.db")


class PipelineManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # SOURCE table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS source (
                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT UNIQUE,
                description TEXT,
                active_flag INTEGER DEFAULT 1,
                priority_nbr INTEGER DEFAULT 1,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT
            )""")

            # SCHEDULE table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedule (
                schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                cron_expr TEXT,
                active_flag INTEGER DEFAULT 1,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(source_id) REFERENCES source(source_id)
            )""")

            # BATCH table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS batch (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                batch_type TEXT,
                run_date TEXT,
                active_flag INTEGER DEFAULT 1,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(source_id) REFERENCES source(source_id)
            )""")

            # TASK table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER,
                task_name TEXT,
                run_id TEXT,
                status TEXT,
                start_ts TEXT,
                end_ts TEXT,
                error_msg TEXT,
                active_flag INTEGER DEFAULT 1,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(batch_id) REFERENCES batch(batch_id)
            )""")
            conn.commit()
        logger.info(f"Pipeline Manager DB initialized at {self.db_path}")

    # -------------------------
    # Task/Run Management
    # -------------------------
    def start_task(self, source: str, batch_type: str, task_name: str, user: str = "system") -> str:
        run_id = f"{task_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Ensure source exists
            cursor.execute("SELECT source_id FROM source WHERE source_name=?", (source,))
            row = cursor.fetchone()
            if row:
                source_id = row[0]
            else:
                cursor.execute(
                    "INSERT INTO source (source_name, insert_ts, insert_user) VALUES (?, ?, ?)",
                    (source, datetime.now(), user)
                )
                source_id = cursor.lastrowid

            # Insert batch
            cursor.execute(
                """INSERT INTO batch (source_id, batch_type, run_date, insert_ts, insert_user)
                   VALUES (?, ?, ?, ?, ?)""",
                (source_id, batch_type, datetime.now().date(), datetime.now(), user)
            )
            batch_id = cursor.lastrowid

            # Insert task
            cursor.execute(
                """INSERT INTO task (batch_id, task_name, run_id, status, start_ts, insert_ts, insert_user)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (batch_id, task_name, run_id, "running", datetime.now(), datetime.now(), user)
            )
            conn.commit()
        logger.info(f"Task started | run_id: {run_id}")
        return run_id

    def complete_task(self, run_id: str, user: str = "system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE task SET status='success', end_ts=?, update_ts=?, update_user=?
                   WHERE run_id=?""",
                (datetime.now(), datetime.now(), user, run_id)
            )
            conn.commit()
        logger.info(f"Task completed | run_id: {run_id}")

    def fail_task(self, run_id: str, error_msg: str, user: str = "system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """UPDATE task SET status='failed', end_ts=?, error_msg=?, update_ts=?, update_user=?
                   WHERE run_id=?""",
                (datetime.now(), error_msg, datetime.now(), user, run_id)
            )
            conn.commit()
        logger.error(f"Task failed | run_id: {run_id} | error: {error_msg}")