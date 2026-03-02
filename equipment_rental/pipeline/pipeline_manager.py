# equipment_rental/pipeline/pipeline_manager.py
import os
import sqlite3
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.constants.constants import PIPELINE_DIR

logger = get_logger()

# Ensure pipeline directory exists
os.makedirs(PIPELINE_DIR, exist_ok=True)
DB_PATH = os.path.join(PIPELINE_DIR, "pipeline_manager.db")


class PipelineManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # ---------------- Source ----------------
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
            # ---------------- Schedule ----------------
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedule (
                schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                schedule_name TEXT,
                frequency TEXT,
                run_ts TEXT,
                next_run_ts TEXT,
                timezone TEXT,
                priority_nbr INTEGER DEFAULT 1,
                active_flag INTEGER DEFAULT 1,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(source_id) REFERENCES source(source_id)
            )
            """)
            # ---------------- Batch ----------------
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS batch (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                batch_name TEXT,
                batch_type TEXT,
                priority_nbr INTEGER DEFAULT 1,
                active_flag INTEGER DEFAULT 1,
                run_date TEXT,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(schedule_id) REFERENCES schedule(schedule_id)
            )
            """)
            # ---------------- Task ----------------
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                target_id INTEGER,
                schedule_id INTEGER,
                batch_id INTEGER,
                task_name TEXT,
                run_id TEXT,
                status TEXT,
                start_ts TEXT,
                end_ts TEXT,
                error_msg TEXT,
                insert_ts TEXT,
                insert_user TEXT,
                update_ts TEXT,
                update_user TEXT,
                FOREIGN KEY(source_id) REFERENCES source(source_id),
                FOREIGN KEY(target_id) REFERENCES source(source_id),
                FOREIGN KEY(schedule_id) REFERENCES schedule(schedule_id),
                FOREIGN KEY(batch_id) REFERENCES batch(batch_id)
            )
            """)
            conn.commit()
        logger.info(f"Pipeline Manager DB initialized at {self.db_path}")

    # ---------------- Source ----------------
    def add_or_get_source(self, source_name, source_type, connection_text, user="system"):
        """Register or return source_id"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_id FROM source WHERE source_name=?", (source_name,))
            row = cursor.fetchone()
            if row:
                return row[0]
            cursor.execute("""
                INSERT INTO source (source_name, source_type, connection_text, insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?)
            """, (source_name, source_type, connection_text, datetime.now(), user))
            conn.commit()
            return cursor.lastrowid

    # ---------------- Schedule ----------------
    def add_schedule(self, source_id, schedule_name, frequency, run_ts=None, next_run_ts=None,
                     timezone=None, priority_nbr=1, active_flag=1, user="system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO schedule (source_id, schedule_name, frequency, run_ts, next_run_ts, timezone,
                                      priority_nbr, active_flag, insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, schedule_name, frequency, run_ts, next_run_ts, timezone,
                  priority_nbr, active_flag, datetime.now(), user))
            conn.commit()
            return cursor.lastrowid

    # ---------------- Batch ----------------
    def add_batch(self, schedule_id, batch_name, batch_type, priority_nbr=1, active_flag=1, run_date=None, user="system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO batch (schedule_id, batch_name, batch_type, priority_nbr, active_flag, run_date,
                                   insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (schedule_id, batch_name, batch_type, priority_nbr, active_flag, run_date or datetime.now().date(),
                  datetime.now(), user))
            conn.commit()
            return cursor.lastrowid

    # ---------------- Task / Run ----------------
    def start_task(self, source_id, target_id, schedule_id, batch_id, task_name, user="system"):
        """Start a new task/run"""
        run_id = f"{task_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO task (source_id, target_id, schedule_id, batch_id, task_name, run_id, status, start_ts,
                                  insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, target_id, schedule_id, batch_id, task_name, run_id, "running",
                  datetime.now(), datetime.now(), user))
            conn.commit()
        logger.info(f"Task started | run_id: {run_id}")
        return run_id

    def complete_task(self, run_id, user="system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE task SET status='success', end_ts=?, update_ts=?, update_user=?
                WHERE run_id=?
            """, (datetime.now(), datetime.now(), user, run_id))
            conn.commit()
        logger.info(f"Task completed | run_id: {run_id}")

    def fail_task(self, run_id, error_msg, user="system"):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE task SET status='failed', end_ts=?, error_msg=?, update_ts=?, update_user=?
                WHERE run_id=?
            """, (datetime.now(), error_msg, datetime.now(), user, run_id))
            conn.commit()
        logger.error(f"Task failed | run_id: {run_id} | error: {error_msg}")