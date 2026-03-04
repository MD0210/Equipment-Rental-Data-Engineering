import os
import sqlite3
import uuid
from datetime import datetime, timedelta
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

            # TASK (ADDED pipeline_run_id)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task (
                task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_run_id TEXT,
                source_id INTEGER,
                target_id INTEGER,
                schedule_id INTEGER,
                batch_id INTEGER,
                task_name TEXT,
                run_id TEXT,
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

            conn.commit()

        logger.info(f"Pipeline Manager DB initialized at {self.db_path}")

    # ==========================================================
    # PIPELINE RUN
    # ==========================================================

    def create_pipeline_run(self):
        """
        Creates a unique pipeline execution ID.
        Used to group all tasks under one medallion execution.
        """
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
    # SCHEDULE
    # ==========================================================

    def _calculate_next_run(self, run_ts, frequency):
        run_dt = datetime.strptime(run_ts, "%Y-%m-%d %H:%M:%S")

        if frequency == "daily":
            return run_dt + timedelta(days=1)
        elif frequency == "weekly":
            return run_dt + timedelta(days=7)
        elif frequency == "hourly":
            return run_dt + timedelta(hours=1)
        elif frequency == "monthly":
            return run_dt + timedelta(days=30)
        else:
            return None

    def add_or_get_schedule(self, source_id, schedule_name,
                            frequency, run_ts, timezone,
                            priority_nbr, active_flag):

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT schedule_id FROM schedule WHERE schedule_name=?",
                           (schedule_name,))
            row = cursor.fetchone()

            if row:
                return row[0]

            next_run = None
            if run_ts and frequency:
                next_run_dt = self._calculate_next_run(run_ts, frequency)
                next_run = next_run_dt.strftime("%Y-%m-%d %H:%M:%S") if next_run_dt else None

            cursor.execute("""
                INSERT INTO schedule (
                    source_id, schedule_name, frequency,
                    run_ts, next_run_ts, timezone,
                    priority_nbr, active_flag,
                    insert_ts, insert_user
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                source_id, schedule_name, frequency,
                run_ts, next_run, timezone,
                priority_nbr, active_flag,
                datetime.now(), "system"
            ))

            conn.commit()
            return cursor.lastrowid

    # ==========================================================
    # BATCH
    # ==========================================================

    def add_batch(self, schedule_id, batch_name, batch_type,
                  priority_nbr, active_flag):

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO batch (
                    schedule_id, batch_name, batch_type,
                    priority_nbr, active_flag,
                    run_date, insert_ts, insert_user
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                schedule_id, batch_name, batch_type,
                priority_nbr, active_flag,
                datetime.now().date(),
                datetime.now(), "system"
            ))

            conn.commit()
            return cursor.lastrowid

    # ==========================================================
    # TASK
    # ==========================================================

    def start_task(self, pipeline_run_id,
                   source_id, target_id,
                   schedule_id, batch_id, task_name):

        run_id = f"{task_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO task (
                    pipeline_run_id,
                    source_id, target_id, schedule_id, batch_id,
                    task_name, run_id, status,
                    start_ts, insert_ts, insert_user
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pipeline_run_id,
                source_id, target_id, schedule_id, batch_id,
                task_name, run_id, "running",
                datetime.now(), datetime.now(), "system"
            ))

            conn.commit()

        logger.info(f"Task started | pipeline_run_id={pipeline_run_id} | run_id={run_id}")
        return run_id

    def complete_task(self, run_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT start_ts FROM task WHERE run_id=?", (run_id,))
            row = cursor.fetchone()

            if not row:
                logger.error(f"No task found for run_id={run_id}")
                return

            start_dt = datetime.fromisoformat(row[0])
            end_dt = datetime.now()
            duration = (end_dt - start_dt).total_seconds()

            cursor.execute("""
                UPDATE task
                SET status='success',
                    end_ts=?,
                    duration_sec=?,
                    update_ts=?,
                    update_user=?
                WHERE run_id=?
            """, (
                end_dt, duration,
                datetime.now(), "system",
                run_id
            ))

            conn.commit()

        logger.info(f"Task completed | run_id={run_id} | duration={duration} sec")

    def fail_task(self, run_id, error_msg):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            end_dt = datetime.now()

            cursor.execute("""
                UPDATE task
                SET status='failed',
                    end_ts=?,
                    error_msg=?,
                    update_ts=?,
                    update_user=?
                WHERE run_id=?
            """, (
                end_dt, error_msg,
                datetime.now(), "system",
                run_id
            ))

            conn.commit()

        logger.error(f"Task failed | run_id={run_id} | error={error_msg}")