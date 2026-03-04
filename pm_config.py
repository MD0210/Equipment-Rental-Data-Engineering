# pm_config.py
import sqlite3
from datetime import datetime
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def print_table(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    if not rows:
        print(f"No records found in {table_name}.")
        return
    # Print headers
    col_names = [desc[0] for desc in cursor.description]
    print(f"\n{table_name.upper()} TABLE")
    print("-" * 50)
    print("\t".join(col_names))
    print("-" * 50)
    for r in rows:
        print("\t".join([str(x) for x in r]))
    print("-" * 50)


def add_record(pm: PipelineManager, table: str):
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()

        if table == "source":
            name = input("Source Name: ").strip()
            type_ = input("Source Type: ").strip()
            conn_text = input("Connection Text: ").strip()
            cursor.execute("""
                INSERT INTO source (source_name, source_type, connection_text, insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?)
            """, (name, type_, conn_text, datetime.now(), "user"))
            conn.commit()
            print(f"Source added with ID {cursor.lastrowid}")

        elif table == "schedule":
            print_table(cursor, "source")
            source_id = int(input("Source ID: ").strip())
            sched_name = input("Schedule Name: ").strip()
            freq = input("Frequency (daily/weekly/monthly/manual): ").strip()
            run_ts = input("Run timestamp (YYYY-MM-DD HH:MM:SS): ").strip()
            tz = input("Timezone (UTC): ").strip() or "UTC"
            priority = int(input("Priority number: ").strip() or 1)
            active = int(input("Active flag (1=active,0=inactive): ").strip() or 1)
            next_run = None
            cursor.execute("""
                INSERT INTO schedule
                (source_id, schedule_name, frequency, run_ts, next_run_ts, timezone, priority_nbr, active_flag,
                 insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (source_id, sched_name, freq, run_ts, next_run, tz, priority, active, datetime.now(), "user"))
            conn.commit()
            print(f"Schedule added with ID {cursor.lastrowid}")

        elif table == "batch":
            print_table(cursor, "schedule")
            sched_id = int(input("Schedule ID: ").strip())
            batch_name = input("Batch Name: ").strip()
            batch_type = input("Batch Type (full/incremental): ").strip()
            priority = int(input("Priority number: ").strip() or 1)
            active = int(input("Active flag (1=active,0=inactive): ").strip() or 1)
            cursor.execute("""
                INSERT INTO batch (schedule_id, batch_name, batch_type, priority_nbr, active_flag, run_date,
                                   insert_ts, insert_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (sched_id, batch_name, batch_type, priority, active, datetime.now().date(),
                  datetime.now(), "user"))
            conn.commit()
            print(f"Batch added with ID {cursor.lastrowid}")

        else:
            print(f"Add not supported for table {table}")


def update_record(pm: PipelineManager, table: str):
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        print_table(cursor, table)
        record_id = input(f"Enter the ID of the record to update in {table}: ").strip()

        cursor.execute(f"PRAGMA table_info({table})")
        columns = [col[1] for col in cursor.fetchall() if col[1] not in ("insert_ts", "insert_user")]
        print(f"Columns available to update: {columns}")
        col = input("Column to update: ").strip()
        if col not in columns:
            print("Invalid column")
            return
        val = input("New value: ").strip()

        # Determine ID column
        id_col = [c for c in columns if c.endswith("_id")][0]

        cursor.execute(f"""
            UPDATE {table}
            SET {col} = ?, update_ts = ?, update_user = ?
            WHERE {id_col} = ?
        """, (val, datetime.now(), "user", record_id))
        conn.commit()
        print(f"{table} ID {record_id} updated successfully.")


def delete_record(pm: PipelineManager, table: str):
    with sqlite3.connect(pm.db_path) as conn:
        cursor = conn.cursor()
        print_table(cursor, table)
        record_id = input(f"Enter the ID of the record to delete in {table}: ").strip()
        # Determine ID column
        cursor.execute(f"PRAGMA table_info({table})")
        id_col = [col[1] for col in cursor.fetchall() if col[1].endswith("_id")][0]
        cursor.execute(f"DELETE FROM {table} WHERE {id_col} = ?", (record_id,))
        conn.commit()
        print(f"{table} ID {record_id} deleted successfully.")


def main():
    pm = PipelineManager()  # DB initialized here

    actions = {
        "1": "Add",
        "2": "Update",
        "3": "Delete",
        "4": "View",
        "0": "Exit"
    }

    tables = ["source", "schedule", "batch"]

    while True:
        print("\nSelect action:")
        for k, v in actions.items():
            print(f"{k}: {v}")
        act = input("Choice: ").strip()
        if act == "0":
            break

        print(f"Available tables: {tables}")
        tbl = input("Select table: ").strip()
        if tbl not in tables:
            print("Invalid table")
            continue

        if act == "1":
            add_record(pm, tbl)
        elif act == "2":
            update_record(pm, tbl)
        elif act == "3":
            delete_record(pm, tbl)
        elif act == "4":
            with sqlite3.connect(pm.db_path) as conn:
                print_table(conn.cursor(), tbl)
        else:
            print("Invalid action")


if __name__ == "__main__":
    main()