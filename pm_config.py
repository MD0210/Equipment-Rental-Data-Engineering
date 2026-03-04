# pm_config.py
import sqlite3
from equipment_rental.pipeline.pipeline_manager import PipelineManager
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def list_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print("Tables in pipeline_manager.db:")
    for t in tables:
        print(f" - {t}")
    return tables


def show_table_data(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
    rows = cursor.fetchall()
    print(f"\nSample data from {table_name} (first 10 rows):")
    for row in rows:
        print(row)


def main():
    pm = PipelineManager()
    pm.init_db()  # ensure DB exists

    conn = pm.conn
    cursor = conn.cursor()

    while True:
        action = input("\nSelect action (add/update/delete/view/exit): ").strip().lower()

        if action == "exit":
            print("Exiting PM config.")
            break

        tables = list_tables(cursor)
        table_name = input("Select table to operate on: ").strip()
        if table_name not in tables:
            print("Invalid table. Try again.")
            continue

        # ---------------- ADD ----------------
        if action == "add":
            cursor.execute(f"PRAGMA table_info({table_name})")
            cols = [col[1] for col in cursor.fetchall() if col[1] != "id"]
            values = {}
            print(f"Enter values for columns: {cols}")
            for col in cols:
                values[col] = input(f"{col}: ").strip()
            placeholders = ", ".join(["?"] * len(values))
            cols_str = ", ".join(values.keys())
            cursor.execute(
                f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})",
                tuple(values.values())
            )
            conn.commit()
            print("Row added successfully.")
            show_table_data(cursor, table_name)

        # ---------------- UPDATE ----------------
        elif action == "update":
            row_id = input("Enter the id of the row to update: ").strip()
            cursor.execute(f"PRAGMA table_info({table_name})")
            cols = [col[1] for col in cursor.fetchall() if col[1] != "id"]
            updates = {}
            for col in cols:
                val = input(f"New value for {col} (leave blank to skip): ").strip()
                if val != "":
                    updates[col] = val
            if updates:
                set_clause = ", ".join([f"{c}=?" for c in updates])
                cursor.execute(
                    f"UPDATE {table_name} SET {set_clause} WHERE id=?",
                    tuple(list(updates.values()) + [row_id])
                )
                conn.commit()
                print("Row updated successfully.")
            show_table_data(cursor, table_name)

        # ---------------- DELETE ----------------
        elif action == "delete":
            row_id = input("Enter the id of the row to delete (or multiple comma-separated ids): ").strip()
            ids = [i.strip() for i in row_id.split(",")]
            cursor.execute(
                f"DELETE FROM {table_name} WHERE id IN ({','.join(['?']*len(ids))})",
                tuple(ids)
            )
            conn.commit()
            print("Row(s) deleted successfully.")
            show_table_data(cursor, table_name)

        # ---------------- VIEW ----------------
        elif action == "view":
            show_table_data(cursor, table_name)

        else:
            print("Invalid action. Choose add/update/delete/view/exit.")


if __name__ == "__main__":
    main()