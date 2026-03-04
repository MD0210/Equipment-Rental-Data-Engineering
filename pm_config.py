# pm_config.py
import sqlite3
from equipment_rental.pipeline.pipeline_manager import PipelineManager

def select_operation():
    print("Pipeline Manager Config")
    print("=======================")
    print("1. Add record")
    print("2. Update record")
    print("3. Delete record")
    print("0. Exit")
    return input("Select operation: ").strip()

def select_table(pm: PipelineManager):
    print("\nAvailable tables in pipeline_manager DB:")
    # List table names dynamically
    cursor = pm.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    for i, t in enumerate(tables, 1):
        print(f"{i}. {t}")
    choice = int(input("Select table: "))
    return tables[choice-1]

def add_record(pm: PipelineManager, table: str):
    cursor = pm.conn.cursor()
    cursor.execute(f"PRAGMA table_info({table});")
    columns = [col[1] for col in cursor.fetchall()]
    values = []
    for col in columns:
        val = input(f"Enter value for {col}: ")
        values.append(f"'{val}'")
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)});"
    cursor.execute(sql)
    pm.conn.commit()
    print(f"Record added to {table}.")

def update_record(pm: PipelineManager, table: str):
    record_id = input("Enter ID (primary key) of the record to update: ").strip()
    column = input("Enter column name to update: ").strip()
    new_val = input(f"Enter new value for {column}: ").strip()
    sql = f"UPDATE {table} SET {column}='{new_val}' WHERE id={record_id};"
    pm.conn.execute(sql)
    pm.conn.commit()
    print(f"Record {record_id} updated in {table}.")

def delete_record(pm: PipelineManager, table: str):
    record_id = input("Enter ID of record to delete: ").strip()
    sql = f"DELETE FROM {table} WHERE id={record_id};"
    pm.conn.execute(sql)
    pm.conn.commit()
    print(f"Record {record_id} deleted from {table}.")

def main():
    pm = PipelineManager()  # your PipelineManager class handles the connection
    pm.init_db()            # ensure the DB exists

    while True:
        op = select_operation()
        if op == "0":
            break

        table = select_table(pm)

        if op == "1":
            add_record(pm, table)
        elif op == "2":
            update_record(pm, table)
        elif op == "3":
            delete_record(pm, table)
        else:
            print("Invalid option.")

if __name__ == "__main__":
    main()