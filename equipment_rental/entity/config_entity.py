# config_entity.py
from dataclasses import dataclass

@dataclass
class BronzeConfig:
    excel_sheets: list
    db_tables: list
    bronze_dir: str

@dataclass
class SilverConfig:
    active_status: str
    completed_status: str
    cancelled_status: str
    silver_dir: str
    quarantine_dir: str

@dataclass
class GoldConfig:
    gold_dir: str
    equipment_filename: str
    customer_filename: str
    monthly_filename: str

@dataclass
class PipelineManagerConfig:
    source_table: str
    schedule_table: str
    batch_table: str
    task_table: str

@dataclass
class DBConfig:
    connection_string: str