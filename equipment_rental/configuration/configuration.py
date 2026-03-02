import os
from equipment_rental.constants.constants import ARTIFACT_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR

class Config:
    """
    Centralized configuration for the Equipment Rental Medallion Pipeline
    """

    # ===== ARTIFACT DIRECTORIES =====
    BASE_ARTIFACT_DIR = ARTIFACT_DIR
    BRONZE_DIR = BRONZE_DIR
    SILVER_DIR = SILVER_DIR
    GOLD_DIR = GOLD_DIR
    QUARANTINE_DIR = QUARANTINE_DIR

    # ===== BRONZE SETTINGS =====
    EXCEL_SUPPORTED_SHEETS = ["Equipment_master", "Customer_Master", "Rental_Transactions", "Date_Dimension"]
    DB_SUPPORTED_TABLES = ["Equipment_master", "Customer_Master", "Rental_Transactions", "Date_Dimension"]

    # ===== SILVER SETTINGS =====
    SILVER_ACTIVE_STATUS = "active"
    SILVER_COMPLETED_STATUS = "completed"
    SILVER_CANCELLED_STATUS = "cancelled"

    # ===== GOLD SETTINGS =====
    GOLD_AGGREGATION_FILENAME = "equipment_aggregation.csv"
    GOLD_CUSTOMER_FILENAME = "customer_aggregation.csv"
    GOLD_MONTHLY_FILENAME = "monthly_aggregation.csv"

    # ===== METADATA =====
    DEFAULT_LOAD_USER = "system"
    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

    # ===== DATABASE CONFIG =====
    # Example SQLite, can be replaced by any connection string
    DB_CONNECTION_STRING = os.getenv("EQUIPMENT_DB_CONN", "sqlite:///artifacts/equipment_rental.db")

    # ===== PIPELINE MANAGER SETTINGS =====
    PIPELINE_MANAGER_TABLES = {
        "source": "pipeline_source",
        "schedule": "pipeline_schedule",
        "batch": "pipeline_batch",
        "task": "pipeline_task"
    }