#artifact_entity.py
from dataclasses import dataclass

@dataclass
class BronzeArtifact:
    ingested_files: dict  # e.g., {"Rental_Transactions": "artifacts/bronze/Rental_Transactions.csv"}

@dataclass
class SilverArtifact:
    rental_transactions_active_file: str
    rental_transactions_completed_file: str
    rental_transactions_cancelled_file: str
    rental_transactions_all_file: str
    quarantine_file: str

@dataclass
class GoldArtifact:
    equipment_agg_file: str
    customer_agg_file: str
    monthly_agg_file: str