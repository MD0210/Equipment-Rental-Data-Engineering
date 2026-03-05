#artifact_entity.py
from dataclasses import dataclass

@dataclass
class BronzeArtifact:
    ingested_files: dict  # e.g., {"Rental_Transactions": "artifacts/bronze/Rental_Transactions.csv"}

@dataclass
class SilverArtifact:
    active_file: str
    completed_file: str
    cancelled_file: str
    all_file: str
    quarantine_file: str

@dataclass
class GoldArtifact:
    equipment_agg_file: str
    customer_agg_file: str
    monthly_agg_file: str