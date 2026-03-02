import pandas as pd
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.constants.constants import SILVER_DIR
from equipment_rental.components.quarantine_handler import QuarantineHandler

logger = get_logger()


class SilverTransformation:
    def __init__(self):
        self.quarantine_handler = QuarantineHandler()

    def transform(self, validated_tables: dict, table_name: str, pipeline_run_id: str = None):
        """
        Transform validated tables:
        - Compute RentalDays, TotalRevenue
        - Save CSVs for active/completed/cancelled/all
        """
        transformed_tables = {}

        # -------- Rental Transactions --------
        if table_name.lower() == "rental_transactions":
            for status in ["active", "completed", "cancelled", "all"]:
                df = validated_tables.get(status)
                if df is None or df.empty:
                    continue

                df = df.copy()
                # Compute RentalDays
                df["RentalDays"] = (df["EndDate"].fillna(pd.Timestamp.today()) - df["StartDate"]).dt.days + 1
                df["RentalDays"] = df["RentalDays"].clip(lower=1)

                # Compute TotalRevenue
                if "ActualRevenue" in df.columns:
                    df["TotalRevenue"] = df["ActualRevenue"]
                elif "DailyRate" in df.columns:
                    df["TotalRevenue"] = df["DailyRate"] * df["RentalDays"]
                else:
                    df["TotalRevenue"] = 0

                # Add metadata
                df["pipeline_run_id"] = pipeline_run_id
                df["load_timestamp"] = datetime.now()

                # Save CSV
                save_csv(df, f"{SILVER_DIR}/{table_name}_{status}.csv")
                transformed_tables[status] = df

            # Quarantine already handled in MedallionPipeline
            logger.info(f"Silver transformation completed for {table_name} | total rows: {len(validated_tables.get('all', []))}")

        else:
            # -------- Master Tables --------
            df = validated_tables.get("clean") or validated_tables.get("all")
            if df is not None and not df.empty:
                df = df.copy()
                df["pipeline_run_id"] = pipeline_run_id
                df["load_timestamp"] = datetime.now()
                save_csv(df, f"{SILVER_DIR}/{table_name}_clean.csv")
                transformed_tables["all"] = df
                logger.info(f"Master table transformation completed for {table_name} | rows: {len(df)}")

        return transformed_tables