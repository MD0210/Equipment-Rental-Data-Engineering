import pandas as pd
from datetime import datetime
import getpass
from equipment_rental.logger.logger import get_logger
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.components.quarantine_handler import QuarantineHandler
from equipment_rental.constants.constants import SILVER_DIR

logger = get_logger()

class SilverTransformation:

    def __init__(self):
        self.quarantine_handler = QuarantineHandler()

    def transform(self, validated_tables: dict, table_name: str, pipeline_run_id: str = None):
        """
        Transform validated tables:
        - Compute RentalDays, TotalRevenue
        - Join master tables
        - Save CSVs for active/completed/cancelled/all
        - Trigger quarantine handler
        """
        if table_name.lower() == "rental_transactions":
            df_all = validated_tables["all"].copy()
            df_all["RentalDays"] = (df_all["EndDate"].fillna(pd.Timestamp.today()) - df_all["StartDate"]).dt.days + 1
            df_all["RentalDays"] = df_all["RentalDays"].clip(lower=1)

            if "ActualRevenue" in df_all.columns:
                df_all["TotalRevenue"] = df_all["ActualRevenue"]
            elif "DailyRate" in df_all.columns:
                df_all["TotalRevenue"] = df_all["DailyRate"] * df_all["RentalDays"]
            else:
                df_all["TotalRevenue"] = 0

            df_all["pipeline_run_id"] = pipeline_run_id
            df_all["load_timestamp"] = datetime.now()

            # -------------------
            # Save CSVs
            # -------------------
            for status in ["active", "completed", "cancelled", "all"]:
                temp_df = validated_tables[status]
                save_csv(temp_df, f"{SILVER_DIR}/{table_name}_{status}.csv")

            # -------------------
            # Trigger quarantine handler
            # -------------------
            quarantine_df = validated_tables["quarantine"]
            if not quarantine_df.empty:
                self.quarantine_handler.create_quarantine(quarantine_df, source_file=table_name)

            logger.info(f"Silver transformation completed for {table_name} | Rows: {len(df_all)}")
            return df_all

        else:
            # For master tables, just save CSV
            df_clean = validated_tables["clean"]
            save_csv(df_clean, f"{SILVER_DIR}/{table_name}_clean.csv")
            logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")
            return df_clean