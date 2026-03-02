# equipment_rental/components/silver_transformation.py
from datetime import datetime
import pandas as pd
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
        - Compute RentalDays, TotalRevenue for Rental_Transactions
        - Join master tables if needed
        - Save CSVs for active/completed/cancelled/all
        - Trigger quarantine handler
        """
        # -------- Rental Transactions --------
        if table_name.lower() == "rental_transactions":
            df_all = validated_tables.get("all")
            if df_all is None or df_all.empty:
                logger.warning(f"No data found for {table_name} to transform")
                return {}

            df_all = df_all.copy()
            # Compute RentalDays
            df_all["RentalDays"] = (df_all["EndDate"].fillna(pd.Timestamp.today()) - df_all["StartDate"]).dt.days + 1
            df_all["RentalDays"] = df_all["RentalDays"].clip(lower=1)

            # Compute TotalRevenue
            if "ActualRevenue" in df_all.columns:
                df_all["TotalRevenue"] = df_all["ActualRevenue"]
            elif "DailyRate" in df_all.columns:
                df_all["TotalRevenue"] = df_all["DailyRate"] * df_all["RentalDays"]
            else:
                df_all["TotalRevenue"] = 0

            # Add metadata
            df_all["pipeline_run_id"] = pipeline_run_id
            df_all["load_timestamp"] = datetime.now()

            # Save CSVs per status
            for status in ["active", "completed", "cancelled", "all"]:
                temp_df = validated_tables.get(status)
                if temp_df is not None and not temp_df.empty:
                    save_csv(temp_df, f"{SILVER_DIR}/{table_name}_{status}.csv")

            # Trigger quarantine handler
            quarantine_df = validated_tables.get("quarantine")
            if quarantine_df is not None and not quarantine_df.empty:
                self.quarantine_handler.save_quarantine(
                    df=quarantine_df,
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )
                logger.warning(f"{len(quarantine_df)} rows quarantined | table: {table_name}")

            logger.info(f"Silver transformation completed for {table_name} | Rows: {len(df_all)}")
            return {"all": df_all}

        # -------- Master Tables --------
        else:
            df_clean = validated_tables.get("clean")
            if df_clean is None or df_clean.empty:
                df_clean = validated_tables.get("all")
            if df_clean is None:
                df_clean = pd.DataFrame()  # fallback to empty

            if not df_clean.empty:
                df_clean = df_clean.copy()
                df_clean["pipeline_run_id"] = pipeline_run_id
                df_clean["load_timestamp"] = datetime.now()
                save_csv(df_clean, f"{SILVER_DIR}/{table_name}_clean.csv")
                logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")

            return {"all": df_clean}