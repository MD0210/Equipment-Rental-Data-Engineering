import pandas as pd
from datetime import datetime
import getpass
import os
from equipment_rental.logger.logger import get_logger
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.components.quarantine_handler import QuarantineHandler
from equipment_rental.constants.constants import SILVER_DIR

logger = get_logger()

class SilverTransformation:

    def __init__(self):
        self.quarantine_handler = QuarantineHandler()
        self.current_ts = datetime.now()
        self.current_user = getpass.getuser()

    def transform(self, validated_tables: dict, table_name: str, pipeline_run_id: str = None):
        """
        Transform validated tables:
        - Compute RentalDays, TotalRevenue
        - Add metadata columns
        - Save CSVs for active/completed/cancelled/all
        - Trigger quarantine handler for flagged rows
        """

        table_name_lower = table_name.lower()

        # -----------------------------
        # Rental_Transactions
        # -----------------------------
        if table_name_lower == "rental_transactions":
            df_all = validated_tables["all"].copy()

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
            df_all["load_timestamp"] = self.current_ts
            df_all["insert_ts"] = self.current_ts
            df_all["insert_user"] = self.current_user
            df_all["update_ts"] = self.current_ts
            df_all["update_user"] = self.current_user

            # -----------------------------
            # Add quarantine info in all table
            # -----------------------------
            quarantine_df = validated_tables.get("quarantine")
            df_all["quarantined"] = False
            df_all["quarantine_reason"] = ""

            if quarantine_df is not None and not quarantine_df.empty:
                df_all.loc[df_all.index.isin(quarantine_df.index), "quarantined"] = True
                df_all.loc[df_all.index.isin(quarantine_df.index), "quarantine_reason"] = "overlapping dates or status mismatch"

                # Trigger quarantine handler
                self.quarantine_handler.create_quarantine(quarantine_df, source_file=table_name)

            # -----------------------------
            # Save CSVs per status
            # -----------------------------
            for status in ["active", "completed", "cancelled", "all"]:
                temp_df = validated_tables.get(status)
                if temp_df is not None and not temp_df.empty:
                    # Add metadata for individual status tables
                    temp_df["pipeline_run_id"] = pipeline_run_id
                    temp_df["load_timestamp"] = self.current_ts
                    temp_df["insert_ts"] = self.current_ts
                    temp_df["insert_user"] = self.current_user
                    temp_df["update_ts"] = self.current_ts
                    temp_df["update_user"] = self.current_user

                    file_path = os.path.join(SILVER_DIR, f"{table_name}_{status}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
                    save_csv(temp_df, file_path)

            logger.info(f"Silver transformation completed for {table_name} | Total rows: {len(df_all)}")
            return df_all

        # -----------------------------
        # Master Tables (Customer_Master, Equipment_Master, Date_Dimension)
        # -----------------------------
        else:
            df_clean = validated_tables.get("clean") or validated_tables.get("all")
            if df_clean is None or df_clean.empty:
                logger.warning(f"No data to transform for master table {table_name}")
                return pd.DataFrame()

            # Add metadata
            df_clean["pipeline_run_id"] = pipeline_run_id
            df_clean["load_timestamp"] = self.current_ts
            df_clean["insert_ts"] = self.current_ts
            df_clean["insert_user"] = self.current_user
            df_clean["update_ts"] = self.current_ts
            df_clean["update_user"] = self.current_user

            file_path = os.path.join(SILVER_DIR, f"{table_name}_clean_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
            save_csv(df_clean, file_path)

            logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")
            return df_clean