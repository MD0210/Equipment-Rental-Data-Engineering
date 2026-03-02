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
        - Save CSVs for active/completed/cancelled/all (for rentals)
        - Save master tables
        - Trigger quarantine handler for rental_transactions
        """
        transformed_tables = {}

        # -------- Rental Transactions --------
        if table_name.lower() == "rental_transactions":
            df_all = validated_tables.get("all")
            if df_all is not None and not df_all.empty:
                df_all = df_all.copy()
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

                # Save CSVs for each status
                for status in ["active", "completed", "cancelled", "all"]:
                    df_status = validated_tables.get(status)
                    if df_status is not None and not df_status.empty:
                        save_csv(df_status, f"{SILVER_DIR}/{table_name}_{status}.csv")
                        transformed_tables[status] = df_status

                # Trigger quarantine handler
                quarantine_df = validated_tables.get("quarantine")
                if quarantine_df is not None and not quarantine_df.empty:
                    self.quarantine_handler.save_quarantine(
                        df=quarantine_df,
                        table_name=table_name,
                        pipeline_run_id=pipeline_run_id
                    )
                    logger.warning(f"{len(quarantine_df)} rows quarantined | table: {table_name}")

            return transformed_tables

        # -------- Master Tables --------
        else:
            df_clean = validated_tables.get("clean") or validated_tables.get("all")
            if df_clean is not None and not df_clean.empty:
                df_clean = df_clean.copy()
                df_clean["pipeline_run_id"] = pipeline_run_id
                df_clean["load_timestamp"] = datetime.now()
                save_csv(df_clean, f"{SILVER_DIR}/{table_name}_clean.csv")
                transformed_tables["all"] = df_clean
                logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")

            return transformed_tables