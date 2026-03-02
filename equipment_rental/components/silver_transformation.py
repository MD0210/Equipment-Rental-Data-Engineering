from datetime import datetime
import pandas as pd
from equipment_rental.logger.logger import get_logger
import getpass

logger = get_logger()

class SilverTransformation:
    """
    Transforms validated rental transactions for Gold aggregation.
    Adds computed columns and ensures consistent data formats.
    """

    def transform_rentals(
        self, df: pd.DataFrame, source_file: str, pipeline_run_id: str = None
    ) -> pd.DataFrame:
        try:
            df = df.copy()

            # Ensure datetime
            df["StartDate"] = pd.to_datetime(df["StartDate"])
            df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")

            # Compute rental days
            df["RentalDays"] = (df["EndDate"].fillna(pd.Timestamp.today()) - df["StartDate"]).dt.days + 1
            df["RentalDays"] = df["RentalDays"].clip(lower=1)

            # Compute total revenue
            if "TotalRevenue" not in df.columns:
                if "ActualRevenue" in df.columns:
                    df["TotalRevenue"] = df["ActualRevenue"]
                elif "DailyRate" in df.columns:
                    df["TotalRevenue"] = df["DailyRate"] * df["RentalDays"]
                else:
                    df["TotalRevenue"] = 0

            # Compute utilization %
            # Utilization = RentalDays / TotalDays possible in dataset period per equipment
            df["UtilizationPct"] = 0.0
            min_date = df["StartDate"].min()
            max_date = df["EndDate"].max() if df["EndDate"].notna().any() else pd.Timestamp.today()
            total_days_period = (max_date - min_date).days + 1

            utilization = df.groupby("EquipmentID")["RentalDays"].sum() / total_days_period * 100
            df["UtilizationPct"] = df["EquipmentID"].map(utilization).round(2)

            # Standardize Status
            df["Status"] = df["Status"].str.lower()
            df.loc[~df["Status"].isin(["active", "completed", "cancelled"]), "Status"] = "unknown"

            # Add metadata
            current_ts = datetime.now()
            current_user = getpass.getuser()
            df["pipeline_run_id"] = pipeline_run_id
            df["load_timestamp"] = current_ts
            df["source_file"] = source_file
            df["insert_ts"] = current_ts
            df["insert_user"] = current_user
            df["update_ts"] = current_ts
            df["update_user"] = current_user

            logger.info(
                f"Silver transformation completed: {len(df)} rows from {source_file}"
            )

            return df

        except Exception as e:
            logger.error(f"Silver transformation failed: {str(e)}")
            raise