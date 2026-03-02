# equipment_rental/components/quarantine_handler.py
from datetime import datetime
import pandas as pd
from equipment_rental.logger.logger import get_logger
import getpass

logger = get_logger()

class QuarantineHandler:
    """
    Handles quarantining rental transactions based on business rules:
    - Overlapping rentals per EquipmentID
    - Missing or inconsistent dates
    - Status inconsistencies
    """

    def create_quarantine(self, df: pd.DataFrame, source_file: str, pipeline_run_id: str = None) -> dict:
        """
        Processes the rental dataframe and splits into:
        active, completed, cancelled, all, and quarantine datasets.
        Adds metadata (timestamp, source_file, audit columns)
        Returns a dictionary of DataFrames.
        """
        try:
            df = df.copy()

            # Ensure dates are datetime
            df["StartDate"] = pd.to_datetime(df["StartDate"])
            df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")

            # Sort by EquipmentID and StartDate
            df = df.sort_values(["EquipmentID", "StartDate"])

            # Initialize overlap flag
            df["overlap_flag"] = False

            # Check for overlapping rentals per EquipmentID
            for equip_id, group in df.groupby("EquipmentID"):
                group = group.sort_values("StartDate")
                for i in range(1, len(group)):
                    prev_end = group.iloc[i - 1]["EndDate"]
                    curr_start = group.iloc[i]["StartDate"]
                    # If current start <= previous end, mark overlap
                    if pd.notna(prev_end) and curr_start <= prev_end:
                        df.loc[group.index[i], "overlap_flag"] = True
                        df.loc[group.index[i - 1], "overlap_flag"] = True

            # Split datasets
            active_df = df[(df["EndDate"].isna()) & (df["overlap_flag"] == False) & (df["Status"].str.lower() == "active")].copy()
            completed_df = df[(df["EndDate"].notna()) & (df["overlap_flag"] == False) & (df["Status"].str.lower() == "completed")].copy()
            cancelled_df = df[df["Status"].str.lower() == "cancelled"].copy()
            all_df = df.copy()
            quarantine_df = df[df["overlap_flag"] == True].copy()

            # Add metadata / audit columns
            current_ts = datetime.now()
            current_user = getpass.getuser()
            for temp_df in [active_df, completed_df, cancelled_df, all_df, quarantine_df]:
                temp_df["pipeline_run_id"] = pipeline_run_id
                temp_df["load_timestamp"] = current_ts
                temp_df["source_file"] = source_file
                temp_df["insert_ts"] = current_ts
                temp_df["insert_user"] = current_user
                temp_df["update_ts"] = current_ts
                temp_df["update_user"] = current_user

            logger.info(
                f"Quarantine process completed. Active: {len(active_df)}, "
                f"Completed: {len(completed_df)}, Cancelled: {len(cancelled_df)}, "
                f"Quarantine: {len(quarantine_df)}"
            )

            return {
                "active": active_df,
                "completed": completed_df,
                "cancelled": cancelled_df,
                "all": all_df,
                "quarantine": quarantine_df
            }

        except Exception as e:
            logger.error(f"Quarantine processing failed: {str(e)}")
            raise