# equipment_rental/components/silver_validation.py
import pandas as pd
from datetime import datetime
from equipment_rental.logger.logger import get_logger
import getpass

logger = get_logger()

class SilverValidation:
    """
    Validates rental transactions and splits them into Silver tables:
    active, completed, cancelled, all, quarantine.
    """

    def validate_rentals(
        self, df: pd.DataFrame, source_file: str, pipeline_run_id: str = None
    ) -> dict:
        try:
            required_cols = ["TransactionID", "EquipmentID", "CustomerID", "StartDate", "EndDate", "Status"]
            for col in required_cols:
                if col not in df.columns:
                    raise ValueError(f"Missing required column: {col}")

            # Drop rows with missing EquipmentID or CustomerID
            valid_df = df.dropna(subset=["EquipmentID", "CustomerID"])
            invalid_df = df[~df.index.isin(valid_df.index)]
            if not invalid_df.empty:
                logger.warning(f"{len(invalid_df)} rows removed due to missing EquipmentID or CustomerID")

            df = valid_df.copy()

            # Ensure datetime
            df["StartDate"] = pd.to_datetime(df["StartDate"])
            df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")  # active rentals may have NaT

            # Handle missing Status
            df["Status"] = df["Status"].fillna("active")

            # Initialize overlap flag
            df["overlap_flag"] = False

            # Overlap detection per EquipmentID
            for equip_id, group in df.groupby("EquipmentID"):
                group = group.sort_values("StartDate")
                for i, t1 in enumerate(group.itertuples()):
                    t1_end = t1.EndDate if pd.notna(t1.EndDate) else pd.Timestamp.max
                    for j in range(i + 1, len(group)):
                        t2 = group.iloc[j]
                        t2_end = t2.EndDate if pd.notna(t2.EndDate) else pd.Timestamp.max
                        if t1.StartDate <= t2_end and t2.StartDate <= t1_end:
                            df.loc[t1.Index, "overlap_flag"] = True
                            df.loc[t2.name, "overlap_flag"] = True

            # Add metadata columns
            current_ts = datetime.now()
            current_user = getpass.getuser()
            df["pipeline_run_id"] = pipeline_run_id
            df["load_timestamp"] = current_ts
            df["source_file"] = source_file
            df["insert_ts"] = current_ts
            df["insert_user"] = current_user
            df["update_ts"] = current_ts
            df["update_user"] = current_user

            # Split tables
            active_df = df[(df["EndDate"].isna()) & (df["overlap_flag"] == False) & (df["Status"].str.lower() == "active")]
            completed_df = df[(df["EndDate"].notna()) & (df["overlap_flag"] == False) & (df["Status"].str.lower() == "completed")]
            cancelled_df = df[df["Status"].str.lower() == "cancelled"]
            all_df = df.copy()
            quarantine_df = df[df["overlap_flag"] == True]

            logger.info(f"Silver validation completed: {len(df)} rows processed | "
                        f"Active: {len(active_df)}, Completed: {len(completed_df)}, "
                        f"Cancelled: {len(cancelled_df)}, Quarantine: {len(quarantine_df)}")

            return {
                "active": active_df,
                "completed": completed_df,
                "cancelled": cancelled_df,
                "all": all_df,
                "quarantine": quarantine_df
            }

        except Exception as e:
            logger.error(f"Silver validation failed: {str(e)}")
            raise