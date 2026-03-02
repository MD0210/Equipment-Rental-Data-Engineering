import pandas as pd
from datetime import datetime
import getpass
from equipment_rental.logger.logger import get_logger

logger = get_logger()

class SilverValidation:
    """
    Validates each table according to schema.
    Rental_Transactions special handling: detects overlapping rentals, invalid dates, status inconsistencies.
    """

    def validate(self, df: pd.DataFrame, table_name: str, source_file: str, pipeline_run_id: str = None) -> dict:
        table_name = table_name.lower()

        if table_name == "rental_transactions":
            return self._validate_rental_transactions(df, source_file, pipeline_run_id)
        else:
            return self._validate_master_table(df, table_name, source_file, pipeline_run_id)

    # ------------------------------
    # RENTAL TRANSACTIONS VALIDATION
    # ------------------------------
    def _validate_rental_transactions(self, df, source_file, pipeline_run_id):
        required_cols = [
            "TransactionID", "EquipmentID", "CustomerID",
            "StartDate", "EndDate", "Status", "RentalDays", "DailyRate"
        ]

        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        df = df.copy()
        df["StartDate"] = pd.to_datetime(df["StartDate"])
        df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")
        df["Status"] = df["Status"].str.lower().fillna("active")

        # Initialize quarantine columns
        df["quarantined"] = False
        df["quarantine_reason"] = None

        # -------------------
        # Overlap detection
        # -------------------
        df = df.sort_values(["EquipmentID", "StartDate"])
        for equip_id, group in df.groupby("EquipmentID"):
            for i, t1 in enumerate(group.itertuples()):
                t1_end = t1.EndDate if pd.notna(t1.EndDate) else pd.Timestamp.max
                for j in range(i + 1, len(group)):
                    t2 = group.iloc[j]
                    t2_end = t2.EndDate if pd.notna(t2.EndDate) else pd.Timestamp.max
                    # Overlapping rentals
                    if t1.StartDate <= t2_end and t2.StartDate <= t1_end:
                        df.loc[t1.Index, "quarantined"] = True
                        df.loc[t1.Index, "quarantine_reason"] = "Overlapping rental"
                        df.loc[t2.name, "quarantined"] = True
                        df.loc[t2.name, "quarantine_reason"] = "Overlapping rental"

        # -------------------
        # Status vs EndDate consistency
        # -------------------
        active_but_has_end = (df["Status"] == "active") & df["EndDate"].notna()
        df.loc[active_but_has_end, ["quarantined", "quarantine_reason"]] = [True, "Active but has EndDate"]

        completed_but_no_end = (df["Status"] == "completed") & df["EndDate"].isna()
        df.loc[completed_but_no_end, ["quarantined", "quarantine_reason"]] = [True, "Completed but missing EndDate"]

        # -------------------
        # Split tables
        # -------------------
        df["pipeline_run_id"] = pipeline_run_id
        df["load_timestamp"] = datetime.now()
        df["source_file"] = source_file

        active_df = df[(df["Status"]=="active") & (~df["quarantined"])]
        completed_df = df[(df["Status"]=="completed") & (~df["quarantined"])]
        cancelled_df = df[(df["Status"]=="cancelled") & (~df["quarantined"])]
        quarantine_df = df[df["quarantined"]]
        all_df = df.copy()

        logger.info(f"Rental_Transactions validation completed | Total: {len(df)} | Active: {len(active_df)}, Completed: {len(completed_df)}, Cancelled: {len(cancelled_df)}, Quarantined: {len(quarantine_df)}")

        return {
            "active": active_df,
            "completed": completed_df,
            "cancelled": cancelled_df,
            "all": all_df,
            "quarantine": quarantine_df
        }

    # ------------------------------
    # MASTER TABLE VALIDATION
    # ------------------------------
    def _validate_master_table(self, df, table_name, source_file, pipeline_run_id):
        df = df.copy()
        # Identify ID columns automatically
        id_columns = [col for col in df.columns if "id" in col.lower()]
        if not id_columns:
            logger.warning(f"No ID column detected in {table_name}")

        # Drop rows with null IDs
        for col in id_columns:
            df = df[df[col].notna()]

        # Drop duplicates
        for col in id_columns:
            df = df.drop_duplicates(subset=[col])

        df["pipeline_run_id"] = pipeline_run_id
        df["load_timestamp"] = datetime.now()
        df["source_file"] = source_file

        logger.info(f"{table_name} validation completed | Rows: {len(df)}")
        return {"clean": df, "all": df}