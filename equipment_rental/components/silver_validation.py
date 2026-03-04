import pandas as pd
from datetime import datetime
from equipment_rental.logger.logger import get_logger

logger = get_logger()


class SilverValidation:
    """
    Silver Layer Validation

    Responsibilities:
    - Validate schema
    - Enforce business rules
    - Detect duplicates & nulls
    - Revenue reconciliation
    - Overlap detection
    - Quarantine invalid records
    - Calculate business metrics
    """

    def validate(self, df: pd.DataFrame, table_name: str,
                 source_file: str,
                 pipeline_run_id: str = None) -> dict:

        table_name = table_name.lower()

        if table_name == "rental_transactions":
            return self._validate_rental_transactions(
                df, source_file, pipeline_run_id
            )
        else:
            return self._validate_master_table(
                df, table_name, source_file, pipeline_run_id
            )

    # ============================================================
    # RENTAL TRANSACTIONS VALIDATION
    # ============================================================
    def _validate_rental_transactions(self, df, source_file, pipeline_run_id):

        required_cols = [
            "TransactionID", "EquipmentID", "CustomerID",
            "StartDate", "EndDate",
            "RentalDays", "DailyRate",
            "ActualRevenue", "Status"
        ]

        # -------------------------
        # Schema Validation
        # -------------------------
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        df = df.copy()

        # -------------------------
        # Standardise Data Types
        # -------------------------
        df["TransactionID"] = df["TransactionID"].astype(str)
        df["EquipmentID"] = df["EquipmentID"].astype(str)
        df["CustomerID"] = df["CustomerID"].astype(str)

        df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
        df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")

        df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce")
        df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce")
        df["ActualRevenue"] = pd.to_numeric(df["ActualRevenue"], errors="coerce")

        df["Status"] = df["Status"].str.lower().fillna("active")

        # -------------------------
        # Initialize quarantine columns
        # -------------------------
        df["quarantined"] = False
        df["quarantine_reason"] = None

        # ============================================================
        # DATA QUALITY RULES
        # ============================================================

        # 1️⃣ Critical Null Checks
        critical_nulls = (
            df["EquipmentID"].isna() |
            df["CustomerID"].isna() |
            df["StartDate"].isna()
        )
        df.loc[critical_nulls, ["quarantined", "quarantine_reason"]] = \
            [True, "Missing critical field"]

        # 2️⃣ Duplicate TransactionID
        duplicate_txn = df.duplicated(subset=["TransactionID"], keep=False)
        df.loc[duplicate_txn, ["quarantined", "quarantine_reason"]] = \
            [True, "Duplicate TransactionID"]

        # 3️⃣ Invalid RentalDays
        invalid_days = df["RentalDays"] <= 0
        df.loc[invalid_days, ["quarantined", "quarantine_reason"]] = \
            [True, "Invalid RentalDays (<=0)"]

        # 4️⃣ Invalid DailyRate
        invalid_rate = df["DailyRate"] <= 0
        df.loc[invalid_rate, ["quarantined", "quarantine_reason"]] = \
            [True, "Invalid DailyRate (<=0)"]

        # 5️⃣ RentalDays mismatch with date difference
        date_diff = (df["EndDate"] - df["StartDate"]).dt.days
        mismatch_days = (
            df["EndDate"].notna() &
            df["RentalDays"].notna() &
            (date_diff != df["RentalDays"])
        )
        df.loc[mismatch_days, ["quarantined", "quarantine_reason"]] = \
            [True, "RentalDays mismatch with date difference"]

        # 6️⃣ Status vs EndDate consistency
        active_but_has_end = (
            (df["Status"] == "active") &
            df["EndDate"].notna()
        )
        df.loc[active_but_has_end, ["quarantined", "quarantine_reason"]] = \
            [True, "Active but has EndDate"]

        completed_but_no_end = (
            (df["Status"] == "completed") &
            df["EndDate"].isna()
        )
        df.loc[completed_but_no_end, ["quarantined", "quarantine_reason"]] = \
            [True, "Completed but missing EndDate"]


        # 7️⃣ Overlapping Rentals Detection
        for equip_id, group in df.groupby("EquipmentID"):
            group = group.sort_values("StartDate").reset_index()

            for i in range(len(group) - 1):
                t1_end = pd.to_datetime(group.loc[i, "EndDate"]).normalize() if pd.notna(group.loc[i, "EndDate"]) else pd.Timestamp.max
                t2_start = pd.to_datetime(group.loc[i + 1, "StartDate"]).normalize()

                if t1_end > t2_start:  # Only flag if strictly overlaps
                    t1_idx = group.loc[i, "index"]
                    t2_idx = group.loc[i + 1, "index"]

                    overlap_ids = f"{group.loc[i, 'TransactionID']},{group.loc[i + 1, 'TransactionID']}"

                    df.loc[t1_idx, ["quarantined", "quarantine_reason"]] = [True, f"Overlapping rental with {overlap_ids}"]
                    df.loc[t2_idx, ["quarantined", "quarantine_reason"]] = [True, f"Overlapping rental with {overlap_ids}"]


        # ============================================================
        # Metadata Enrichment
        # ============================================================
        df["pipeline_run_id"] = pipeline_run_id
        df["load_timestamp"] = datetime.now()
        df["source_file"] = source_file

        # ============================================================
        # Split into Logical Outputs
        # ============================================================
        active_df = df[(df["Status"] == "active") & (~df["quarantined"])]
        completed_df = df[(df["Status"] == "completed") & (~df["quarantined"])]
        cancelled_df = df[(df["Status"] == "cancelled") & (~df["quarantined"])]
        quarantine_df = df[df["quarantined"]]
        all_df = df.copy()

        # ============================================================
        # Business Metrics – Equipment Utilisation
        # ============================================================
        utilisation_df = (
            completed_df
            .groupby("EquipmentID")["RentalDays"]
            .sum()
            .reset_index()
        )
        utilisation_df.rename(
            columns={"RentalDays": "TotalRentalDays"},
            inplace=True
        )

        logger.info(
            f"Rental_Transactions validation completed | "
            f"Total: {len(df)} | "
            f"Active: {len(active_df)}, "
            f"Completed: {len(completed_df)}, "
            f"Cancelled: {len(cancelled_df)}, "
            f"Quarantined: {len(quarantine_df)}"
        )

        return {
            "active": active_df,
            "completed": completed_df,
            "cancelled": cancelled_df,
            "quarantine": quarantine_df,
            "all": all_df,
            "equipment_utilisation": utilisation_df
        }

    # ============================================================
    # MASTER TABLE VALIDATION
    # ============================================================
    def _validate_master_table(self, df, table_name, source_file, pipeline_run_id):
        df = df.copy()

        # Detect ID columns
        id_columns = [col for col in df.columns if "id" in col.lower()]

        if not id_columns:
            logger.warning(f"No ID column detected in {table_name}")

        # Keep only rows with non-null IDs
        for col in id_columns:
            df = df[df[col].notna()]

        # Remove full duplicate rows only (all columns)
        df = df.drop_duplicates(keep="first")

        # Metadata enrichment
        df["pipeline_run_id"] = pipeline_run_id
        df["load_timestamp"] = datetime.now()
        df["source_file"] = source_file

        # Save info
        logger.info(f"{table_name} validation completed | Rows: {len(df)}")

        return {
            "clean": df,
            "all": df
        }