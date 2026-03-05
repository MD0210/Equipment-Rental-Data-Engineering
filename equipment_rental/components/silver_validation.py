import pandas as pd
from datetime import datetime
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import (
    SilverValidationException,
    QuarantineProcessingException
)

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

        try:

            table_name = table_name.lower()

            if table_name == "rental_transactions":
                return self._validate_rental_transactions(
                    df, source_file, pipeline_run_id
                )
            else:
                return self._validate_master_table(
                    df, table_name, source_file, pipeline_run_id
                )

        except Exception as e:

            logger.error(
                f"Silver validation failed | table={table_name} | error={str(e)}"
            )

            raise SilverValidationException(
                f"Validation failed for table: {table_name}",
                errors=str(e)
            ) from e

    # ============================================================
    # RENTAL TRANSACTIONS VALIDATION
    # ============================================================
    def _validate_rental_transactions(self, df, source_file, pipeline_run_id):

        try:

            required_cols = [
                "TransactionID", "EquipmentID", "CustomerID",
                "StartDate", "EndDate",
                "RentalDays", "DailyRate",
                "ActualRevenue", "Status"
            ]

            for col in required_cols:
                if col not in df.columns:
                    raise SilverValidationException(
                        f"Missing required column: {col}"
                    )

            df = df.copy()

            # Standardise types
            df["TransactionID"] = df["TransactionID"].astype(str)
            df["EquipmentID"] = df["EquipmentID"].astype(str)
            df["CustomerID"] = df["CustomerID"].astype(str)
            df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
            df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")
            df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce")
            df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce")
            df["ActualRevenue"] = pd.to_numeric(df["ActualRevenue"], errors="coerce")
            df["Status"] = df["Status"].str.lower().fillna("active")

            # Initialize quarantine columns
            df["quarantined"] = 0
            df["quarantine_reason"] = ""

            def quarantine(row, reason):

                if row["quarantine_reason"]:
                    row["quarantine_reason"] += f", {reason}"
                    row["quarantined"] += 1
                else:
                    row["quarantine_reason"] = reason
                    row["quarantined"] = 1

                return row

            # 1️⃣ Critical Null Checks
            mask = df["EquipmentID"].isna() | df["CustomerID"].isna() | df["StartDate"].isna()
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Missing critical field"), axis=1)

            # 2️⃣ Duplicate TransactionID
            mask = df.duplicated(subset=["TransactionID"], keep=False)
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Duplicate TransactionID"), axis=1)

            # 3️⃣ Invalid RentalDays
            mask = df["RentalDays"] <= 0
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Invalid RentalDays (<=0)"), axis=1)

            # 4️⃣ Invalid DailyRate
            mask = df["DailyRate"] <= 0
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Invalid DailyRate (<=0)"), axis=1)

            # 5️⃣ RentalDays mismatch
            date_diff = (df["EndDate"] - df["StartDate"]).dt.days
            mask = df["EndDate"].notna() & df["RentalDays"].notna() & (date_diff != df["RentalDays"])
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "RentalDays mismatch"), axis=1)

            # 6️⃣ Status vs EndDate checks
            mask = (df["Status"] == "active") & df["EndDate"].notna()
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Active but has EndDate"), axis=1)

            mask = (df["Status"] == "completed") & df["EndDate"].isna()
            df.loc[mask] = df.loc[mask].apply(lambda r: quarantine(r, "Completed but missing EndDate"), axis=1)

            # 7️⃣ Overlapping Rentals
            for equip_id, group in df.groupby("EquipmentID"):

                group = group.sort_values("StartDate").reset_index()

                for i in range(len(group) - 1):

                    t1_end = pd.to_datetime(group.loc[i, "EndDate"]).normalize() \
                        if pd.notna(group.loc[i, "EndDate"]) else pd.Timestamp.max

                    t2_start = pd.to_datetime(group.loc[i + 1, "StartDate"]).normalize()

                    if t1_end > t2_start:

                        t1_idx = group.loc[i, "index"]
                        t2_idx = group.loc[i + 1, "index"]

                        overlap_ids = f"{group.loc[i, 'TransactionID']},{group.loc[i + 1, 'TransactionID']}"

                        for idx in [t1_idx, t2_idx]:

                            df.loc[idx, "quarantined"] = int(df.loc[idx, "quarantined"]) + 1

                            existing_reason = df.loc[idx, "quarantine_reason"]
                            new_reason = f"Overlapping rental with {overlap_ids}"

                            if existing_reason:
                                df.loc[idx, "quarantine_reason"] = f"{existing_reason}; {new_reason}"
                            else:
                                df.loc[idx, "quarantine_reason"] = new_reason

            # Metadata
            df["pipeline_run_id"] = pipeline_run_id
            df["load_timestamp"] = datetime.now()
            df["source_file"] = source_file

            active_df = df[(df["Status"] == "active") & (df["quarantined"] == 0)]
            completed_df = df[(df["Status"] == "completed") & (df["quarantined"] == 0)]
            cancelled_df = df[(df["Status"] == "cancelled") & (df["quarantined"] == 0)]
            quarantine_df = df[df["quarantined"] > 0]
            all_df = df.copy()

            # Equipment Utilisation
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

        except Exception as e:

            logger.error(
                f"Rental transaction validation failed | error={str(e)}"
            )

            raise SilverValidationException(
                "Rental transaction validation failed",
                errors=str(e)
            ) from e

    # ============================================================
    # MASTER TABLE VALIDATION
    # ============================================================
    def _validate_master_table(self, df, table_name, source_file, pipeline_run_id):

        try:

            df = df.copy()

            id_columns = [col for col in df.columns if "id" in col.lower()]

            if not id_columns:
                logger.warning(f"No ID column detected in {table_name}")

            for col in id_columns:
                df = df[df[col].notna()]

            df = df.drop_duplicates(keep="first")

            df["pipeline_run_id"] = pipeline_run_id
            df["load_timestamp"] = datetime.now()
            df["source_file"] = source_file

            logger.info(
                f"{table_name} validation completed | Rows: {len(df)}"
            )

            return {
                "clean": df,
                "all": df
            }

        except Exception as e:

            logger.error(
                f"Master table validation failed | table={table_name} | error={str(e)}"
            )

            raise SilverValidationException(
                f"Validation failed for master table: {table_name}",
                errors=str(e)
            ) from e