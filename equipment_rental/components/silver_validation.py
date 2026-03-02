import pandas as pd
from datetime import datetime
from equipment_rental.logger.logger import get_logger
import getpass

logger = get_logger()


class SilverValidation:

    def validate(
        self,
        df: pd.DataFrame,
        table_name: str,
        source_file: str,
        pipeline_run_id: str = None
    ) -> dict:

        try:
            table_name = table_name.lower()

            if table_name == "rental_transactions":
                return self._validate_rentals(df, source_file, pipeline_run_id)

            else:
                return self._validate_master_table(df, table_name, source_file, pipeline_run_id)

        except Exception as e:
            logger.error(f"Silver validation failed: {str(e)}")
            raise

    # ============================================
    # RENTAL TRANSACTIONS VALIDATION
    # ============================================
    def _validate_rentals(self, df, source_file, pipeline_run_id):

        required_cols = [
            "TransactionID",
            "EquipmentID",
            "CustomerID",
            "StartDate",
            "EndDate",
            "Status"
        ]

        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Drop rows missing critical IDs
        df = df.dropna(subset=["EquipmentID", "CustomerID"])

        df["StartDate"] = pd.to_datetime(df["StartDate"])
        df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")
        df["Status"] = df["Status"].fillna("active")

        df["overlap_flag"] = False

        # Overlap detection
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

        df = self._add_metadata(df, source_file, pipeline_run_id)

        return {
            "active": df[(df["EndDate"].isna()) & (~df["overlap_flag"])],
            "completed": df[(df["EndDate"].notna()) & (~df["overlap_flag"])],
            "cancelled": df[df["Status"].str.lower() == "cancelled"],
            "all": df,
            "quarantine": df[df["overlap_flag"]]
        }

    # ============================================
    # GENERIC MASTER TABLE VALIDATION
    # ============================================
    def _validate_master_table(self, df, table_name, source_file, pipeline_run_id):

        # Identify ID columns automatically
        id_columns = [col for col in df.columns if "id" in col.lower()]

        if not id_columns:
            logger.warning(f"No ID column detected in {table_name}")

        # Remove rows with null IDs
        for col in id_columns:
            df = df[df[col].notna()]

        # Remove duplicate IDs
        for col in id_columns:
            df = df.drop_duplicates(subset=[col])

        df = self._add_metadata(df, source_file, pipeline_run_id)

        logger.info(f"Master validation completed for {table_name} | Rows: {len(df)}")

        return {
            "clean": df,
            "all": df
        }

    # ============================================
    # METADATA HELPER
    # ============================================
    def _add_metadata(self, df, source_file, pipeline_run_id):

        current_ts = datetime.now()
        current_user = getpass.getuser()

        df["pipeline_run_id"] = pipeline_run_id
        df["load_timestamp"] = current_ts
        df["source_file"] = source_file
        df["insert_ts"] = current_ts
        df["insert_user"] = current_user
        df["update_ts"] = current_ts
        df["update_user"] = current_user

        return df