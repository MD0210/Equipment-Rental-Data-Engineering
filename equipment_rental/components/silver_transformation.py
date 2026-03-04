# equipment_rental/components/silver_transformation.py
from datetime import datetime
import pandas as pd
from equipment_rental.logger.logger import get_logger
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.constants.constants import SILVER_DIR
from equipment_rental.components.quarantine_handler import QuarantineHandler

logger = get_logger()

class SilverTransformation:
    """
    Silver Transformation Layer

    Responsibilities:
    - Apply final business transformations
    - Compute derived metrics
    - Save clean datasets
    - Optionally skip quarantine and equipment utilisation
    - Ensure metadata consistency
    """

    def __init__(self, save_quarantine: bool = False, save_utilisation: bool = True):
        """
        :param save_quarantine: Whether to save quarantine CSVs
        :param save_utilisation: Whether to save equipment utilisation CSV
        """
        self.quarantine_handler = QuarantineHandler()
        self.save_quarantine = save_quarantine
        self.save_utilisation = save_utilisation

    def transform(self, validated_tables: dict,
                  table_name: str,
                  pipeline_run_id: str = None) -> dict:

        table_name = table_name.lower()

        if table_name == "rental_transactions":
            return self._transform_rental_transactions(validated_tables, table_name, pipeline_run_id)
        else:
            return self._transform_master_table(validated_tables, table_name, pipeline_run_id)

    def _transform_rental_transactions(self, validated_tables: dict, table_name: str, pipeline_run_id: str):
        df_all = validated_tables.get("all")
        if df_all is None or df_all.empty:
            logger.warning(f"No data found for {table_name} to transform")
            return {}

        df_all = df_all.copy()

        # Ensure RentalDays consistency
        df_all["ComputedRentalDays"] = ((df_all["EndDate"].fillna(pd.Timestamp.today())
                                        - df_all["StartDate"]).dt.days + 1).clip(lower=1)
        if "RentalDays" in df_all.columns:
            df_all["RentalDays"] = df_all["ComputedRentalDays"]

        # Revenue calculation
        if "DailyRate" in df_all.columns:
            df_all["ExpectedRevenue"] = df_all["RentalDays"] * df_all["DailyRate"]
        df_all["TotalRevenue"] = df_all.get("ActualRevenue", df_all.get("ExpectedRevenue", 0))

        if "ExpectedRevenue" in df_all.columns and "ActualRevenue" in df_all.columns:
            df_all["RevenueDifference"] = df_all["ActualRevenue"] - df_all["ExpectedRevenue"]

        # Metadata
        df_all["pipeline_run_id"] = pipeline_run_id
        df_all["load_timestamp"] = datetime.now()

        outputs = {}

        # Save only the desired statuses
        for status in ["active", "completed", "cancelled"]:
            temp_df = validated_tables.get(status)
            if temp_df is not None and not temp_df.empty:
                temp_df = df_all.loc[temp_df.index]
                save_csv(temp_df, f"{SILVER_DIR}/{table_name}_{status}.csv")
                outputs[status] = temp_df

        # Save full dataset
        save_csv(df_all, f"{SILVER_DIR}/{table_name}_all.csv")
        outputs["all"] = df_all

        # Save equipment utilisation only if requested
        if self.save_utilisation:
            utilisation_df = validated_tables.get("equipment_utilisation")
            if utilisation_df is not None and not utilisation_df.empty:
                utilisation_df = utilisation_df.copy()
                utilisation_df["pipeline_run_id"] = pipeline_run_id
                utilisation_df["load_timestamp"] = datetime.now()
                save_csv(utilisation_df, f"{SILVER_DIR}/equipment_utilisation.csv")
                outputs["equipment_utilisation"] = utilisation_df

        # Handle quarantine only if requested
        if self.save_quarantine:
            quarantine_df = validated_tables.get("quarantine")
            if quarantine_df is not None and not quarantine_df.empty:
                self.quarantine_handler.save_quarantine(
                    df=quarantine_df,
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )
                outputs["quarantine"] = quarantine_df

        logger.info(f"Silver transformation completed for {table_name} | Rows: {len(df_all)}")
        return outputs

    def _transform_master_table(self, validated_tables: dict, table_name: str, pipeline_run_id: str):
        df_clean = validated_tables.get("clean") or validated_tables.get("all")
        if df_clean is None or df_clean.empty:
            logger.warning(f"No data found for {table_name}")
            return {}

        df_clean = df_clean.copy()
        df_clean["pipeline_run_id"] = pipeline_run_id
        df_clean["load_timestamp"] = datetime.now()

        save_csv(df_clean, f"{SILVER_DIR}/{table_name}_clean.csv")
        logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")
        return {"all": df_clean}