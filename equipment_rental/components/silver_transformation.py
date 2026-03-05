# equipment_rental/components/silver_validation.py
from datetime import datetime
import pandas as pd
from equipment_rental.logger.logger import get_logger
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.components.quarantine_handler import QuarantineHandler
from equipment_rental.constants.constants import SILVER_DIR
from equipment_rental.exception.exception import SilverTransformationException

logger = get_logger()


class SilverTransformation:
    """
    Silver Transformation Layer

    Responsibilities:
    - Apply final business transformations
    - Compute derived metrics
    - Save clean datasets
    - Save utilisation dataset
    - Trigger quarantine handling
    - Ensure metadata consistency
    """

    def __init__(self):
        self.quarantine_handler = QuarantineHandler()

    # ============================================================
    # MAIN TRANSFORM METHOD
    # ============================================================
    def transform(self, validated_tables: dict,
                  table_name: str,
                  pipeline_run_id: str = None) -> dict:

        table_name = table_name.lower()

        try:

            if table_name == "rental_transactions":
                return self._transform_rental_transactions(
                    validated_tables,
                    table_name,
                    pipeline_run_id
                )
            else:
                return self._transform_master_table(
                    validated_tables,
                    table_name,
                    pipeline_run_id
                )

        except Exception as e:
            logger.error(
                f"Silver transformation failed | table={table_name} | error={str(e)}"
            )
            raise SilverTransformationException(
                f"Silver transformation failed for table: {table_name}",
                errors=str(e)
            ) from e

    # ============================================================
    # RENTAL TRANSACTIONS TRANSFORMATION
    # ============================================================
    def _transform_rental_transactions(self,
                                       validated_tables: dict,
                                       table_name: str,
                                       pipeline_run_id: str):

        try:
            df_all = validated_tables.get("all")
            if df_all is None or df_all.empty:
                raise SilverTransformationException(f"No data to transform for {table_name}")

            df_all = df_all.copy()

            # --------------------------------------------------------
            # Ensure RentalDays consistency
            # --------------------------------------------------------
            df_all["ComputedRentalDays"] = (
                (df_all["EndDate"].fillna(pd.Timestamp.today()) - df_all["StartDate"]).dt.days + 1
            ).clip(lower=1)

            if "RentalDays" in df_all.columns:
                df_all["RentalDays"] = df_all["ComputedRentalDays"]

            # --------------------------------------------------------
            # Revenue Calculation
            # --------------------------------------------------------
            if "DailyRate" in df_all.columns:
                df_all["ExpectedRevenue"] = df_all["RentalDays"] * df_all["DailyRate"]

            if "ActualRevenue" in df_all.columns:
                df_all["TotalRevenue"] = df_all["ActualRevenue"]
            else:
                df_all["TotalRevenue"] = df_all.get("ExpectedRevenue", 0)

            if "ExpectedRevenue" in df_all.columns and "ActualRevenue" in df_all.columns:
                df_all["RevenueDifference"] = df_all["ActualRevenue"] - df_all["ExpectedRevenue"]

            # --------------------------------------------------------
            # Metadata Enrichment
            # --------------------------------------------------------
            df_all["pipeline_run_id"] = pipeline_run_id
            df_all["load_timestamp"] = datetime.now()

            # ========================================================
            # Save Clean Status-Based Outputs
            # ========================================================
            outputs = {}
            for status in ["active", "completed", "cancelled"]:
                temp_df = validated_tables.get(status)
                if temp_df is not None and not temp_df.empty:
                    temp_df = df_all.loc[temp_df.index]
                    save_csv(f"{SILVER_DIR}/{table_name}_{status}.csv", temp_df)
                    outputs[status] = temp_df

            save_csv(f"{SILVER_DIR}/{table_name}_all.csv", df_all)
            outputs["all"] = df_all

            # ========================================================
            # Save Equipment Utilisation
            # ========================================================
            utilisation_df = validated_tables.get("equipment_utilisation")
            if utilisation_df is not None and not utilisation_df.empty:
                utilisation_df = utilisation_df.copy()
                utilisation_df["pipeline_run_id"] = pipeline_run_id
                utilisation_df["load_timestamp"] = datetime.now()
                save_csv(f"{SILVER_DIR}/equipment_utilisation.csv", utilisation_df)
                outputs["equipment_utilisation"] = utilisation_df

            # ========================================================
            # Handle Quarantine
            # ========================================================
            quarantine_df = validated_tables.get("quarantine")
            if quarantine_df is not None and not quarantine_df.empty:
                self.quarantine_handler.save_quarantine(
                    df=quarantine_df,
                    table_name=table_name,
                    pipeline_run_id=pipeline_run_id
                )
                logger.warning(f"{len(quarantine_df)} rows quarantined | table: {table_name}")
                outputs["quarantine"] = quarantine_df

            logger.info(f"Silver transformation completed for {table_name} | Rows: {len(df_all)}")
            return outputs

        except Exception as e:
            logger.error(f"Rental transactions transformation failed | error={str(e)}")
            raise SilverTransformationException(
                f"Transformation failed for rental_transactions",
                errors=str(e)
            ) from e

    # ============================================================
    # MASTER TABLE TRANSFORMATION
    # ============================================================
    def _transform_master_table(self,
                                validated_tables: dict,
                                table_name: str,
                                pipeline_run_id: str):

        try:
            df_clean = validated_tables.get("clean")
            if df_clean is None or df_clean.empty:
                df_clean = validated_tables.get("all")

            if df_clean is None or df_clean.empty:
                raise SilverTransformationException(f"No data to transform for {table_name}")

            df_clean = df_clean.copy()
            df_clean["pipeline_run_id"] = pipeline_run_id
            df_clean["load_timestamp"] = datetime.now()
            save_csv(f"{SILVER_DIR}/{table_name}_clean.csv", df_clean)

            logger.info(f"Master table transformation completed for {table_name} | Rows: {len(df_clean)}")
            return {"all": df_clean}

        except Exception as e:
            logger.error(f"Master table transformation failed | table={table_name} | error={str(e)}")
            raise SilverTransformationException(
                f"Transformation failed for master table: {table_name}",
                errors=str(e)
            ) from e