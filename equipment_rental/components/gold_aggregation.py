# equipment_rental/components/gold_aggregation.py
from datetime import datetime
import pandas as pd
from equipment_rental.constants.constants import GOLD_DIR
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.logger.logger import get_logger
import os

logger = get_logger()


class GoldAggregation:
    """
    Aggregates rental transactions to produce high-level metrics:
    - Equipment utilization %
    - Revenue by equipment, customer, and month
    """

    def aggregate(self, df: pd.DataFrame, pipeline_run_id: str = None):
        """
        Performs aggregations on validated rental transactions.
        Saves aggregated results to GOLD_DIR as CSV files.
        Includes pipeline_run_id in output.
        """
        try:
            if df is None or df.empty:
                logger.warning("Input DataFrame is empty. Aborting Gold aggregation.")
                return

            df = df.copy()

            # -----------------------
            # Ensure essential columns
            # -----------------------
            for col in ["TransactionID", "EquipmentID", "CustomerID", "StartDate", "EndDate", "RentalDays", "DailyRate"]:
                if col not in df.columns:
                    if col in ["RentalDays", "DailyRate"]:
                        df[col] = 0
                    else:
                        df[col] = pd.NaT if "Date" in col else None

            # Ensure correct data types
            df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce").fillna(0)
            df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce").fillna(0)
            df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
            df["EndDate"] = pd.to_datetime(df["EndDate"], errors="coerce")

            # Compute revenue
            df["Revenue"] = df["RentalDays"] * df["DailyRate"]

            # Add metadata
            df["load_timestamp"] = datetime.now()
            df["pipeline_run_id"] = pipeline_run_id
            df["source_file"] = df.get("source_file", "silver_validation")

            # ----------------- Equipment-level aggregation -----------------
            equipment_agg_list = []
            for equip_id, group in df.groupby("EquipmentID"):
                if group.empty:
                    continue

                # Dates for utilization calculation
                start_date = group["StartDate"].min()
                end_date = group["EndDate"].max() if group["EndDate"].notna().any() else pd.Timestamp.today()

                total_days_available = (end_date - start_date).days + 1 if pd.notna(start_date) else 0
                total_rental_days = group["RentalDays"].sum()
                utilization_pct = (total_rental_days / total_days_available * 100) if total_days_available > 0 else 0

                equipment_agg_list.append({
                    "EquipmentID": equip_id,
                    "total_rentals": group["TransactionID"].count(),
                    "total_revenue": group["Revenue"].sum(),
                    "avg_rental_days": round(group["RentalDays"].mean(), 2),
                    "utilization_pct": round(utilization_pct, 2),
                    "pipeline_run_id": pipeline_run_id,
                    "load_timestamp": datetime.now()
                })

            equipment_agg = pd.DataFrame(equipment_agg_list)
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))
            logger.info(f"Equipment aggregation rows: {len(equipment_agg)}")

            # ----------------- Customer-level aggregation -----------------
            customer_agg = df.groupby("CustomerID").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
            ).reset_index()
            customer_agg["pipeline_run_id"] = pipeline_run_id
            customer_agg["load_timestamp"] = datetime.now()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))
            logger.info(f"Customer aggregation rows: {len(customer_agg)}")

            # ----------------- Monthly revenue aggregation -----------------
            df["RentalMonth"] = df["StartDate"].dt.to_period("M")
            monthly_agg = df.groupby("RentalMonth").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
            ).reset_index()
            monthly_agg["pipeline_run_id"] = pipeline_run_id
            monthly_agg["load_timestamp"] = datetime.now()
            save_csv(monthly_agg, os.path.join(GOLD_DIR, "monthly_aggregation.csv"))
            logger.info(f"Monthly aggregation rows: {len(monthly_agg)}")

            logger.info(f"Gold aggregation completed successfully | pipeline_run_id={pipeline_run_id}")

            return {
                "equipment_aggregation": equipment_agg,
                "customer_aggregation": customer_agg,
                "monthly_aggregation": monthly_agg
            }

        except Exception as e:
            logger.error(f"Gold aggregation failed: {str(e)}")
            raise