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
    Aggregates rental transactions to produce high-level business metrics:
    - Equipment utilization %
    - Revenue by equipment, customer, and month
    """

    def aggregate(self, df: pd.DataFrame, pipeline_run_id: str = None):
        """
        Performs aggregations on Silver rental_transactions DataFrame.
        Ignores quarantined rows. Saves CSV outputs to GOLD_DIR.
        """
        try:
            if df is None or df.empty:
                logger.warning("No input data for Gold aggregation")
                return

            df = df.copy()

            # ----------------- Filter out quarantined rows -----------------
            if "quarantined" in df.columns:
                df = df[df["quarantined"] == 0]

            if df.empty:
                logger.warning("No valid rows after filtering quarantined records")
                return

            # ----------------- Ensure essential columns -----------------
            for col in ["DailyRate", "RentalDays", "StartDate", "EndDate", "TransactionID", "EquipmentID", "CustomerID"]:
                if col not in df.columns:
                    df[col] = 0 if col in ["DailyRate", "RentalDays"] else pd.NaT if "Date" in col else None

            # ----------------- Convert numeric types -----------------
            df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce").fillna(0)
            df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce").fillna(0)

            # ----------------- Compute Revenue -----------------
            df["Revenue"] = df["DailyRate"] * df["RentalDays"]

            # ----------------- Add metadata -----------------
            df["load_timestamp"] = datetime.now()
            df["pipeline_run_id"] = pipeline_run_id
            df["source_file"] = df.get("source_file", "silver_validation")

            # ----------------- Equipment-level aggregation -----------------
            equipment_list = []
            for equip_id, group in df.groupby("EquipmentID"):
                start = group["StartDate"].min()
                end = group["EndDate"].max() if group["EndDate"].notna().any() else pd.Timestamp.today()

                total_days_available = (end - start).days + 1
                total_rental_days = group["RentalDays"].sum()
                utilization_pct = (total_rental_days / total_days_available * 100) if total_days_available > 0 else 0

                equipment_list.append({
                    "EquipmentID": equip_id,
                    "total_rentals": group["TransactionID"].count(),
                    "total_revenue": group["Revenue"].sum(),
                    "avg_rental_days": round(group["RentalDays"].mean(), 2),
                    "utilization_pct": round(utilization_pct, 2),
                    "pipeline_run_id": pipeline_run_id,
                    "load_timestamp": datetime.now()
                })

            equipment_agg = pd.DataFrame(equipment_list)
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # ----------------- Customer-level aggregation -----------------
            customer_agg = df.groupby("CustomerID").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()
            customer_agg["pipeline_run_id"] = pipeline_run_id
            customer_agg["load_timestamp"] = datetime.now()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # ----------------- Monthly revenue aggregation -----------------
            df["RentalMonth"] = df["StartDate"].dt.to_period("M")
            monthly_agg = df.groupby("RentalMonth").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()
            monthly_agg["pipeline_run_id"] = pipeline_run_id
            monthly_agg["load_timestamp"] = datetime.now()
            save_csv(monthly_agg, os.path.join(GOLD_DIR, "monthly_aggregation.csv"))

            logger.info(f"Gold aggregation completed successfully | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Gold aggregation failed: {str(e)}")
            raise