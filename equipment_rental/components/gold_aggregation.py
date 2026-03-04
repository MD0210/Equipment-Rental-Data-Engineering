# equipment_rental/components/gold_aggregation.py
from datetime import datetime
import pandas as pd
import os
from equipment_rental.constants.constants import GOLD_DIR
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.logger.logger import get_logger

logger = get_logger()


class GoldAggregation:
    """
    Aggregates Silver validated rental transactions to produce high-level metrics:
    - Equipment utilization %
    - Revenue by equipment, customer, and month
    """

    def aggregate(self, df: pd.DataFrame, pipeline_run_id: str = None):
        try:
            if df.empty:
                logger.warning("Input DataFrame is empty. Nothing to aggregate.")
                return

            # Only consider non-quarantined rows if exists
            if "quarantined" in df.columns:
                df = df[df["quarantined"] == 0].copy()
            else:
                df = df.copy()

            if df.empty:
                logger.warning("No rows available for aggregation after quarantine filter.")
                return

            # Ensure essential columns
            for col in ["DailyRate", "RentalDays", "StartDate", "EndDate",
                        "TransactionID", "EquipmentID", "CustomerID"]:
                if col not in df.columns:
                    df[col] = 0 if col in ["DailyRate", "RentalDays"] else pd.NaT if "Date" in col else None

            # Ensure numeric types
            df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce").fillna(0)
            df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce").fillna(0)

            # Compute revenue
            df["Revenue"] = df["DailyRate"] * df["RentalDays"]

            # Add metadata
            df["load_timestamp"] = datetime.now()
            df["pipeline_run_id"] = pipeline_run_id
            df["source_file"] = df.get("source_file", "silver_validation")

            # Equipment-level aggregation
            equipment_list = []
            for equip_id, group in df.groupby("EquipmentID"):
                if group.empty:
                    continue
                start = group["StartDate"].min()
                end = group["EndDate"].max() if group["EndDate"].notna().any() else pd.Timestamp.today()
                total_days_available = max((end - start).days + 1, 1)
                total_rental_days = group["RentalDays"].sum()
                utilization_pct = round(total_rental_days / total_days_available * 100, 2)
                equipment_list.append({
                    "EquipmentID": equip_id,
                    "total_rentals": group["TransactionID"].count(),
                    "total_revenue": group["Revenue"].sum(),
                    "avg_rental_days": round(group["RentalDays"].mean(), 2),
                    "utilization_pct": utilization_pct,
                    "pipeline_run_id": pipeline_run_id,
                    "load_timestamp": datetime.now()
                })

            equipment_agg = pd.DataFrame(equipment_list)
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # Customer-level aggregation
            customer_agg = df.groupby("CustomerID").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()
            customer_agg["pipeline_run_id"] = pipeline_run_id
            customer_agg["load_timestamp"] = datetime.now()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # Monthly aggregation
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