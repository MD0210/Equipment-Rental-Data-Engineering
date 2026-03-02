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

    def aggregate(self, df: pd.DataFrame):
        """
        Performs aggregations on validated rental transactions.
        Saves aggregated results to GOLD_DIR as CSV files.
        """
        try:
            df = df.copy()

            # Ensure essential columns
            for col in ["DailyRate", "RentalDays", "StartDate", "EndDate", "TransactionID", "EquipmentID", "CustomerID"]:
                if col not in df.columns:
                    df[col] = 0 if col in ["DailyRate", "RentalDays"] else pd.NaT if "Date" in col else None

            # Ensure numeric types
            df["DailyRate"] = pd.to_numeric(df["DailyRate"], errors="coerce").fillna(0)
            df["RentalDays"] = pd.to_numeric(df["RentalDays"], errors="coerce").fillna(0)

            # Compute revenue
            df["Revenue"] = df["DailyRate"] * df["RentalDays"]

            # Add metadata
            df["load_timestamp"] = datetime.now()
            df["source_file"] = df.get("source_file", "silver_validation")

            # Equipment-level aggregation
            utilization_list = []
            for equip_id, group in df.groupby("EquipmentID"):
                start = group["StartDate"].min()
                end = group["EndDate"].max() if group["EndDate"].notna().any() else pd.Timestamp.today()
                total_days_available = (end - start).days + 1
                total_rental_days = group["RentalDays"].sum()
                utilization_pct = (total_rental_days / total_days_available * 100) if total_days_available > 0 else 0
                utilization_list.append({
                    "EquipmentID": equip_id,
                    "total_rentals": group["TransactionID"].count(),
                    "total_revenue": group["Revenue"].sum(),
                    "avg_rental_days": round(group["RentalDays"].mean(), 2),
                    "utilization_pct": round(utilization_pct, 2)
                })

            equipment_agg = pd.DataFrame(utilization_list)
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # Customer-level aggregation
            customer_agg = df.groupby("CustomerID").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # Monthly revenue aggregation
            df["RentalMonth"] = df["StartDate"].dt.to_period("M")
            monthly_agg = df.groupby("RentalMonth").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()
            save_csv(monthly_agg, os.path.join(GOLD_DIR, "monthly_aggregation.csv"))

            logger.info("Gold aggregation with utilization completed successfully")

        except Exception as e:
            logger.error(f"Gold aggregation failed: {str(e)}")
            raise