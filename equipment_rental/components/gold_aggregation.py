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
    Aggregates rental transactions and master data to produce:
    - Equipment utilization %
    - Customer revenue & rental metrics
    - Monthly revenue
    """

    def aggregate(self,
                  rental_df: pd.DataFrame,
                  equipment_df: pd.DataFrame,
                  customer_df: pd.DataFrame,
                  pipeline_run_id: str = None):
        """
        rental_df: rental_transaction_all.csv
        equipment_df: equipment_master_clean.csv
        customer_df: customer_master_clean.csv
        """

        try:
            if rental_df.empty:
                logger.warning("Rental transaction dataframe is empty. Nothing to aggregate.")
                return

            # ---------------- Filter non-quarantined rentals ----------------
            rental_df = rental_df[rental_df["quarantined"] == 0].copy()
            if rental_df.empty:
                logger.warning("No non-quarantined rental transactions to aggregate.")
                return

            # Ensure numeric types
            rental_df["DailyRate"] = pd.to_numeric(rental_df["DailyRate"], errors="coerce").fillna(0)
            rental_df["RentalDays"] = pd.to_numeric(rental_df["RentalDays"], errors="coerce").fillna(0)

            # Compute revenue
            rental_df["Revenue"] = rental_df["DailyRate"] * rental_df["RentalDays"]

            # Add metadata
            rental_df["load_timestamp"] = datetime.now()
            rental_df["pipeline_run_id"] = pipeline_run_id

            # ---------------- Equipment Aggregation ----------------
            # Join rental_df with equipment_df to enrich metadata
            equipment_agg_df = rental_df.merge(
                equipment_df, on="EquipmentID", how="left", suffixes=("", "_equip")
            )

            equipment_list = []
            for equip_id, group in equipment_agg_df.groupby("EquipmentID"):
                if group.empty:
                    continue
                start = group["StartDate"].min()
                end = group["EndDate"].max() if group["EndDate"].notna().any() else pd.Timestamp.today()
                total_days_available = max((end - start).days + 1, 1)
                total_rental_days = group["RentalDays"].sum()
                utilization_pct = round(total_rental_days / total_days_available * 100, 2)
                equipment_list.append({
                    "EquipmentID": equip_id,
                    "EquipmentName": group["EquipmentName"].iloc[0] if "EquipmentName" in group else None,
                    "Category": group["Category"].iloc[0] if "Category" in group else None,
                    "total_rentals": group["TransactionID"].count(),
                    "total_revenue": group["Revenue"].sum(),
                    "avg_rental_days": round(group["RentalDays"].mean(), 2),
                    "utilization_pct": utilization_pct,
                    "pipeline_run_id": pipeline_run_id,
                    "load_timestamp": datetime.now()
                })

            equipment_agg = pd.DataFrame(equipment_list)
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # ---------------- Customer Aggregation ----------------
            customer_agg_df = rental_df.merge(
                customer_df, on="CustomerID", how="left", suffixes=("", "_cust")
            )

            customer_agg = customer_agg_df.groupby("CustomerID").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
            ).reset_index()

            # Add customer metadata
            customer_agg = customer_agg.merge(
                customer_df, on="CustomerID", how="left"
            )

            customer_agg["pipeline_run_id"] = pipeline_run_id
            customer_agg["load_timestamp"] = datetime.now()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # ---------------- Monthly Aggregation ----------------
            rental_df["RentalMonth"] = rental_df["StartDate"].dt.to_period("M")
            monthly_agg = rental_df.groupby("RentalMonth").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
            ).reset_index()

            monthly_agg["pipeline_run_id"] = pipeline_run_id
            monthly_agg["load_timestamp"] = datetime.now()
            save_csv(monthly_agg, os.path.join(GOLD_DIR, "monthly_aggregation.csv"))

            logger.info(f"Gold aggregation completed successfully | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Gold aggregation failed: {str(e)}")
            raise