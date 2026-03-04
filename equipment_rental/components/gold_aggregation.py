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
    Aggregates Silver validated tables into gold-level metrics:
    - Equipment utilization %
    - Revenue by equipment, customer, and month
    Only uses non-quarantined transactions.
    """

    def aggregate(self,
                  rental_df: pd.DataFrame,
                  customer_df: pd.DataFrame,
                  equipment_df: pd.DataFrame,
                  pipeline_run_id: str = None):
        """
        rental_df: rental_transaction_all
        customer_df: customer_master_clean
        equipment_df: equipment_master_clean
        """

        try:
            # ----------------- Filter only valid transactions -----------------
            if "quarantined" in rental_df.columns:
                rental_df = rental_df[rental_df["quarantined"] == 0].copy()
            else:
                logger.warning("No 'quarantined' column found in rental_df, using all rows.")

            if rental_df.empty:
                logger.warning("No valid rental transactions for aggregation.")
                return

            # ----------------- Ensure numeric columns -----------------
            for col in ["DailyRate", "RentalDays"]:
                if col not in rental_df.columns:
                    rental_df[col] = 0
            rental_df["DailyRate"] = pd.to_numeric(rental_df["DailyRate"], errors="coerce").fillna(0)
            rental_df["RentalDays"] = pd.to_numeric(rental_df["RentalDays"], errors="coerce").fillna(0)

            rental_df["Revenue"] = rental_df["DailyRate"] * rental_df["RentalDays"]

            # ----------------- Add metadata -----------------
            rental_df["load_timestamp"] = datetime.now()
            rental_df["pipeline_run_id"] = pipeline_run_id
            customer_df["pipeline_run_id"] = pipeline_run_id
            equipment_df["pipeline_run_id"] = pipeline_run_id

            # ----------------- Equipment-level aggregation -----------------
            equipment_agg = (
                rental_df.groupby("EquipmentID")
                .agg(
                    total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                    total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
                    avg_rental_days=pd.NamedAgg(column="RentalDays", aggfunc="mean"),
                    total_rental_days=pd.NamedAgg(column="RentalDays", aggfunc="sum"),
                )
                .reset_index()
            )

            # Merge with equipment master to include name/category/etc
            equipment_agg = equipment_agg.merge(
                equipment_df[["EquipmentID", "EquipmentName", "Category"]],
                on="EquipmentID",
                how="left"
            )

            # Compute utilization %
            utilization_list = []
            for idx, row in equipment_agg.iterrows():
                equip_rentals = rental_df[rental_df["EquipmentID"] == row["EquipmentID"]]
                start = equip_rentals["StartDate"].min()
                end = equip_rentals["EndDate"].max() if equip_rentals["EndDate"].notna().any() else pd.Timestamp.today()
                total_days_available = max((end - start).days + 1, 1)
                utilization_pct = round(row["total_rental_days"] / total_days_available * 100, 2)
                equipment_agg.at[idx, "utilization_pct"] = utilization_pct

            equipment_agg["avg_rental_days"] = equipment_agg["avg_rental_days"].round(2)
            equipment_agg["load_timestamp"] = datetime.now()
            save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # ----------------- Customer-level aggregation -----------------
            customer_agg = (
                rental_df.groupby("CustomerID")
                .agg(
                    total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                    total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
                )
                .reset_index()
            )

            # Merge with customer master to include name/type/etc
            customer_agg = customer_agg.merge(
                customer_df[["CustomerID", "CustomerName", "CustomerType", "AccountManager"]],
                on="CustomerID",
                how="left"
            )

            customer_agg["load_timestamp"] = datetime.now()
            save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # ----------------- Monthly revenue aggregation -----------------
            rental_df["RentalMonth"] = rental_df["StartDate"].dt.to_period("M")
            monthly_agg = rental_df.groupby("RentalMonth").agg(
                total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum"),
            ).reset_index()

            monthly_agg["load_timestamp"] = datetime.now()
            monthly_agg["pipeline_run_id"] = pipeline_run_id
            save_csv(monthly_agg, os.path.join(GOLD_DIR, "monthly_aggregation.csv"))

            logger.info(f"Gold aggregation completed successfully | pipeline_run_id={pipeline_run_id}")

        except Exception as e:
            logger.error(f"Gold aggregation failed: {str(e)}")
            raise