# equipment_rental/components/gold_aggregation.py
from datetime import datetime
import pandas as pd
import os
from equipment_rental.constants.constants import GOLD_DIR
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.logger.logger import get_logger
from equipment_rental.exception.exception import GoldAggregationException

logger = get_logger()


class GoldAggregation:

    def aggregate(self,
                  rental_df: pd.DataFrame,
                  equipment_df: pd.DataFrame,
                  customer_df: pd.DataFrame,
                  pipeline_run_id: str = None):

        try:
            if rental_df is None or rental_df.empty:
                logger.warning("Rental transaction dataframe is empty. Nothing to aggregate.")
                return

            rental_df = rental_df.copy()

            # ---------------- Ensure columns exist ----------------
            for col in ["DailyRate", "RentalDays", "quarantined"]:
                if col not in rental_df.columns:
                    rental_df[col] = 0

            rental_df["DailyRate"] = pd.to_numeric(rental_df["DailyRate"], errors="coerce").fillna(0)
            rental_df["RentalDays"] = pd.to_numeric(rental_df["RentalDays"], errors="coerce").fillna(0)

            # Compute revenue
            rental_df["Revenue"] = rental_df["DailyRate"] * rental_df["RentalDays"]

            # Add metadata
            rental_df["load_timestamp"] = datetime.now()
            rental_df["pipeline_run_id"] = pipeline_run_id

            # ---------------- Filter non-quarantined rentals ----------------
            rental_df = rental_df[rental_df["quarantined"] == 0]
            if rental_df.empty:
                logger.warning("No non-quarantined rental transactions to aggregate.")
                return

            # ---------------- Equipment Aggregation ----------------
            if equipment_df is not None and not equipment_df.empty:
                equipment_df = equipment_df.copy()
                equipment_df["load_timestamp"] = datetime.now()
                equipment_df["pipeline_run_id"] = pipeline_run_id

                equip_agg_list = []
                merged_eq = rental_df.merge(equipment_df, on="EquipmentID", how="left")
                for equip_id, group in merged_eq.groupby("EquipmentID"):
                    start = pd.to_datetime(group["StartDate"]).min()
                    end = pd.to_datetime(group["EndDate"]).max() if group["EndDate"].notna().any() else pd.Timestamp.today()
                    total_days_available = max((end - start).days + 1, 1)
                    total_rental_days = group["RentalDays"].sum()
                    utilization_pct = round(total_rental_days / total_days_available * 100, 2)
                    equip_agg_list.append({
                        "EquipmentID": equip_id,
                        "EquipmentName": group.get("EquipmentName", pd.Series([None])).iloc[0],
                        "Category": group.get("Category", pd.Series([None])).iloc[0],
                        "total_rentals": group["TransactionID"].count(),
                        "total_revenue": group["Revenue"].sum(),
                        "avg_rental_days": round(group["RentalDays"].mean(), 2),
                        "utilization_pct": utilization_pct,
                        "pipeline_run_id": pipeline_run_id,
                        "load_timestamp": datetime.now()
                    })

                equipment_agg = pd.DataFrame(equip_agg_list)
                save_csv(equipment_agg, os.path.join(GOLD_DIR, "equipment_aggregation.csv"))

            # ---------------- Customer Aggregation ----------------
            if customer_df is not None and not customer_df.empty:
                customer_df = customer_df.copy()
                customer_df["load_timestamp"] = datetime.now()
                customer_df["pipeline_run_id"] = pipeline_run_id

                cust_agg = rental_df.groupby("CustomerID").agg(
                    total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                    total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
                ).reset_index()

                cust_agg = cust_agg.merge(customer_df, on="CustomerID", how="left")
                save_csv(cust_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # ---------------- Monthly Aggregation ----------------
            rental_df["RentalMonth"] = pd.to_datetime(rental_df["StartDate"]).dt.to_period("M")
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
            raise GoldAggregationException(
                "Gold aggregation failed",
                errors=str(e)
            ) from e