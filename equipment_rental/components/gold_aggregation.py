from datetime import datetime
import pandas as pd
import os
from equipment_rental.constants.constants import GOLD_DIR
from equipment_rental.utils.common_utils import save_csv
from equipment_rental.logger.logger import get_logger

logger = get_logger()

class GoldAggregation:
    """
    Aggregates rental transactions to produce high-level metrics:
    - Equipment utilization %
    - Revenue by equipment, customer, and month
    """

    def aggregate(self, rental_df: pd.DataFrame, customer_df: pd.DataFrame, equipment_df: pd.DataFrame, pipeline_run_id: str = None):
        try:
            # ---------------- Safety Checks ----------------
            if rental_df is None:
                raise ValueError("rental_df cannot be None")

            if customer_df is None:
                logger.warning("customer_df is None; creating empty DataFrame")
                customer_df = pd.DataFrame()

            if equipment_df is None:
                logger.warning("equipment_df is None; creating empty DataFrame")
                equipment_df = pd.DataFrame()

            df = rental_df.copy()

            # Only include non-quarantined rentals
            if "quarantined" in df.columns:
                df = df[df["quarantined"] == 0].copy()
            else:
                logger.warning("'quarantined' column not found; using all rows")

            # Ensure essential numeric columns
            for col in ["DailyRate", "RentalDays"]:
                if col not in df.columns:
                    df[col] = 0
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # Compute Revenue
            df["Revenue"] = df["DailyRate"] * df["RentalDays"]
            df["load_timestamp"] = datetime.now()
            df["pipeline_run_id"] = pipeline_run_id

            # ----------------- Equipment-level aggregation -----------------
            if not df.empty and "EquipmentID" in df.columns:
                equipment_list = []
                for equip_id, group in df.groupby("EquipmentID"):
                    start = pd.to_datetime(group["StartDate"]).min()
                    end = pd.to_datetime(group["EndDate"]).max() if group["EndDate"].notna().any() else pd.Timestamp.today()
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
            if not df.empty and "CustomerID" in df.columns:
                # Optional: join with customer_df to enrich info
                if not customer_df.empty:
                    df = df.merge(
                        customer_df[["CustomerID", "CustomerName", "CustomerType", "AccountManager"]],
                        on="CustomerID",
                        how="left"
                    )

                customer_agg = df.groupby("CustomerID").agg(
                    total_rentals=pd.NamedAgg(column="TransactionID", aggfunc="count"),
                    total_revenue=pd.NamedAgg(column="Revenue", aggfunc="sum")
                ).reset_index()
                customer_agg["pipeline_run_id"] = pipeline_run_id
                customer_agg["load_timestamp"] = datetime.now()
                save_csv(customer_agg, os.path.join(GOLD_DIR, "customer_aggregation.csv"))

            # ----------------- Monthly revenue aggregation -----------------
            if not df.empty and "StartDate" in df.columns:
                df["RentalMonth"] = pd.to_datetime(df["StartDate"]).dt.to_period("M")
                monthly_agg = df.groupby("RentalMonth").agg(
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