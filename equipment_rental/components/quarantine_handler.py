# equipment_rental/components/quarantine_handler.py
import os
import pandas as pd
from equipment_rental.constants.constants import QUARANTINE_DIR
from equipment_rental.logger.logger import get_logger
from datetime import datetime

logger = get_logger()

class QuarantineHandler:
    def __init__(self):
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

    def save_quarantine(self, df: pd.DataFrame, table_name: str, pipeline_run_id: str = None):
        """
        Save the quarantine dataframe to CSV for review.
        Adds metadata: pipeline_run_id and timestamp.
        """
        try:
            df = df.copy()
            current_ts = datetime.now()
            df["pipeline_run_id"] = pipeline_run_id
            df["quarantine_ts"] = current_ts

            file_name = f"{table_name}_quarantine_{current_ts.strftime('%Y%m%d%H%M%S')}.csv"
            file_path = os.path.join(QUARANTINE_DIR, file_name)

            df.to_csv(file_path, index=False)
            logger.warning(f"Quarantine data saved | table: {table_name} | rows: {len(df)} | path: {file_path}")

        except Exception as e:
            logger.error(f"Failed to save quarantine for {table_name}: {str(e)}")
            raise