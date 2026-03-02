import os
from datetime import datetime
import pandas as pd
from equipment_rental.logger.logger import get_logger
from equipment_rental.constants.constants import QUARANTINE_DIR

logger = get_logger()

class QuarantineHandler:
    """
    Handles quarantined rows from Silver validation.
    Saves them to CSV for auditing or manual review.
    """

    def __init__(self):
        # Ensure quarantine directory exists
        os.makedirs(QUARANTINE_DIR, exist_ok=True)

    def create_quarantine(self, df: pd.DataFrame, source_file: str = None):
        """
        Save quarantined DataFrame to CSV
        """
        try:
            if df.empty:
                logger.info("No quarantined rows to handle.")
                return

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{source_file}_quarantine_{timestamp}.csv" if source_file else f"quarantine_{timestamp}.csv"
            file_path = os.path.join(QUARANTINE_DIR, filename)

            df.to_csv(file_path, index=False)
            logger.info(f"Quarantined rows saved: {len(df)} rows | Path: {file_path}")

        except Exception as e:
            logger.error(f"Failed to save quarantined rows: {str(e)}")
            raise