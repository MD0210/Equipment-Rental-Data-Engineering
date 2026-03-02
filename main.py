from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.constants.constants import ARTIFACT_DIR
from equipment_rental.logger.logger import get_logger

logger = get_logger()

def main():
    """
    Main entry point for the Equipment Rental Medallion Pipeline.
    Supports ingestion from Excel sheets and database queries.
    """

    try:
        pipeline = MedallionPipeline()

        # Example 1: Excel ingestion
        excel_file = "data/Equipment_Hire_Dataset.xlsx"
        sheets = ["Equipment_master", "Customer_Master", "Rental_Transactions", "Date_Dimension"]
        for sheet in sheets:
            pipeline.run(source_type="excel", source=excel_file, sheet_name=sheet)

        # Example 2: Database ingestion (SQLite / other DB)
        db_connection = "sqlite:///artifacts/equipment_rental.db"
        queries = {
            "Equipment_master": "SELECT * FROM Equipment_master",
            "Customer_Master": "SELECT * FROM Customer_Master",
            "Rental_Transactions": "SELECT * FROM Rental_Transactions",
            "Date_Dimension": "SELECT * FROM Date_Dimension"
        }
        for table_name, query in queries.items():
            pipeline.run(source_type="db", source=db_connection, table_name=table_name, query=query)

        logger.info("Medallion pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")

if __name__ == "__main__":
    main()