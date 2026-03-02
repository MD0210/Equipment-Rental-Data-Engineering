# main.py

import sys
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def run_excel_pipeline():
    """
    Run Medallion Pipeline using Excel as source
    """
    pipeline = MedallionPipeline()

    pipeline.run(
        source_name="excel_rental_source",
        file_path="data/Equipment_Hire_Dataset.xlsx",
        sheet_name="Rental_Transaction"
    )


def run_sqlite_pipeline():
    """
    Run Medallion Pipeline using SQLite as source
    """
    pipeline = MedallionPipeline()

    db_query = {
        "connection_str": "sqlite:///data/equipment_rental.db",
        "query": "SELECT * FROM Rental_Transaction",
        "table_name": "Rental_Transaction"
    }

    pipeline.run(
        source_name="sqlite_rental_source",
        file_path=None,
        db_query=db_query
    )


def main():
    """
    Entry point of application.
    Pass argument:
        python main.py excel
        python main.py db
    """

    if len(sys.argv) < 2:
        print("Usage: python main.py [excel|db]")
        sys.exit(1)

    source_type = sys.argv[1].lower()

    try:
        if source_type == "excel":
            logger.info("Starting Excel pipeline...")
            run_excel_pipeline()

        elif source_type == "db":
            logger.info("Starting SQLite pipeline...")
            run_sqlite_pipeline()

        else:
            print("Invalid source type. Use 'excel' or 'db'.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()