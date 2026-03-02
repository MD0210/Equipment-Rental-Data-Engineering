import sys
import argparse
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def run_pipeline(tables, source_type, source_file=None, db_file=None, batch_type="full", schedule=None):
    pipeline = MedallionPipeline()

    for table_name in tables:
        logger.info(f"Running pipeline for table/sheet: {table_name} | schedule: {schedule}")
        db_query = None
        if source_type == "db" and db_file:
            db_query = {
                "connection_str": f"sqlite:///{db_file}",
                "query": f"SELECT * FROM {table_name}",
                "table_name": table_name
            }

        pipeline.run(
            source_name=f"{source_type}_rental_source",
            source_type=source_type,
            table_name=table_name,
            file_path=source_file,
            db_query=db_query,
            batch_type=batch_type,
            schedule=schedule
        )


def main():
    parser = argparse.ArgumentParser(description="Run Medallion Pipeline for Equipment Rental")
    parser.add_argument("--source", required=True, choices=["excel", "db"], help="Data source type")
    parser.add_argument("--tables", required=True, help="Comma-separated list of tables/sheets to process")
    parser.add_argument("--file", help="Excel file path (if source=excel)")
    parser.add_argument("--db_file", help="Database file path (if source=db)")
    parser.add_argument("--batch-type", default="full", choices=["full", "incremental"], help="Batch type")
    parser.add_argument("--schedule", type=str, help="Optional schedule identifier")

    args = parser.parse_args()
    tables = [t.strip() for t in args.tables.split(",")]

    try:
        run_pipeline(
            tables=tables,
            source_type=args.source,
            source_file=args.file,
            db_file=args.db_file,
            batch_type=args.batch_type,
            schedule=args.schedule
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()