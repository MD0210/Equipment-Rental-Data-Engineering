# main.py
import sys
import argparse
from equipment_rental.pipeline.medallion_pipeline import MedallionPipeline
from equipment_rental.logger.logger import get_logger

logger = get_logger()


def run_pipeline(source_type, tables, file_path=None, db_file=None, batch_type="full", rerun_id=None):
    """
    Run Medallion Pipeline for given tables/sheets.
    """
    pipeline = MedallionPipeline()

    for table_name in tables:
        logger.info(f"Running pipeline for table/sheet: {table_name}")
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
            file_path=file_path,
            db_query=db_query,
            batch_type=batch_type,
            rerun_id=rerun_id
        )


def main():
    parser = argparse.ArgumentParser(description="Run Medallion Pipeline")
    parser.add_argument("--source", type=str, required=True, choices=["excel", "db"], help="Data source: excel or db")
    parser.add_argument("--tables", type=str, required=True, help="Comma-separated list of tables/sheets to process")
    parser.add_argument("--batch-type", type=str, default="full", choices=["full", "incremental"], help="Batch type")
    parser.add_argument("--rerun-id", type=int, default=None, help="Optional run_id to rerun failed tasks")
    parser.add_argument("--file-path", type=str, default="data/Equipment_Hire_Dataset.xlsx", help="Path to Excel file (if source is excel)")
    parser.add_argument("--db-file", type=str, default="data/equipment_rental.db", help="Path to SQLite DB file (if source is db)")

    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",")]

    try:
        run_pipeline(
            source_type=args.source,
            tables=tables,
            file_path=args.file_path,
            db_file=args.db_file,
            batch_type=args.batch_type,
            rerun_id=args.rerun_id
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()