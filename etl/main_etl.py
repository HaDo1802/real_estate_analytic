"""
Real Estate ETL Pipeline
========================

Main ETL script that orchestrates the extraction, transformation, and loading
of real estate data from the Zillow API to PostgreSQL database.
"""

import sys
import os
import pandas as pd
from datetime import datetime
import logging

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import custom modules
from extract import fetch_zillow
from transform import transform_real_estate_data, validate_transformed_data
from load import load_records_to_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_etl_pipeline(location: str = "Los Angeles, CA", status="ForSale") -> bool:
    """
    Run the complete ETL pipeline for real estate data.

    Args:
        location: Location to search for properties
        status: Status of the properties to filter (e.g., "ForSale", "Sold")

    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    logger.info("=" * 60)
    logger.info("STARTING REAL ESTATE ETL PIPELINE")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        # Step 1: Extract data from API
        logger.info("Step 1: Extracting data from Zillow API...")
        raw_data = fetch_zillow(location, status=status)

        logger.info(f"Extracted {len(raw_data)} properties")

        # Step 2: Transform data
        logger.info("Step 2: Transforming data...")
        try:
            # Dynamic path handling for both local and Docker environments
            if os.path.exists("/opt/airflow"):
                input_file = "/opt/airflow/data/raw_data.csv"
                output_file = "/opt/airflow/data/transformed_real_estate_data.csv"
            else:
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                input_file = os.path.join(base_dir, "data", "raw_data.csv")
                output_file = os.path.join(
                    base_dir, "data", "transformed_real_estate_data.csv"
                )

            logger.info(f"Input file: {input_file}")
            logger.info(f"Output file: {output_file}")

            result = transform_real_estate_data(input_file, output_file)
            # support both: DataFrame or (DataFrame, meta) tuple
            if isinstance(result, tuple):
                df_transformed = result[0]
            else:
                df_transformed = result
            if df_transformed is None:
                df_transformed = pd.DataFrame()
        except Exception as e:
            logger.error(f"Transformation step failed: {e}")
            import traceback

            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        if df_transformed.empty:
            logger.error("No data remained after transformation")
            return False

        # Step 3: Validate transformed data
        logger.info("Step 3: Validating transformed data...")
        if not validate_transformed_data(output_file):
            logger.error("Data validation failed")
            return False

        # Load records to PostgreSQL
        logger.info("Loading records to PostgreSQL...")
        load_records_to_postgres(
            records=df_transformed.to_dict(orient="records"),
            schema=os.getenv("DEFAULT_SCHEMA", "real_estate_data"),
            table=os.getenv("DEFAULT_TABLE", "properties_prices"),
            mode=os.getenv("DEFAULT_MODE", "truncate"),
        )

        # Pipeline completed successfully
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")
        logger.info(f"Records processed: {len(df_transformed)}")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"ETL pipeline failed with error: {e}")
        return False


if __name__ == "__main__":
    # Run the ETL pipeline
    success = run_etl_pipeline(location="Los Angeles, CA", status="ForSale")

    if success:
        print("\\n🎉 ETL Pipeline completed successfully!")
        print("Check the database for your real estate data.")
    else:
        print("\\n❌ ETL Pipeline failed.")
        print("Check the logs for more details.")

