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
from load import load_csv_to_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_etl_pipeline(location: str = "Las Vegas, NV", status="ForSale") -> bool:
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
        # Step 1: Extract data from API for all Vegas locations
        logger.info("Step 1: Extracting data from Zillow API...")
        from extract import LOCATIONS  # Import the locations list

        total_properties = 0
        for location_name in LOCATIONS:
            logger.info(f"Fetching data for {location_name}...")
            raw_data = fetch_zillow(location_name, status=status)
            if not raw_data.empty:
                total_properties += len(raw_data)
                logger.info(
                    f"✅ Fetched {len(raw_data)} properties from {location_name}"
                )
            else:
                logger.warning(f"⚠️ No data from {location_name}")

        logger.info(
            f"📊 Total extracted: {total_properties} properties from {len(LOCATIONS)} locations"
        )

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
        validation_result = validate_transformed_data(output_file)
        if validation_result is not True:
            logger.error("Data validation failed")
            return False

        # Load transformed CSV to PostgreSQL (pass the file path expected by loader)
        logger.info("Loading transformed CSV to PostgreSQL...")
        load_csv_to_postgres(
            output_file,
            schema=os.getenv("DEFAULT_SCHEMA", "real_estate_data"),
            table=os.getenv("DEFAULT_TABLE", "test_table"),
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
    success = run_etl_pipeline(location="Las Vegas, NV", status="ForSale")

    if success:
        print("\\n🎉 ETL Pipeline completed successfully!")
        print("Check the database for your real estate data.")
    else:
        print("\\n❌ ETL Pipeline failed.")
        print("Check the logs for more details.")
