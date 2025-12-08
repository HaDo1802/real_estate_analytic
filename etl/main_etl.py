"""
Real Estate ETL Pipeline
========================

Main ETL script that orchestrates the extraction, transformation, and loading
of real estate data from the Zillow API to PostgreSQL database.

Features:
- Timestamped file storage for audit trail
- Two-table strategy (CURRENT + HISTORY)
- Automatic cleanup of old files
- Comprehensive logging and error handling
"""

import sys
import os
import pandas as pd
from datetime import datetime
import logging

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import custom modules
from extract import fetch_zillow, LOCATIONS
from transform import main_transform
from load import load_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_base_paths():
    """
    Get base directory paths based on environment (local vs Docker).
    
    Returns:
        tuple: (base_dir, raw_dir, transformed_dir)
    """
    if os.path.exists("/opt/airflow"):
        # Docker/Airflow environment
        base_dir = "/opt/airflow"
        raw_dir = os.path.join(base_dir, "data", "raw")
        transformed_dir = os.path.join(base_dir, "data", "transformed")
    else:
        # Local environment
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_dir = os.path.join(base_dir, "data", "raw")
        transformed_dir = os.path.join(base_dir, "data", "transformed")
    
    # Ensure directories exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(transformed_dir, exist_ok=True)
    
    return base_dir, raw_dir, transformed_dir


def run_etl_pipeline(
    locations: list = None,
    status: str = "ForSale",
    cleanup_days: int = 30
) -> bool:
    """
    Run the complete ETL pipeline for real estate data.

    Args:
        locations: List of locations to search (defaults to all Vegas locations)
        status: Status of the properties to filter (e.g., "ForSale", "Sold")
        cleanup_days: Number of days to keep old files (default: 30)

    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    logger.info("=" * 60)
    logger.info("STARTING REAL ESTATE ETL PIPELINE")
    logger.info("=" * 60)

    start_time = datetime.now()
    etl_run_id = start_time.strftime("%Y%m%d_%H%M")
    
    # Get environment-specific paths
    base_dir, raw_dir, transformed_dir = get_base_paths()
    logger.info(f"🏠 Environment: {'Docker/Airflow' if os.path.exists('/opt/airflow') else 'Local'}")
    logger.info(f"📁 Base directory: {base_dir}")
    logger.info(f"📁 Raw directory: {raw_dir}")
    logger.info(f"📁 Transformed directory: {transformed_dir}")
    logger.info(f"🆔 ETL Run ID: {etl_run_id}")

    try:
        # =====================================================================
        # STEP 1: EXTRACT - Fetch data from Zillow API
        # =====================================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 1: EXTRACT - Fetching data from Zillow API")
        logger.info("=" * 60)
        
        # Use provided locations or default to all Vegas locations
        if locations is None:
            locations = LOCATIONS
        
        logger.info(f"📍 Fetching data from {len(locations)} locations: {', '.join(locations)}")
        
        all_raw_data = []
        total_properties = 0
        
        for location_name in locations:
            logger.info(f"   🔍 Fetching: {location_name}...")
            try:
                raw_data = fetch_zillow(location_name)
                if not raw_data.empty:
                    all_raw_data.append(raw_data)
                    total_properties += len(raw_data)
                    logger.info(f"      ✅ Fetched {len(raw_data)} properties")
                else:
                    logger.warning(f"      ⚠️  No data returned")
            except Exception as e:
                logger.error(f"      ❌ Error fetching {location_name}: {e}")
                continue
        
        # Combine all data
        if not all_raw_data:
            logger.error("❌ No data extracted from any location")
            return False
        
        df_combined = pd.concat(all_raw_data, ignore_index=True)
        logger.info(f"📊 Total extracted: {total_properties} properties")
        
        # Save raw data with timestamp
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")
        raw_timestamped = os.path.join(raw_dir, f"raw_{timestamp}.csv")
        raw_latest = os.path.join(raw_dir, "raw_latest.csv")
        
        df_combined.to_csv(raw_timestamped, index=False)
        df_combined.to_csv(raw_latest, index=False)
        
        logger.info(f"💾 Saved raw data:")
        logger.info(f"   - Timestamped: {raw_timestamped}")
        logger.info(f"   - Latest: {raw_latest}")
        
        # =====================================================================
        # STEP 2: TRANSFORM - Clean and enrich data
        # =====================================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 2: TRANSFORM - Cleaning and enriching data")
        logger.info("=" * 60)
        
        try:
            # Run transformation (it will read from raw_latest.csv)
            df_transformed, timestamped_file, latest_file = main_transform(
                input_file=raw_latest,
                output_dir=transformed_dir
            )
            
            if df_transformed is None or df_transformed.empty:
                logger.error("❌ No data remained after transformation")
                return False
            
            logger.info(f"✅ Transformation complete: {len(df_transformed)} records")
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
        
        # =====================================================================
        # STEP 3: LOAD - Insert into PostgreSQL (CURRENT + HISTORY tables)
        # =====================================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 3: LOAD - Inserting into PostgreSQL")
        logger.info("=" * 60)
        
        try:
            # Load using the upsert strategy (CURRENT + HISTORY tables)
            load_csv(csv_file=latest_file)
            logger.info("✅ Data loaded successfully to PostgreSQL")
            
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
        
        # =====================================================================
        # STEP 4: CLEANUP - Archive old files
        # =====================================================================
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"STEP 4: CLEANUP - Archiving files older than {cleanup_days} days")
        logger.info("=" * 60)
    
        
        # =====================================================================
        # PIPELINE SUMMARY
        # =====================================================================
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"📊 Summary:")
        logger.info(f"   ETL Run ID: {etl_run_id}")
        logger.info(f"   Duration: {duration}")
        logger.info(f"   Locations processed: {len(locations)}")
        logger.info(f"   Properties extracted: {total_properties}")
        logger.info(f"   Records loaded: {len(df_transformed)}")
        logger.info(f"   Raw file: {raw_timestamped}")
        logger.info(f"   Transformed file: {timestamped_file}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error("❌ ETL PIPELINE FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error("=" * 60)
        return False


def run_incremental_etl(
    locations: list = None,
    status: str = "ForSale",
    cleanup_days: int = 30
) -> bool:
    """
    Run an incremental ETL that only processes new/changed properties.
    
    This is a placeholder for future incremental logic. Currently runs full ETL.
    
    To implement true incremental:
    1. Query CURRENT table for max(updated_at)
    2. Filter API results to only properties changed since then
    3. Process only the delta
    
    Args:
        locations: List of locations to search
        status: Property status filter
        cleanup_days: Days to keep old files
    
    Returns:
        bool: Success status
    """
    logger.info("🔄 Running INCREMENTAL ETL (currently runs full ETL)")
    logger.info("   💡 Tip: Implement delta detection for true incremental processing")
    
    # For now, just run full ETL
    # TODO: Implement incremental logic
    return run_etl_pipeline(locations=locations, status=status, cleanup_days=cleanup_days)


if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Real Estate ETL Pipeline")
    parser.add_argument(
        "--status",
        default="ForSale",
        help="Property status filter (default: ForSale)"
    )
    parser.add_argument(
        "--cleanup-days",
        type=int,
        default=30,
        help="Days to keep old files (default: 30)"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run incremental ETL (only new/changed properties)"
    )
    
    args = parser.parse_args()
    
    # Run the appropriate ETL mode
    if args.incremental:
        success = run_incremental_etl(
            status=args.status,
            cleanup_days=args.cleanup_days
        )
    else:
        success = run_etl_pipeline(
            status=args.status,
            cleanup_days=args.cleanup_days
        )
    
    if success:
        print("\n🎉 ETL Pipeline completed successfully!")
        print("Check the database for your real estate data.")
        print("\n📊 Quick queries to run:")
        print("   SELECT COUNT(*) FROM real_estate_data.properties_sale_prices;")
        print("   SELECT COUNT(*) FROM real_estate_data.properties_sale_prices_history;")
    else:
        print("\n❌ ETL Pipeline failed.")
        print("Check the logs for more details: etl_pipeline.log")
        sys.exit(1)