import os
import pandas as pd
import logging
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Input/Output paths
DEFAULT_INPUT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "raw", "raw_latest.csv")
)
DEFAULT_OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "transformed")
)


def extract_address_components(address: str) -> dict:
    """Extract street, city, state, zip from address string."""
    if not address or pd.isna(address):
        return {"street_address": None, "city": None, "state": None, "zip_code": None}

    parts = address.split(", ")
    if len(parts) >= 3:
        street_address = parts[0]
        city = parts[1]
        # Handle "State ZIP" format
        state_zip = parts[2].split(" ")
        state = state_zip[0] if state_zip else None
        zip_code = state_zip[1] if len(state_zip) > 1 else None
    else:
        street_address = address
        city = None
        state = None
        zip_code = None

    return {
        "street_address": street_address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }


def calculate_date_listing(daysOnZillow) -> datetime:
    """Calculate listing date from days on Zillow."""
    if pd.isna(daysOnZillow):
        return None
    try:
        days = int(daysOnZillow)
        update_date = (datetime.now(timezone.utc) - pd.Timedelta(days=days)).date()
        return update_date
    except Exception:
        return None


def convert_unix_timestamp(timestamp_value) -> datetime:
    """Convert Unix timestamp (in milliseconds) to datetime."""
    if pd.isna(timestamp_value) or not timestamp_value:
        return None
    try:
        # Handle both string and numeric timestamps
        if isinstance(timestamp_value, str):
            timestamp_value = float(timestamp_value)

        # Convert from milliseconds to seconds
        timestamp_seconds = float(timestamp_value) / 1000.0

        # Convert to datetime
        return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def extract_listing_subtype_info(listing_subtype) -> dict:
    """Extract FSBA and open house flags from listing subtype."""
    default_flags = {"is_fsba": False, "is_open_house": False}

    if not listing_subtype or pd.isna(listing_subtype):
        return default_flags

    if isinstance(listing_subtype, str):
        try:
            listing_subtype = json.loads(listing_subtype.replace("'", '"'))
        except json.JSONDecodeError:
            return default_flags

    if isinstance(listing_subtype, dict):
        return {
            "is_fsba": listing_subtype.get("is_FSBA", False),
            "is_open_house": listing_subtype.get("is_openHouse", False),
        }

    return default_flags


def normalize_lot_area_value(area_value, unit_value) -> float:
    """
    Normalize lot area: convert acres to sqft (1 acre = 43560 sqft).
    """
    if pd.isna(area_value) or area_value is None:
        return None

    if unit_value is not None and not pd.isna(unit_value):
        unit_str = str(unit_value).lower()
        if "acre" in unit_str:
            return round(area_value * 43560.0, 2)
    
    # Return as-is if already in sqft or unknown unit
    return area_value


def extract_vegas_district(address: str, city: str) -> str:
    """Extract Vegas district/neighborhood from address."""
    if not address or pd.isna(address):
        return "Unknown"

    address_lower = address.lower()

    # Map common Vegas areas
    district_mapping = {
        "summerlin": "Summerlin",
        "henderson": "Henderson",
        "north las vegas": "North Las Vegas",
        "enterprise": "Enterprise",
        "spring valley": "Spring Valley",
        "green valley": "Green Valley",
        "centennial": "Centennial",
        "anthem": "Anthem",
        "mountains edge": "Mountains Edge",
        "downtown": "Downtown Las Vegas",
        "strip": "The Strip",
        "fremont": "Downtown Las Vegas",
        "paradise": "Paradise",
        "winchester": "Winchester",
    }

    # Check address for district keywords
    for keyword, district in district_mapping.items():
        if keyword in address_lower:
            return district

    # Fallback to city name if available
    if city and not pd.isna(city):
        city_clean = str(city).strip()
        if city_clean:
            return city_clean

    return "Las Vegas"


def main_transform(input_file=DEFAULT_INPUT, output_dir=DEFAULT_OUTPUT_DIR):

    try:
        # === STEP 1: Read raw data ===
        logger.info(f"📂 Reading raw data from: {input_file}")
        
        if not os.path.exists(input_file):
            logger.error(f"❌ Input file does not exist: {input_file}")
            raise FileNotFoundError(f"Input file does not exist: {input_file}")
        
        df = pd.read_csv(input_file)
        initial_count = len(df)
        logger.info(f"   Loaded {initial_count} raw records")
        
        # === STEP 2: Data cleaning & transformation ===
        logger.info("🧹 Starting data transformation...")
        
        df_transformed = df.copy()
        is_docker = os.path.exists("/opt/airflow")
        if not is_docker:
            df_transformed = pd.read_csv("/Users/hado/Desktop/Career/Coding/Data Engineer/Project/real_estate_project/data/raw/raw_latest.csv")
        else:
            df_transformed = pd.read_csv("/opt/airflow/data/raw/raw_latest.csv")
        # Delete unnecessary columns


        
        # Extract address components
        logger.info("   Extracting address components...")
        if "address" in df_transformed.columns:
            address_components = df_transformed["address"].apply(extract_address_components)
            for component in ["street_address", "city", "state", "zip_code"]:
                df_transformed[component] = address_components.apply(
                    lambda x: x.get(component)
                )
        else:
            df_transformed["street_address"] = None
            df_transformed["city"] = None
            df_transformed["state"] = None
            df_transformed["zip_code"] = None
        
        # Extract listing subtype information
        logger.info("   Extracting listing subtype information...")
        if "listingSubType" in df_transformed.columns:
            listing_flags = df_transformed["listingSubType"].apply(
                extract_listing_subtype_info
            )
            df_transformed["is_fsba"] = listing_flags.apply(lambda x: x.get("is_fsba"))
            df_transformed["is_open_house"] = listing_flags.apply(
                lambda x: x.get("is_open_house")
            )
        else:
            df_transformed["is_fsba"] = False
            df_transformed["is_open_house"] = False
        
        # Convert Unix timestamp fields to proper datetime format
        logger.info("   Converting timestamp fields...")
        timestamp_fields = ["datePriceChanged"]
        for field in timestamp_fields:
            if field in df_transformed.columns:
                df_transformed[field] = df_transformed[field].apply(convert_unix_timestamp)
        
        # Add date_listing column
        logger.info("   Adding date_listing column...")
        if "daysOnZillow" in df_transformed.columns:
            df_transformed["date_listing"] = df_transformed["daysOnZillow"].apply(
                calculate_date_listing
            )
        else:
            df_transformed["date_listing"] = None
        
        # Normalize lot area to sqft
        logger.info("   Normalizing lot area to sqft...")
        df_transformed["Normalized_lotAreaValue"] = df_transformed.apply(
            lambda row: normalize_lot_area_value(
                row.get("lotAreaValue"), row.get("lotAreaUnit")
            ),
            axis=1,
        )
        df_transformed['lotAreaUnit'] = 'sqft'
        # Extract Vegas districts
        logger.info("   Extracting Vegas districts...")
        df_transformed["vegas_district"] = df_transformed.apply(
            lambda row: extract_vegas_district(row.get("address", ""), row.get("city", "")),
            axis=1,
        )
        
        # Rename zpid to zillow_property_id
        df_final = df_transformed.rename(columns={"zpid": "zillow_property_id"})
        df_final = df_final.drop(columns=['address', 'extraction_location'], errors='ignore')
        
        logger.info("   Deleting unnecessary columns...")
        columns_to_delete = [
            "has3DModel",
            "comingSoonOnMarketDate",
            "contingentListingType",
            "detailUrl",
            "hasImage",
            "hasVideo",
            "imgSrc",
            "carouselPhotos",
            "ImgSrc",
            "variableData",
            "currency","newConstructionType", "listingSubType","Country", "lotAreaUnit"
        ]
        df_final.drop(
            columns=[col for col in columns_to_delete if col in df_final.columns],
            inplace=True,
        )

        ordered_cols = ['zillow_property_id', 'street_address', 'city', 'vegas_district', 'zip_code','latitude', 'longitude', 'livingArea', 'Normalized_lotAreaValue', 'bathrooms', 'bedrooms',  'price', 'rentZestimate', 'zestimate','propertyType','Unit', 'daysOnZillow', 'date_listing', 'datePriceChanged','listingStatus', 'is_fsba', 'is_open_house',  'processed_at', 'etl_run_id']
        df_final = df_final.reindex(columns=ordered_cols)
        # === STEP 3: Add ETL metadata ===
        logger.info("📝 Adding ETL metadata...")
        current_time = datetime.now()
        
        # Add timestamp for this ETL run
        df_final['processed_at'] = current_time
        
        # Add ETL run ID (useful for tracking batches)
        etl_run_id = current_time.strftime("%Y%m%d_%H%M")
        df_final['etl_run_id'] = etl_run_id
        
        logger.info(f"   ETL Run ID: {etl_run_id}")
        logger.info(f"   Processed at: {current_time}")
        
        # === STEP 4: Data quality validation ===
        logger.info("✅ Running data quality checks...")
        
        # Remove rows where essential fields are missing
        essential_fields = ["zillow_property_id", "price"]
        before_filter = len(df_final)
        df_final = df_final.dropna(subset=essential_fields)
        removed_count = before_filter - len(df_final)
        if removed_count > 0:
            logger.info(f"   Removed {removed_count} records with missing critical fields")
        
    
        final_count = len(df_final)
        logger.info(f"   ✅ Data quality checks passed: {final_count} valid records")
        
        # === STEP 5: Save transformed data ===
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save timestamped file (for audit trail)
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        timestamped_file = os.path.join(output_dir, f"transformed_{timestamp}.csv")
        df_final.to_csv(timestamped_file, index=False)
        logger.info(f"💾 Saved timestamped file: {timestamped_file}")
        
        # Save as 'latest' file (for load script to read)
        latest_file = os.path.join(output_dir, "transformed_latest.csv")
        df_final.to_csv(latest_file, index=False)
        logger.info(f"💾 Saved latest file: {latest_file}")
        
        # === STEP 6: Summary statistics ===
        logger.info("=" * 60)
        logger.info("📊 TRANSFORMATION SUMMARY:")
        logger.info(f"   Input file: {input_file}")
        logger.info(f"   Initial records: {initial_count}")
        logger.info(f"   Final records: {final_count}")
        logger.info(f"   Records filtered: {initial_count - final_count}")
        logger.info(f"   ETL Run ID: {etl_run_id}")
        logger.info(f"   Output files:")
        logger.info(f"     - Timestamped: {timestamped_file}")
        logger.info(f"     - Latest: {latest_file}")
        logger.info("=" * 60)
        
        return df_final, timestamped_file, latest_file
        
    except FileNotFoundError:
        logger.error(f"❌ Input file not found: {input_file}")
        raise
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    
    
if __name__ == "__main__":
    # Environment detection
    is_docker = os.path.exists("/opt/airflow")
    env_name = "Docker/Airflow" if is_docker else "Local"
    logger.info(f"🚀 Running in {env_name} environment")
    
    try:
        # Run transformation
        df_transformed, timestamped_path, latest_path = main_transform()
        
        logger.info("🎉 Transformation completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        exit(1)