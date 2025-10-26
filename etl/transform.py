import pandas as pd
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_address_components(address: str) -> Dict[str, Optional[str]]:

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


def calculate_date_listing(daysOnZillow: Any) -> Optional[datetime]:
    if pd.isna(daysOnZillow):
        return None
    try:
        days = int(daysOnZillow)
        update_date = datetime.now(timezone.utc) - pd.Timedelta(days=days)
        return update_date
    except Exception:
        return None


def extract_listing_subtype_info(listing_subtype: Any) -> Dict[str, bool]:
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


def transform_real_estate_data(input_path: str, output_path: str) -> pd.DataFrame:

    if not os.path.exists(input_path):
        logger.error(f"Input file does not exist: {input_path}")
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    # Read input CSV
    df = pd.read_csv(input_path)
    logger.info(f"Starting transformation of {len(df)} records from {input_path}")

    # Create a copy to avoid modifying the original dataframe
    df_transformed = df.copy()

    # Extract address components
    logger.info("Extracting address components...")
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

    # Clean integer fields
    logger.info("Cleaning integer fields...")
    integer_fields = ["bathrooms", "bedrooms", "daysOnZillow"]
    for i in integer_fields:
        if i in df_transformed.columns:
            df_transformed[i] = df_transformed[i].apply(
                lambda x: int(x) if pd.notna(x) else None
            )
        else:
            df_transformed[i] = None

    # Extract listing subtype information
    logger.info("Extracting listing subtype information...")
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

    # Add data processing timestamp
    df_transformed["processed_at"] = datetime.now(timezone.utc)

    # Add date_listing column
    if "daysOnZillow" in df_transformed.columns:
        df_transformed["date_listing"] = df_transformed["daysOnZillow"].apply(
            calculate_date_listing
        )
    else:
        df_transformed["date_listing"] = None

    # Select final columns for database loading
    excluded_columns = ["imgSrc", "detailUrl", "hasImage", "carouselPhotos"]
    available_columns = [
        col for col in df_transformed.columns if col not in excluded_columns
    ]
    df_final = df_transformed[available_columns].copy()
    df_final = df_final.rename(columns={"zpid": "zillow_property_id"})

    # Remove rows where essential fields are missing
    essential_fields = ["zillow_property_id", "street_address"]
    df_final = df_final.dropna(subset=essential_fields, how="all")

    # Save cleaned output
    try:
        df_final.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned data to {output_path} ({len(df_final)} records)")
    except Exception as e:
        logger.error(f"Failed to save output to {output_path}: {e}")
        raise

    logger.info(
        f"Transformation completed. Final dataset has {len(df_final)} records and {len(df_final.columns)} columns"
    )

    return df_final


def validate_transformed_data(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path)

    logger.info("Validating transformed data...")

    # Check for required columns
    required_columns = ["zillow_property_id", "street_address", "processed_at"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False

    # Check for duplicate property IDs
    if df["zillow_property_id"].duplicated().any():
        logger.warning("Duplicate property IDs found in the data")

    # Check data types
    numeric_columns = [
        "price",
        "zestimate",
        "bathrooms",
        "bedrooms",
        "living_area_sqft",
        "lot_area_sqft",
        "latitude",
        "longitude",
        "daysOnZillow",
    ]

    for col in numeric_columns:
        if col in df.columns:
            non_numeric = df[col].apply(
                lambda x: x is not None and not isinstance(x, (int, float))
            )
            if non_numeric.any():
                logger.warning(f"Non-numeric values found in column {col}")

    logger.info("Data validation completed")
    return True


if __name__ == "__main__":
    import argparse
    import os

    # Set up command line argument parsing (arguments optional for testing)
    parser = argparse.ArgumentParser(description="Transform real estate data")
    parser.add_argument(
        "--input",
        default="/Users/hado/Desktop/Career/Coding/Data Engineer/Project/real_estate_project/data/raw_data.csv",
        help="Input CSV file path (raw data). If omitted, a sensible default is used for testing.",
    )
    parser.add_argument(
        "--output",
        default="/Users/hado/Desktop/Career/Coding/Data Engineer/Project/real_estate_project/data/transformed_real_estate_data.csv",
        help="Output CSV file path (transformed data). If omitted, a sensible default is used for testing.",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    logger.info(f"🔄 Starting transformation process")
    logger.info(f"📥 Input file: {args.input}")
    logger.info(f"📤 Output file: {args.output}")

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error(f"❌ Input file {args.input} does not exist")
        exit(1)

    try:
        # Use provided or default paths
        input_path = args.input
        output_path = args.output

        # Transform data (function handles file I/O internally)
        logger.info("🔄 Starting data transformation...")
        df_cleaned = transform_real_estate_data(input_path, output_path)
        logger.info(f"✅ Transformation completed. {len(df_cleaned)} records processed")

        # Validate transformed data
        logger.info("🔍 Validating transformed data...")
        validation_result = validate_transformed_data(output_path)
        if validation_result is not True:
            logger.warning("⚠️ Data validation reported issues or returned False")
        else:
            logger.info("✅ Data validation passed")

        # Log summary statistics
        logger.info("📈 Transformation Summary:")
        logger.info(f"   • Output records: {len(df_cleaned)}")
        logger.info(f"   • Output columns: {len(df_cleaned.columns)}")
        logger.info(f"   • Output file: {output_path}")
        if os.path.exists(output_path):
            logger.info(f"   • File size: {os.path.getsize(output_path)} bytes")

        logger.info("🎉 Transformation completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        exit(1)
    except pd.errors.EmptyDataError:
        logger.error(f"❌ Input file {args.input} is empty")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Transformation failed: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        exit(1)
