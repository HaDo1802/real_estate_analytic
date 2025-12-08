import requests, os, time, json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_extract.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file in parent directory
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
)

RAPID_API_KEY = os.getenv("RAPID_API_KEY")

LOCATIONS = [
    "Las Vegas, NV"
]
def fetch_zillow(location, max_pages=2):

    # Check if API key is set
    api_key = os.getenv("RAPID_API_KEY")
    if not api_key:
        logger.error("Error: RAPID_API_KEY environment variable not set")
        sys.exit()

    headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": "us-housing-market-data1.p.rapidapi.com"
        }
    url = "https://us-housing-market-data1.p.rapidapi.com/propertyExtendedSearch"
    all_props = []
    page = 1
    try:
        while page <= max_pages:
            # Removed status_type parameter - gets ALL property statuses
            params = {
                "location": location,

                "page": page,
            }

            logger.info(f"Fetching data for {location}, page {page}")

            res = requests.get(
                url,
                headers=headers,
                params=params,
            )

            if res.status_code != 200:
                logger.warning(
                    f"API request failed with status code: {res.status_code}"
                )
                break

            json_data = res.json()

            if not json_data or "props" not in json_data:
                logger.warning("No property data found in API response")
                break

            props = json_data["props"]
            logger.info(f"    Fetched {len(props)} properties on page {page}")
            all_props.extend(props)

            # Pagination logic
            total_pages = int(json_data.get("totalPages", page))
            if page >= total_pages:
                break

            page += 1
            time.sleep(0.2)

        if not all_props:
            logger.error("No data extracted from API")
            return pd.DataFrame()

        # Convert to DataFrame
        df_raw = pd.DataFrame(all_props)

        # Add extraction metadata
        df_raw["extracted_at"] = datetime.now()
        
        # # Set up output path for Docker/Airflow compatibility
        # if os.path.exists("/opt/airflow"):
        #     raw_dir = "/opt/airflow/data/raw"
        # else:
        #     raw_dir = os.path.join(
        #         os.path.dirname(os.path.dirname(__file__)), "data", "raw"
        #     )

        # os.makedirs(raw_dir, exist_ok=True)

        # # Save timestamped file for audit trail
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # raw_timestamped = os.path.join(
        #     raw_dir,
        #     f"raw_{location.replace(', ', '_').replace(' ', '_')}_{timestamp}.csv",
        # )
        # df_raw.to_csv(raw_timestamped, index=False)
        # logger.info(f" Saved timestamped: {raw_timestamped}")

        return df_raw

    except Exception as e:
        logger.error(f"Error fetching data for {location}: {e}")
        return pd.DataFrame()


def fetch_all_locations():
    """
    Fetch data from all configured locations and combine into single file.

    Returns:
        pd.DataFrame: Combined data from all locations
    """
    logger.info("=" * 60)
    logger.info("STARTING DATA EXTRACTION")
    logger.info("=" * 60)
    logger.info(f" Locations to fetch: {len(LOCATIONS)}")

    all_data = []
    total_properties = 0

    for loc in LOCATIONS:
        logger.info(f"\n Fetching: {loc}")
        try:
            df_result = fetch_zillow(loc)

            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                all_data.append(df_result)
                total_properties += len(df_result)
                logger.info(f"    Successfully fetched {len(df_result)} properties")
            else:
                logger.warning(f"     No data fetched for {loc}")

            # Rate limiting between locations
            time.sleep(2)

        except Exception as e:
            logger.error(f"    Failed {loc}: {e}")
            continue

    # Combine all location data
    if not all_data:
        logger.error(" No data extracted from any location")
        return pd.DataFrame()

    df_combined = pd.concat(all_data, ignore_index=True)

    # Remove duplicates based on property ID
    initial_count = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=["zpid"], keep="first")
    duplicates = initial_count - len(df_combined)

    logger.info("")
    logger.info("=" * 60)
    logger.info(" EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"   Locations processed: {len(LOCATIONS)}")
    logger.info(f"   Total properties fetched: {initial_count}")
    logger.info(f"   Unique properties: {len(df_combined)}")
    logger.info(f"   Duplicates removed: {duplicates}")
    logger.info("=" * 60)

    # Save combined files
    if os.path.exists("/opt/airflow"):
        raw_dir = "/opt/airflow/data/raw"
    else:
        raw_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "raw"
        )

    timestamp = datetime.now().strftime("%Y%m%d")

    # Save timestamped combined file
    raw_timestamped = os.path.join(raw_dir, f"raw_{timestamp}.csv")
    df_combined.to_csv(raw_timestamped, index=False)
    logger.info(f"Saved combined timestamped: {raw_timestamped}")

    # Save latest file (for transform script to read)
    raw_latest = os.path.join(raw_dir, "raw_latest.csv")
    df_combined.to_csv(raw_latest, index=False)
    logger.info(f"Saved combined latest: {raw_latest}")

    return df_combined


if __name__ == "__main__":
    logger.info(" Starting Zillow data extraction...")

    df_result = fetch_all_locations()

    if not df_result.empty:
        logger.info(f"\n Extraction completed successfully!")
        logger.info(f"   Total unique properties: {len(df_result)}")

        # Show property status breakdown if available
        if "homeStatus" in df_result.columns:
            logger.info("\n Property Status Breakdown:")
            status_counts = df_result["homeStatus"].value_counts()
            for status, count in status_counts.items():
                logger.info(f"   {status}: {count}")
    else:
        logger.error("\n Extraction failed - no data retrieved")
        sys.exit(1)
