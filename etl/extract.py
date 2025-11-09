import requests, os, time, json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

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

HEADERS = {
    "X-RapidAPI-Key": RAPID_API_KEY,
    "X-RapidAPI-Host": "zillow56.p.rapidapi.com",
}

LOCATIONS = [
    "Las Vegas, NV",
    "Henderson, NV",
    "North Las Vegas, NV",
    "Summerlin, NV",
    "Enterprise, NV",
]


# Extract real estate data from Zillow API
def fetch_zillow(location, status="ForSale"):
    """
    Fetch real estate data from Zillow API

    Args:
        location: Location to search (e.g., "Las Vegas, NV")
        status: Property status (e.g., "ForSale", "ForRent")

    Returns:
        dict: JSON response from API containing property data
    """
    # Check if API key is set
    api_key = os.getenv("RAPID_API_KEY")
    if not api_key:
        print("Error: RAPID_API_KEY environment variable not set")
        return pd.DataFrame()  # Return empty DataFrame

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "zillow-com1.p.rapidapi.com",
    }

    params = {"location": location, "status_type": status, "home_type": "Houses"}

    try:
        print(f"Fetching data for {location} with status {status}")
        res = requests.get(
            "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch",
            headers=headers,
            params=params,
        )

        if res.status_code != 200:
            print(f"API request failed with status code: {res.status_code}")
            return pd.DataFrame()  # Return empty DataFrame

        json_data = res.json()

        if not json_data or "props" not in json_data:
            print("No property data found in API response")
            return pd.DataFrame()  # Return empty DataFrame

        print(f"Successfully fetched {len(json_data['props'])} properties")

        if (
            not json_data
            or not isinstance(json_data, dict)
            or "props" not in json_data
            or not json_data["props"]
        ):
            logger.error("No data extracted from API")
            return pd.DataFrame()  # Return empty DataFrame instead of dict

        # Convert to DataFrame
        df_raw = pd.DataFrame(json_data["props"])

        # Set up output path for Docker/Airflow compatibility
        output_path = (
            "/opt/airflow/data/raw_data.csv"
            if os.path.exists("/opt/airflow")
            else os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "raw_data.csv"
            )
        )
        output_path = os.path.abspath(output_path)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save to CSV with smart accumulation
        file_exists = os.path.exists(output_path)

        # If file exists, append without header; if new file, include header
        if file_exists:
            df_raw.to_csv(output_path, mode="a", header=False, index=False)
            logger.info(
                f"Appended {len(df_raw)} properties to existing file {output_path}"
            )
        else:
            df_raw.to_csv(output_path, index=False)
            logger.info(
                f"Created new file with {len(df_raw)} properties: {output_path}"
            )
        return df_raw

    except Exception as e:
        print(f"Error fetching data for {location}: {e}")
        return pd.DataFrame()  # Return empty DataFrame instead of dict


if __name__ == "__main__":
    print("Run directly from extract.py script: Fetching Zillow data...")

    for loc in LOCATIONS:
        try:
            df_result = fetch_zillow(loc, status="ForSale")

            # Check if we got a valid DataFrame
            if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                print(f"Successfully fetched {len(df_result)} properties from {loc}")
            else:
                print(f"No data fetched for {loc}")

            time.sleep(2)
        except Exception as e:
            print(f"Failed {loc}: {e}")

    print("Zillow data ingestion complete!")
