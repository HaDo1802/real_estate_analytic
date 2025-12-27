import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from extract import fetch_all_locations
from transform import main_transform
from load import load_csv
from logger import get_logger
from email_notifier import EmailNotifier

logger = get_logger(__name__)


def get_base_paths():
    if os.path.exists("/opt/airflow"):
        base_dir = "/opt/airflow"
        raw_dir = os.path.join(base_dir, "data", "raw")
        transformed_dir = os.path.join(base_dir, "data", "transformed")
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_dir = os.path.join(base_dir, "data", "raw")
        transformed_dir = os.path.join(base_dir, "data", "transformed")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(transformed_dir, exist_ok=True)

    return base_dir, raw_dir, transformed_dir


def run_etl_pipeline():
    logger.info("STARTING REAL ESTATE ETL PIPELINE")

    start_time = datetime.now()
    etl_run_id = start_time.strftime("%Y%m%d_%H%M")
    base_dir, raw_dir, transformed_dir = get_base_paths()
    env_type = "Docker/Airflow" if os.path.exists("/opt/airflow") else "Local"

    logger.info(f"Environment: {env_type} | ETL Run ID: {etl_run_id}")

    details = {
        "etl_run_id": etl_run_id,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "environment": env_type,
    }

    try:
        logger.info("STAGE 1: EXTRACT")
        df_extracted = fetch_all_locations()
        if df_extracted.empty:
            details["error"] = "No data extracted from API"
            details["failed_step"] = "EXTRACT"
            return False, details

        details["properties_extracted"] = len(df_extracted)
        logger.info(f"EXTRACT COMPLETED: {len(df_extracted)} properties")

        logger.info("STAGE 2: TRANSFORM")
        raw_latest = os.path.join(raw_dir, "raw_latest.csv")
        df_transformed, timestamped_file, latest_file = main_transform(
            input_file=raw_latest, output_dir=transformed_dir
        )

        if df_transformed is None or df_transformed.empty:
            details["error"] = "No data after transformation"
            details["failed_step"] = "TRANSFORM"
            return False, details

        details["records_transformed"] = len(df_transformed)
        logger.info(f"TRANSFORM COMPLETED")

        logger.info("STAGE 3: LOAD")
        load_csv(csv_file=latest_file)
        details["records_loaded"] = len(df_transformed)
        logger.info("LOAD COMPLETED\n")

        end_time = datetime.now()
        duration = end_time - start_time

        details["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        details["duration"] = str(duration).split(".")[0]
        details["quality_rate"] = f"{len(df_transformed)/len(df_extracted)*100:.1f}%"
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(
            f"Duration: {details['duration']} | Quality: {details['quality_rate']}"
        )
        return True, details

    except Exception as e:
        logger.error(f"ETL PIPELINE FAILED: {str(e)}", exc_info=True)
        details["error"] = str(e)
        details["failed_step"] = details.get("failed_step", "UNKNOWN")
        return False, details


if __name__ == "__main__":
    logger.info("Real Estate ETL Pipeline - Main Entry Point\n")

    email_notifier = EmailNotifier()
    pipeline_start = datetime.now()

    success, details = run_etl_pipeline()

    details["total_execution_time"] = str(datetime.now() - pipeline_start).split(".")[0]

    if success:
        logger.info("Sending success email...")
        email_notifier.send_notification(success=True, details=details)
    else:
        logger.error(f"Total execution time: {details['total_execution_time']}")
        logger.error(f"Failed at: {details.get('failed_step', 'UNKNOWN')}")
        logger.error("Sending failure email...")
        email_notifier.send_notification(success=False, details=details)
        sys.exit(1)
