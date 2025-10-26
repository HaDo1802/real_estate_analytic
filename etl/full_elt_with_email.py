#!/usr/bin/env python3
"""
Cron Job Wrapper for Real Estate ETL Pipeline
=============================================

This script is designed to be called by cron. It runs the ETL pipeline
and sends email notifications about the results.
"""

import sys
import os
import traceback
from datetime import datetime
import logging

# Add the project directory to Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Import modules
try:
    from main_etl import run_etl_pipeline
    from email_notifier import EmailNotifier
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

# Configure logging for cron
log_file = os.path.join(project_dir, "cron_etl.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_pipeline_with_notification():
    """Run the ETL pipeline and send email notification."""
    start_time = datetime.now()
    notifier = EmailNotifier()

    logger.info("Starting scheduled ETL pipeline run")

    try:
        # Run the ETL pipeline
        success = run_etl_pipeline(location="Los Angeles, CA", status="ForSale")

        end_time = datetime.now()
        duration = end_time - start_time

        if success:
            logger.info("ETL pipeline completed successfully")

            # Send success notification
            details = {
                "duration": str(duration),
                "location": "Los Angeles, CA",
                "records_processed": "Check logs for details",
            }

            notifier.send_notification(success=True, details=details)

        else:
            logger.error("ETL pipeline failed")

            # Send failure notification
            details = {
                "error": "Pipeline returned False - check logs for details",
                "location": "Los Angeles, CA",
            }

            notifier.send_notification(success=False, details=details)

    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time

        error_msg = str(e)
        logger.error(f"ETL pipeline crashed with error: {error_msg}")
        logger.error(f"Traceback: {traceback.format_exc()}")

        # Send failure notification
        details = {
            "error": error_msg,
            "location": "Los Angeles, CA",
            "traceback": traceback.format_exc(),
        }

        notifier.send_notification(success=False, details=details)

    logger.info(f"Pipeline run completed. Duration: {duration}")


if __name__ == "__main__":
    run_pipeline_with_notification()
