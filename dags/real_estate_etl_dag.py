from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
import sys
import os

# Add ETL directory to Python path
sys.path.append("/opt/airflow/etl")

# Import your ETL modules
from main_etl import run_etl_pipeline
from load import load_csv_to_postgres

# Default arguments for the DAG
default_args = {
    "owner": "hado",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 25),
    "email": ["havando1802@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "catchup": False,
}

# Create the DAG
with DAG(
    "real_estate_etl_pipeline",
    default_args=default_args,
    description="Real Estate ETL Pipeline - Runs every 4 hours",
    schedule_interval=timedelta(hours=4),  # Every 4 hours
    max_active_runs=1,  # Only one instance running at a time
    tags=["real_estate", "etl", "zillow"],
    catchup=False) as dag:

    # Setup data directory
    setup_data_dir = BashOperator(
        task_id="setup_data_directory",
        bash_command="mkdir -p /opt/airflow/data && chmod 755 /opt/airflow/data",
    )

    # 1. scrape
    scrape_real_estate_data = BashOperator(
        task_id="scrape_real_estate_data",
        bash_command="python /opt/airflow/etl/extract.py ",
    )
    # 2. Log note
    note = BashOperator(
        task_id="note",
        bash_command="echo '✅ Extracted real estate data'",
    )

    # 3. Clean / transform
    clean_data = BashOperator(
        task_id="clean_data",
        bash_command="python /opt/airflow/etl/transform.py "
        "--input /opt/airflow/data/raw_data.csv "
        "--output /opt/airflow/data/transformed_real_estate_data.csv "
        "--log-level INFO",
        retries=2,
    )

    note_clean_data = BashOperator(
        task_id="note_clean_data",
        bash_command="echo '✅ Cleaned data ready for loading'",
    )

    # 4. Upload cleaned data into Postgres
    upload_cleaned_data_to_postgres = BashOperator(
        task_id="upload_cleaned_data_to_postgres",
        bash_command="python /opt/airflow/etl/load.py",
    )

    # 5. Send success email
    success_email = EmailOperator(
        task_id="send_success_email",
        to=["havando1802@gmail.com"],
        subject="✅ Real Estate ETL Pipeline - Success",
        html_content="""
        <h3>🏠 Real Estate ETL Pipeline Completed Successfully</h3>
        <p><strong>Execution Date:</strong> {{ ds }}</p>
        <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
        <p><strong>Status:</strong> ✅ Success</p>
        <p>Your Los Angeles real estate data has been updated successfully!</p>
        <p><strong>Data processed:</strong> Raw data → Transformed → PostgreSQL</p>
        """,
    )


# Set task dependencies
(
    setup_data_dir
    >> scrape_real_estate_data
    >> note
    >> clean_data
    >> note_clean_data
    >> upload_cleaned_data_to_postgres
    >> success_email
)
