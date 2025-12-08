import os
import pandas as pd
import logging
import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from dotenv import load_dotenv

load_dotenv()
DEFAULT_FILE = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "data", "transformed_latest.csv"
    )
)
DEFAULT_SCHEMA = os.getenv("DEFAULT_SCHEMA", "real_estate_data")
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "properties_sale_prices")
DEFAULT_MODE = os.getenv("DEFAULT_MODE", "append")


def get_connection():
    """Connect to Postgres using .env credentials with environment auto-detection."""

    # Auto-detect environment and use appropriate credentials
    if os.path.exists("/opt/airflow"):
        # Running inside Docker/Airflow
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
        )
    else:
        # Running locally
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST_LOCAL", "localhost"),
            dbname=os.getenv("POSTGRES_DB_LOCAL"),
            user=os.getenv("POSTGRES_USER_LOCAL"),
            password=os.getenv("POSTGRES_PASSWORD_LOCAL"),
            port=int(os.getenv("POSTGRES_PORT_LOCAL", 5432)),
        )


def ensure_schema_and_table(csv_file, schema, table, conn):
    """Create schema/table if they don't exist with optimized column types."""
    df = pd.read_csv(csv_file, nrows=1)
    cur = conn.cursor()

    # Ensure schema
    cur.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
    )

    # Ensure table
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema=%s AND table_name=%s
        )
    """,
        (schema, table),
    )
    exists = cur.fetchone()[0]

    if not exists:
        logger.info(f"Creating table {schema}.{table} with optimized column types...")

        # Define column types for better performance and visualization
        column_types = {
            "zillow_property_id": "BIGINT",
            "street_address": "TEXT",
            "city": "TEXT",
            "vegas_district": "TEXT",
            "zip_code": "TEXT",
            "latitude": "DOUBLE PRECISION",
            "longitude": "DOUBLE PRECISION",
            "livingArea": "DOUBLE PRECISION",
            "Normalized_lotAreaValue": "DOUBLE PRECISION",
            "bathrooms": "DOUBLE PRECISION",
            "bedrooms": "INTEGER",
            "price": "BIGINT",
            "rentZestimate": "DOUBLE PRECISION",
            "zestimate": "DOUBLE PRECISION",
            "propertyType": "TEXT",
            "Unit": "TEXT",
            "daysOnZillow": "INTEGER",
            "date_listing": "TIMESTAMPTZ",
            "datePriceChanged": "TIMESTAMPTZ",
            "listingStatus": "TEXT",
            "is_fsba": "BOOLEAN",
            "is_open_house": "BOOLEAN",
            "processed_at": "TIMESTAMPTZ NOT NULL",
            "etl_run_id": "TEXT NOT NULL"
        }

        # Build column definitions
        column_definitions = []
        for col in df.columns:
            col_type = column_types.get(col, "TEXT")
            column_definitions.append(f'"{col}" {col_type}')

        columns_sql = ", ".join(column_definitions)

        # Add primary key constraint on zillow_property_id to prevent duplicates
        # Use ON CONFLICT DO NOTHING for graceful duplicate handling
        create_sql = sql.SQL(
            "CREATE TABLE {}.{} (" + columns_sql + ", UNIQUE (zillow_property_id,etl_run_id))"
        ).format(sql.Identifier(schema), sql.Identifier(table))
        cur.execute(create_sql)

        # # Add indexes for better query performance
        # logger.info(f"Creating indexes for {schema}.{table}...")
        # indexes = [
        #     f"CREATE INDEX IF NOT EXISTS idx_{table}_price ON {schema}.{table} (price)",
        #     f"CREATE INDEX IF NOT EXISTS idx_{table}_location ON {schema}.{table} (vegas_district)",
        #     f"CREATE INDEX IF NOT EXISTS idx_{table}_category ON {schema}.{table} (price_category)",
        #     f'CREATE INDEX IF NOT EXISTS idx_{table}_property_type ON {schema}.{table} ("propertyType")',
        #     f'CREATE INDEX IF NOT EXISTS idx_{table}_listing_status ON {schema}.{table} ("listingStatus")',
        #     f"CREATE INDEX IF NOT EXISTS idx_{table}_processed_at ON {schema}.{table} (processed_at)",
        # ]

        # for index_sql in indexes:
        #     try:
        #         cur.execute(index_sql)
        #     except Exception as e:
        #         logger.warning(f"Could not create index: {e}")

        conn.commit()
        logger.info(f"✅ Created {schema}.{table} with optimized schema and indexes")
    cur.close()


def load_csv(csv_file=DEFAULT_FILE):
    conn = get_connection()
    cur = conn.cursor()

    try:
        ensure_schema_and_objects(conn)

        df = pd.read_csv(csv_file)[ORDERED_COLS]

        # ✅ minimal cleanup only
        df = df.where(pd.notna(df), None)

        tmp_file = "/tmp/property_history_load.csv"
        df.to_csv(tmp_file, index=False)

        logger.info("COPYing data into history table...")

        with open(tmp_file, "r", encoding="utf-8") as f:
            cur.copy_expert(
                sql.SQL(
                    "COPY {}.{} FROM STDIN WITH CSV HEADER"
                ).format(
                    sql.Identifier(DEFAULT_SCHEMA),
                    sql.Identifier(HISTORY_TABLE)
                ),
                f,
            )

        conn.commit()
        logger.info("✅ COPY completed")

    except Exception as e:
        conn.rollback()
        logger.exception("❌ Load failed")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Environment detection and validation
    is_docker = os.path.exists("/opt/airflow")
    env_name = "Docker/Airflow" if is_docker else "Local"

    logger.info(f" Running in {env_name} environment")

    # Validate required environment variables based on environment
    if is_docker:
        required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
        config_info = {
            "Host": os.getenv("POSTGRES_HOST", "postgres"),
            "Database": os.getenv("POSTGRES_DB"),
            "User": os.getenv("POSTGRES_USER"),
            "Port": os.getenv("POSTGRES_PORT", "5432"),
        }
    else:
        required_vars = [
            "POSTGRES_DB_LOCAL",
            "POSTGRES_USER_LOCAL",
            "POSTGRES_PASSWORD_LOCAL",
        ]
        config_info = {
            "Host": os.getenv("POSTGRES_HOST_LOCAL", "localhost"),
            "Database": os.getenv("POSTGRES_DB_LOCAL"),
            "User": os.getenv("POSTGRES_USER_LOCAL"),
            "Port": os.getenv("POSTGRES_PORT_LOCAL", "5432"),
        }

    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f" Missing required environment variables: {missing_vars}")
        logger.error("Please check your .env file")
        exit(1)

    logger.info(f" Configuration:")
    logger.info(f" Environment: {env_name}")
    for key, value in config_info.items():
        logger.info(f" {key}: {value}")
    logger.info(f" Schema: {DEFAULT_SCHEMA}")
    logger.info(f" Table: {DEFAULT_TABLE}")
    logger.info(f" File: {DEFAULT_FILE}")

    try:
        logger.info(" Testing database connection...")
        conn = get_connection()
        conn.close()
        logger.info(" Database connection successful!")

        logger.info(" Loading data to PostgreSQL...")
        load_csv_to_postgres()
        logger.info(" Data load completed successfully!")

    except Exception as e:
        logger.error(f" Error: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        exit(1)