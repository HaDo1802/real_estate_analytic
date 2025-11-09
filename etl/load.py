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
        os.path.dirname(__file__), "..", "data", "transformed_real_estate_data.csv"
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
            # ID fields
            "zillow_property_id": "BIGINT",
            # Numeric fields
            "bathrooms": "INTEGER",
            "bedrooms": "INTEGER",
            "livingArea": "DECIMAL(10,2)",
            "lotAreaValue": "DECIMAL(12,2)",
            "price": "BIGINT",
            "rentZestimate": "DECIMAL(10,2)",
            "zestimate": "DECIMAL(12,2)",
            "daysOnZillow": "INTEGER",
            # Geographic fields
            "latitude": "DECIMAL(10,7)",
            "longitude": "DECIMAL(11,7)",
            # New calculated fields
            "price_per_sqft": "DECIMAL(8,2)",
            "rent_per_sqft": "DECIMAL(8,2)",
            # Date/Time fields
            "processed_at": "TIMESTAMP WITH TIME ZONE",
            "etl_run_id": "VARCHAR(20)",
            "date_listing": "TIMESTAMP WITH TIME ZONE",
            "datePriceChanged": "TIMESTAMP",
            "comingSoonOnMarketDate": "TIMESTAMP",
            # Boolean fields
            "has3DModel": "BOOLEAN",
            "hasVideo": "BOOLEAN",
            "hasImage": "BOOLEAN",
            "is_fsba": "BOOLEAN",
            "is_open_house": "BOOLEAN",
            # Categorical fields (for indexing)
            "propertyType": "VARCHAR(50)",
            "listingStatus": "VARCHAR(30)",
            "price_category": "VARCHAR(20)",
            "vegas_district": "VARCHAR(100)",
            "city": "VARCHAR(100)",
            "state": "VARCHAR(5)",
            "zip_code": "VARCHAR(10)",
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
            "CREATE TABLE {}.{} (" + columns_sql + ", UNIQUE (zillow_property_id))"
        ).format(sql.Identifier(schema), sql.Identifier(table))
        cur.execute(create_sql)

        # Add indexes for better query performance
        logger.info(f"Creating indexes for {schema}.{table}...")
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table}_price ON {schema}.{table} (price)",
            f"CREATE INDEX IF NOT EXISTS idx_{table}_location ON {schema}.{table} (vegas_district)",
            f"CREATE INDEX IF NOT EXISTS idx_{table}_category ON {schema}.{table} (price_category)",
            f'CREATE INDEX IF NOT EXISTS idx_{table}_property_type ON {schema}.{table} ("propertyType")',
            f'CREATE INDEX IF NOT EXISTS idx_{table}_listing_status ON {schema}.{table} ("listingStatus")',
            f"CREATE INDEX IF NOT EXISTS idx_{table}_processed_at ON {schema}.{table} (processed_at)",
        ]

        for index_sql in indexes:
            try:
                cur.execute(index_sql)
            except Exception as e:
                logger.warning(f"Could not create index: {e}")

        conn.commit()
        logger.info(f"✅ Created {schema}.{table} with optimized schema and indexes")
    cur.close()


def load_csv_to_postgres(
    csv_file=DEFAULT_FILE, schema=DEFAULT_SCHEMA, table=DEFAULT_TABLE, mode=DEFAULT_MODE
):
    """Load CSV into Postgres with truncate or append."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        ensure_schema_and_table(csv_file, schema, table, conn)

        if mode == "truncate":
            logger.info(f"Truncating {schema}.{table}...")
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )

        logger.info(f"Loading {csv_file} into {schema}.{table} [mode={mode}]...")

        if mode == "append":
            # For append mode, use a more sophisticated approach to handle duplicates
            # Read CSV into pandas first to process duplicates
            df_new = pd.read_csv(csv_file)
            new_records = len(df_new)

            # Create a temporary table for new data
            temp_table = f"temp_{table}_{int(pd.Timestamp.now().timestamp())}"
            logger.info(f"Creating temp table: {temp_table}")

            # Create temp table with same structure
            temp_create_sql = sql.SQL(
                "CREATE TEMP TABLE {} AS SELECT * FROM {}.{} WHERE FALSE"
            ).format(
                sql.Identifier(temp_table),
                sql.Identifier(schema),
                sql.Identifier(table),
            )
            cur.execute(temp_create_sql)
            logger.info(f"✅ Temp table created: {temp_table}")

            # Load new data into temp table
            with open(csv_file, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    sql.SQL("COPY {} FROM STDIN WITH CSV HEADER DELIMITER ','").format(
                        sql.Identifier(temp_table)
                    ),
                    f,
                )

            # Check temp table count
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(temp_table))
            )
            result = cur.fetchone()
            temp_count = result[0] if result else 0
            logger.info(f"📊 Loaded {temp_count} records into temp table")

            # Insert only new records using ON CONFLICT for duplicate handling
            logger.info("🔍 Inserting records with duplicate handling...")
            insert_new_sql = sql.SQL(
                """
                INSERT INTO {}.{} 
                SELECT * FROM {}
                ON CONFLICT (zillow_property_id) DO NOTHING
                """
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(temp_table),
            )

            try:
                cur.execute(insert_new_sql)
                inserted_count = cur.rowcount
                logger.info(f"✅ Successfully inserted {inserted_count} new records")
            except Exception as e:
                logger.error(f"❌ Insert failed: {e}")
                # Log the problematic query for debugging
                logger.error(f"Query: {insert_new_sql}")
                raise

            skipped_count = temp_count - inserted_count

            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            result = cur.fetchone()
            total_count = result[0] if result else 0

            logger.info(f"📊 Append Summary:")
            logger.info(f"   New records in CSV: {new_records}")
            logger.info(f"   Records in temp table: {temp_count}")
            logger.info(f"   Records inserted: {inserted_count}")
            logger.info(f"   Duplicates skipped: {skipped_count}")
            logger.info(f"   Total records in table: {total_count}")

        else:
            # Original truncate mode - simple COPY
            with open(csv_file, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    sql.SQL(
                        "COPY {}.{} FROM STDIN WITH CSV HEADER DELIMITER ','"
                    ).format(sql.Identifier(schema), sql.Identifier(table)),
                    f,
                )

        conn.commit()
        logger.info(f" Load complete: {schema}.{table}")

    except Exception as e:
        conn.rollback()
        logger.error(f" Load failed: {e}")
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
