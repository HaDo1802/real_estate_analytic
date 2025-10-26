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
    os.path.join(os.path.dirname(__file__), "..", "data", "transformed_real_estate_data.csv")
)
DEFAULT_SCHEMA = os.getenv("DEFAULT_SCHEMA", "real_estate_data")
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "properties_prices")
DEFAULT_MODE = os.getenv("DEFAULT_MODE", "truncate")

def get_connection():
    """Connect to Postgres using .env credentials."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "zillow_api"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "dovanhadovanha"),
        port=os.getenv("POSTGRES_PORT", 5432),
    )

def ensure_schema_and_table(csv_file, schema, table, conn):
    """Create schema/table if they don't exist."""
    df = pd.read_csv(csv_file, nrows=1)
    cur = conn.cursor()

    # Ensure schema
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

    # Ensure table
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema=%s AND table_name=%s
        )
    """, (schema, table))
    exists = cur.fetchone()[0]

    if not exists:
        logger.info(f"Creating table {schema}.{table}...")
        columns = ", ".join([f"{col} TEXT" for col in df.columns])
        create_sql = sql.SQL("CREATE TABLE {}.{} (" + columns + ")").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        cur.execute(create_sql)
        conn.commit()
        logger.info(f"✅ Created {schema}.{table}")
    cur.close()

def load_csv_to_postgres(csv_file=DEFAULT_FILE, schema=DEFAULT_SCHEMA, 
                         table=DEFAULT_TABLE, mode=DEFAULT_MODE):
    """Load CSV into Postgres with truncate or append."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        ensure_schema_and_table(csv_file, schema, table, conn)

        if mode == "truncate":
            logger.info(f"Truncating {schema}.{table}...")
            cur.execute(sql.SQL("TRUNCATE TABLE {}.{}")
                        .format(sql.Identifier(schema), sql.Identifier(table)))

        logger.info(f"Loading {csv_file} into {schema}.{table} [mode={mode}]...")
        with open(csv_file, "r", encoding="utf-8") as f:
            cur.copy_expert(
                sql.SQL("COPY {}.{} FROM STDIN WITH CSV HEADER DELIMITER ','")
                .format(sql.Identifier(schema), sql.Identifier(table)),
                f
            )

        conn.commit()
        logger.info(f"✅ Load complete: {schema}.{table}")

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Load failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def load_records_to_postgres(records: list[dict], schema=DEFAULT_SCHEMA, 
                         table=DEFAULT_TABLE, mode=DEFAULT_MODE):
    if not records:
        logger.warning("No records to load.")
    df = pd.DataFrame(records)
    # tmp_file = "tmp_records.csv"
    # df.to_csv(tmp_file, index=False)
    load_csv_to_postgres(DEFAULT_FILE, schema, table, mode)
    
if __name__ == "__main__":
    load_csv_to_postgres()

