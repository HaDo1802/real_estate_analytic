# Real Estate ETL Pipeline

A production-focused Extract, Transform, Load (ETL) pipeline that extracts real‑estate listings (Zillow), transforms and validates them, and loads them into a PostgreSQL database for analytics.

Table of contents
- [Project status](#project-status)
- [Repository layout](#repository-layout)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Run full pipeline](#run-full-pipeline)
  - [Run individual modules](#run-individual-modules)
- [Database schema](#database-schema)
- [Logging & monitoring](#logging--monitoring)
- [Testing & development](#testing--development)
- [Troubleshooting](#troubleshooting)
- [Roadmap / Next steps](#roadmap--next-steps)
- [Contributing](#contributing)
- [License](#license)

## Project status
Stable for development and demonstration purposes. Intended for local or controlled production use after adding production-grade secrets management, CI/CD, and monitoring.

## Repository layout
This README is tailored to the files and layout that currently exist in this repository:

```
real_estate_project/
├── etl/
│   ├── extract.py          # Data extraction from Zillow API
│   ├── transform.py        # Data transformation and cleaning
│   ├── load.py             # PostgreSQL database loading
│   ├── main.py             # Main ETL pipeline orchestrator (run_etl_pipeline, setup_database)
│   └── notebook.ipynb      # Exploration & manual testing
├── requirement.txt         # Python dependencies (note: singular filename)
└── README.md               # This file
```

> Note: The above reflects the current repository files exactly (including `requirement.txt`). If you prefer `requirements.txt` please rename and update any CI/automation accordingly.

## Quick start

Prerequisites
- Python 3.8+
- PostgreSQL (local or remote)
- Valid Zillow API credentials (or equivalent data source)

Install dependencies:
```bash
pip install -r requirement.txt
```

Create a PostgreSQL database:
```sql
CREATE DATABASE real_estate;
```

Set required environment variables (example; adapt to your environment and secret manager):
```bash
export POSTGRES_PASSWORD="your_db_password"
export ZILLOW_API_KEY="your_zillow_api_key"   # recommended; extract.py may need to be adapted to read this
```

## Configuration

Database configuration is passed as a dictionary to the loader functions. Example config used in the codebase:

```python
db_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'real_estate',
    'username': 'postgres',
    'password': 'your_password'
}
```

If `etl.extract` currently reads the Zillow API key directly from the code, switch it to read from an environment variable (recommended) or a secrets manager.

## Usage

Run the full ETL pipeline (from `etl/main.py`):

```python
from etl.main import run_etl_pipeline

# run the complete pipeline
success = run_etl_pipeline(
    location="Los Angeles, CA",
    status_type="ForSale",   # e.g., ForSale, ForRent, Sold
    home_type="Houses"       # e.g., Houses, Condos, Townhomes
)
```

Run the individual modules (examples):

- Extract:
```python
from etl.extract import extract_real_estate_data

data = extract_real_estate_data(
    location="San Francisco, CA",
    status_type="ForSale",
    home_type="Houses"
)
```

- Transform:
```python
import pandas as pd
from etl.transform import transform_real_estate_data, validate_transformed_data

df_raw = pd.DataFrame(data['props'])   # depends on extract output format
df_transformed = transform_real_estate_data(df_raw)
is_valid = validate_transformed_data(df_transformed)
```

- Load:
```python
from etl.load import load_real_estate_data

success = load_real_estate_data(
    df_raw=df_transformed,
    db_config=db_config
)
```

- Database setup helper:
```python
from etl.main import setup_database
setup_database()  # creates schema and necessary indexes (run once)
```

## Database schema

The pipeline produces a `real_estate_properties` table with the essential columns used by the codebase:

| Column               | Type               | Description |
|---------------------:|-------------------:|------------:|
| id                   | SERIAL             | Primary key |
| zillow_property_id   | BIGINT             | Unique Zillow property identifier |
| street_address       | VARCHAR(255)       | Street address |
| city                 | VARCHAR(100)       | City |
| state                | VARCHAR(50)        | State |
| zip_code             | VARCHAR(20)        | ZIP/postal code |
| price                | DECIMAL(12,2)      | Listing price |
| zestimate            | DECIMAL(12,2)      | Zillow estimate |
| rent_zestimate       | DECIMAL(12,2)      | Rent estimate |
| bathrooms            | INTEGER            | Number of bathrooms |
| bedrooms             | INTEGER            | Number of bedrooms |
| living_area_sqft     | DECIMAL(10,2)      | Living area (sqft) |
| lot_area_sqft        | DECIMAL(12,2)      | Lot area (sqft) |
| latitude             | DECIMAL(10,8)      | Latitude |
| longitude            | DECIMAL(11,8)      | Longitude |
| property_type        | VARCHAR(50)        | Property type |
| listing_status       | VARCHAR(50)        | Listing status |
| days_on_zillow       | INTEGER            | Days on market |
| is_fsba              | BOOLEAN            | For sale by agent flag |
| is_open_house        | BOOLEAN            | Open house flag |
| country              | VARCHAR(50)        | Country |
| currency             | VARCHAR(10)        | Currency code |
| processed_at         | TIMESTAMP          | Processing timestamp |
| created_at           | TIMESTAMP          | Record creation timestamp |
| updated_at           | TIMESTAMP          | Last update timestamp |

The loader uses upserts (INSERT ... ON CONFLICT) to avoid duplicate records. Indexes are created on common query columns — confirm in `etl/main.py` or `setup_database()` for exact index definitions.

## Logging & monitoring

- The pipeline emits logs to `etl_pipeline.log` (see `etl.main` for configuration).
- Logs include extraction results, transformation statistics, DB operations, and errors.
- For production, forward logs to a centralized system (e.g., ELK, Cloud Logging), and add metrics/alerts.

## Testing & development

- Use `etl/notebook.ipynb` for exploratory testing.
- Test each component independently before running the full pipeline:
  - extract -> returns raw payload
  - transform -> returns a validated DataFrame or structured payload
  - load -> persists records to PostgreSQL

Consider adding:
- Unit tests (pytest)
- Integration tests using a disposable PostgreSQL (Docker)
- Linting (flake8/black) and pre-commit hooks

## Troubleshooting

Common issues
- Connection errors: verify PostgreSQL is running, host/port/credentials are correct.
- API limits or missing credentials: ensure the Zillow API key is set and respected by `extract.py`.
- Memory pressure: process large datasets in batches rather than single large in-memory DataFrames.
- Duplicate entries: confirm upsert behavior and unique constraints in DB schema.

Useful SQL checks:
```sql
SELECT COUNT(*) FROM real_estate_properties;
SELECT DISTINCT city, state FROM real_estate_properties;
SELECT AVG(price) FROM real_estate_properties WHERE price IS NOT NULL;
```

## Roadmap / Next steps
- Add CI (tests, lint, build) and a release workflow.
- Replace any hard-coded secrets with environment variables or a secrets manager.
- Add scheduling (cron, Airflow) and observability (metrics, alerts).
- Add additional data sources and enrichments (demographics, school ratings).
- Implement more robust data quality checks and schema evolution handling.

## Contributing
1. Open an issue describing the change or feature.
2. Create a branch: git checkout -b feat/your-feature
3. Add tests and update `requirement.txt` if you add libs.
4. Open a pull request describing your changes.

## License
This project is for educational and development purposes. When using third-party APIs (e.g., Zillow) ensure you comply with their terms of service.