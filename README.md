# Real Estate ETL Pipeline

A production-focused Extract, Transform, Load (ETL) pipeline that extracts real‑estate listings data using Zillow API, transforms and loads them into a PostgreSQL database for furthere analytics.

## 🏗️ Project Structure

```
real_estate_project/
├── etl/
│   ├── extract.py          # Data extraction from Zillow API
│   ├── transform.py        # Data transformation and cleaning
│   ├── load.py             # PostgreSQL database loading
│   ├── main.py             # Main ETL pipeline orchestrator (run_etl_pipeline, setup_database)
│  
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
* Some observation/logics we implement:
- DatePriceChanged is on UNIX format that need to be re-formmated
- lotArea is inconsitent with lotAreaUnit between sqft and acres
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
setup_database()  # Run once to create the schema
```

## 🔑 Configuration Options

### Database Configuration

You can customize the database connection by passing a config dictionary:

```python
db_config = {
    'host': 'localhost',        # Database host
    'port': 5432,              # Database port  
    'database': 'real_estate', # Database name
    'username': 'postgres',    # Username
    'password': 'password'     # Password
}
```

### API Parameters

Customize the data extraction:

```python
run_etl_pipeline(
    location="New York, NY",    # Search location
    status_type="ForRent",      # ForSale, ForRent, Sold
    home_type="Condos"          # Houses, Condos, Townhomes
)
```

## 📝 Logging

The pipeline creates detailed logs in `etl_pipeline.log` with information about:
- Data extraction results
- Transformation statistics
- Database operations
- Error messages and debugging info

## ⚠️ Important Notes

1. **API Key**: Make sure your Zillow API key is properly configured in the `extract.py` file
2. **Database Permissions**: Ensure your PostgreSQL user has CREATE, INSERT, and UPDATE permissions
3. **Data Validation**: The pipeline includes validation steps to ensure data quality
4. **Upserts**: The loading process uses INSERT ... ON CONFLICT to handle duplicate properties
5. **Indexes**: The schema includes optimized indexes for common query patterns

## 🔍 Troubleshooting

### Common Issues

1. **Connection Error**: Check PostgreSQL is running and credentials are correct
2. **API Limits**: The Zillow API may have rate limits - add delays if needed
3. **Memory Issues**: For large datasets, consider processing in batches
4. **Data Quality**: Check the logs for validation warnings and errors

### Useful Queries

Check loaded data:
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