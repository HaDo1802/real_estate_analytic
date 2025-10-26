# Real Estate ETL Pipeline

A complete Extract, Transform, and Load (ETL) pipeline for real estate data from the Zillow API to PostgreSQL database.

## 🏗️ Project Structure

```
real_estate_project/
├── etl/
│   ├── extract.py          # Data extraction from Zillow API
│   ├── transform.py        # Data transformation and cleaning
│   ├── load.py            # PostgreSQL database loading
│   ├── main.py            # Main ETL pipeline orchestrator
│   └── notebook.ipynb     # Jupyter notebook for exploration and testing
├── requirement.txt        # Python dependencies
└── README.md             # This file
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirement.txt
```

### 2. Set Up PostgreSQL

Make sure PostgreSQL is running on your local machine and create a database:

```sql
CREATE DATABASE real_estate;
```

### 3. Configure Environment

Set your PostgreSQL password as an environment variable:

```bash
export POSTGRES_PASSWORD="your_password_here"
```

### 4. Run the ETL Pipeline

```python
from etl.main import run_etl_pipeline

# Run the complete pipeline
success = run_etl_pipeline(
    location="Los Angeles, CA",
    status_type="ForSale",
    home_type="Houses"
)
```

## 📊 Database Schema

The pipeline creates a `real_estate_properties` table with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `zillow_property_id` | BIGINT | Unique Zillow property identifier |
| `street_address` | VARCHAR(255) | Property street address |
| `city` | VARCHAR(100) | City |
| `state` | VARCHAR(50) | State |
| `zip_code` | VARCHAR(20) | ZIP code |
| `price` | DECIMAL(12,2) | Property price |
| `zestimate` | DECIMAL(12,2) | Zillow's estimated value |
| `rent_zestimate` | DECIMAL(12,2) | Estimated rental value |
| `bathrooms` | INTEGER | Number of bathrooms |
| `bedrooms` | INTEGER | Number of bedrooms |
| `living_area_sqft` | DECIMAL(10,2) | Living area in square feet |
| `lot_area_sqft` | DECIMAL(12,2) | Lot area in square feet |
| `latitude` | DECIMAL(10,8) | Property latitude |
| `longitude` | DECIMAL(11,8) | Property longitude |
| `property_type` | VARCHAR(50) | Type of property |
| `listing_status` | VARCHAR(50) | Listing status |
| `days_on_zillow` | INTEGER | Days listed on Zillow |
| `is_fsba` | BOOLEAN | For Sale by Agent flag |
| `is_open_house` | BOOLEAN | Open house availability |
| `country` | VARCHAR(50) | Country |
| `currency` | VARCHAR(10) | Currency |
| `processed_at` | TIMESTAMP | Data processing timestamp |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

## 🔧 Individual Module Usage

### Extract Data

```python
from etl.extract import extract_real_estate_data

data = extract_real_estate_data(
    location="San Francisco, CA",
    status_type="ForSale",
    home_type="Houses"
)
```

### Transform Data

```python
import pandas as pd
from etl.transform import transform_real_estate_data, validate_transformed_data

# Assuming you have raw data in a DataFrame
df_raw = pd.DataFrame(data['props'])
df_transformed = transform_real_estate_data(df_raw)

# Validate the transformation
is_valid = validate_transformed_data(df_transformed)
```

### Load Data

```python
from etl.load import load_real_estate_data

# Load transformed data to PostgreSQL
success = load_real_estate_data(
    df_raw=df_raw,
    db_config={
        'host': 'localhost',
        'port': 5432,
        'database': 'real_estate',
        'username': 'postgres',
        'password': 'your_password'
    }
)
```

## 🧪 Testing and Development

### Using Jupyter Notebook

1. Open the notebook:
   ```bash
   jupyter notebook etl/notebook.ipynb
   ```

2. Run the cells to see the data extraction and transformation in action

3. Test the individual components before running the full pipeline

### Database Setup Helper

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

## 🚀 Next Steps

1. **Scheduling**: Set up cron jobs or Airflow DAGs for automated runs
2. **Monitoring**: Add alerting for pipeline failures
3. **Data Quality**: Implement more sophisticated validation rules
4. **Visualization**: Connect to tools like Grafana or Tableau for analytics
5. **API Expansion**: Add support for additional real estate APIs

## 📄 License

This project is for educational and development purposes. Please respect the Zillow API terms of service.