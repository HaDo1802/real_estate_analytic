# Real Estate Data Pipeline

This project implements a Extract-Transform-Load (ETL) pipeline for real estate listings data, designed to process and analyze real-time property listings. The pipeline extracts property data from Zillow API, leveraging Apache Airflow, PostgreSQL, and Docker to automate data collection and storage for downstream analytics and machine learning applications.

---

## 🗂 Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   └── real_estate_etl_dag.py # Main pipeline workflow
├── etl/                       # ETL modules
│   ├── extract.py            # API data extraction
│   ├── transform.py          # Data cleaning & enrichment
│   ├── load.py               # PostgreSQL loading
│   ├── main_etl.py           # Pipeline orchestrator
│   └── email_notifier.py     # Script for customized email
├── data/                      # Data storage (gitignored)
│   ├── raw/                  # Extracted data snapshots
│   └── transformed/          # Cleaned data ready for loading
├── docker/
│   ├── docker-compose.yaml   # Service orchestration
│   └── Dockerfile            # Custom Airflow image
├── .env                      
├── requirements.txt          # Packages dependencies
└── README.md                 
```

---

## ⚙️ Technology Stack

- **Data Source**: Zillow API (RapidAPI)
- **Data Processing**: Python 3.9+ with Pandas, NumPy
- **Workflow Orchestration**: Apache Airflow 
- **Data Warehouse**: PostgreSQL 13
- **Containerization**: Docker
- **Email Notifications**: SMTP (Gmail)

---

## 🧱 Data Architecture

### 1. Data Source
The project processes real estate listings from the **Zillow API** via [RapidAPI](https://rapidapi.com/apimaker/api/zillow-com1/playground), focusing on the Las Vegas market with plans to expand to additional locations. Current extraction targets multiple neighborhoods including **Summerlin, Henderson, Downtown Las Vegas, and surrounding areas**.

### 2. Data Processing Pipeline

#### **Data Extraction**
- Automatically fetch property listings from Zillow API via RapidAPI
- Extract comprehensive property details including property idetification, prices, location, and other data
- Store raw data with timestamps in `raw_data_YYYYMMDD.csv`, allowing audit tracing for daily run
- Features intelligent pagination and rate limiting for API compliance
- Multi-location support with configurable location list

#### **Data Transformation & Cleaning**
- Parse and standardize address components (street, city, state, zip)
- Normalize lot area measurements (acres to sqft conversion)
- Calculate derived fields (listing dates, district classification)
- Extract listing features (FSBA status, open house indicators)
- Handle missing values and validate data quality
- Generate cleaned dataset with consistent schema stored in `transformed_YYYYMMDD.csv`

#### **Database Loading**
- Load cleaned data into PostgreSQL using efficient COPY operations, allowing bulk insert without burden on worrying data type mismatch configuration
- Implement snapshot-based historical tracking strategy
- Store all records in `properties_data_history` table (append-only)
- Maintain current view via `properties_data_current` (latest snapshot per property)
- Support incremental loading for continuous data updates
- Enable time-series analysis and price tracking

### 3. Data Quality Framework
- Essential field validation (property ID, etl_run_id) use for duplicate detection and removal
- Pipeline monitoring via email notifications
- PostgreSQL data quality checks post-load

---

## 🚀 Project Components

### 📊 Airflow DAGs
Located in `dags/`:

- **Pipeline orchestration** for automated data collection every day at 6 AM
- **Task scheduling** with dependency management
- **Retry logic** for fault-tolerant execution
- **Email notifications** on success/failure
- **Execution tracking** via Airflow web UI (port 8080)

### 🛠 ETL Modules
Located in `etl/`:

- **extract.py**: Multi-location API scraper with pagination
- **transform.py**: Data cleaning, feature engineering, validation
- **load.py**: PostgreSQL COPY operations with environment auto-detection
- **main_etl.py**: Standalone ETL runner for manual execution
- **email_notifier.py**: SMTP notification service with HTML templates

### 🗄 Database Schema
**History Table** (append-only):
```sql
CREATE TABLE real_estate_data.properties_data_history (
    zillow_property_id BIGINT,
    street_address TEXT,
    city TEXT,
    vegas_district TEXT,
    zip_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    livingArea DOUBLE PRECISION,
    Normalized_lotAreaValue DOUBLE PRECISION,
    bathrooms DOUBLE PRECISION,
    bedrooms INTEGER,
    price BIGINT,
    rentZestimate DOUBLE PRECISION,
    zestimate DOUBLE PRECISION,
    propertyType TEXT,
    Unit TEXT,
    daysOnZillow INTEGER,
    date_listing TIMESTAMPTZ,
    datePriceChanged TIMESTAMPTZ,
    listingStatus TEXT,
    is_fsba BOOLEAN,
    is_open_house BOOLEAN,
    processed_at TIMESTAMPTZ NOT NULL,
    etl_run_id TEXT NOT NULL
);
```

**Current View** (latest snapshot):
```sql
CREATE VIEW real_estate_data.properties_data_current AS
SELECT DISTINCT ON (zillow_property_id) *
FROM real_estate_data.properties_data_history
ORDER BY zillow_property_id, processed_at DESC;
```

---

## 📦 Key Dependencies

- **pandas==1.5.0** - Data processing and transformation
- **numpy==1.24.0** - Numerical operations
- **requests==2.28.0** - HTTP library for API calls
- **psycopg2-binary==2.9.0** - PostgreSQL database adapter
- **python-dotenv==1.0.0** - Environment variable management
- **apache-airflow[postgres]==2.9.2** - Workflow orchestration
- **apache-airflow-providers-postgres>=5.0.0** - PostgreSQL integration

---

## 🚀 Key Features

### Comprehensive Data Extraction
- **Multi-location support**: Configurable list of target locations
- **Rate limiting**: API-compliant request throttling (0.2s between calls)
- **Pagination handling**: Automatic traversal of result pages
- **Error recovery**: Robust exception handling with retries


### Snapshot-Based Storage
- **Historical tracking**: Full audit trail of all property changes
- **Price history**: Track listing price changes over time
- **Point-in-time queries**: Analyze market state at any date
- **Zero data loss**: Append-only architecture prevents overwrites

### Production-Ready Operations
- **Automated scheduling**: Runs every 10 minutes via Airflow
- **Email notifications**: Success/failure alerts with execution details
- **Comprehensive logging**: Multi-level logs for debugging, documented inside <a href="file:///Users/hado/Desktop/Career/Coding/Data%20Engineer/Project/real_estate_project/etl_log/log.txt">etl_log/log.txt</a>
- **Environment flexibility**: Auto-detects Docker vs local execution
- **Containerized deployment**: Docker Compose for consistent environments
- **Centralize Configuration**: Leveraging modular logger and .env variables configuration, making it easier to scale and ensure safety
---

## 🎯 Design Decisions

### Why Snapshot-Based Storage?
Traditional upsert strategies overwrite historical data, losing valuable time-series information. This pipeline uses **append-only history** with a **current view** to enable:
- Price trend analysis over time
- Market velocity metrics (average days to sale)
- Point-in-time market snapshots
- Complete audit trail for compliance

### Why Airflow Over Cron?
- **Visual monitoring**: Web UI for pipeline status and logs
- **Dependency management**: Task execution order enforcement
- **Retry logic**: Automatic failure recovery with backoff
- **Scalability**: Easy migration to distributed execution
- **Extensibility**: Rich ecosystem of providers and operators

### Why PostgreSQL?
- **ACID compliance**: Data integrity guarantees
- **Rich data types**: JSONB, arrays, geospatial support
- **Performance**: Optimized for analytical queries
- **Cost**: Open-source with enterprise features
- **Integration**: Native Airflow support
- **Future Consideration**: As data grow, I will move to cloud-based solution such as Snowflake, S3,..
---