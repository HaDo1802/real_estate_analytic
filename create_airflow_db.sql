-- create airflow db (if you need it for Airflow metadata)
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO db_user;

-- create skytrx db for your pipeline
CREATE DATABASE real_estate_data;
GRANT ALL PRIVILEGES ON DATABASE real_estate_data TO db_user;

-- docker exec -it 9642f9acaded psql -U db_user -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE real_estate_data TO db_user;"