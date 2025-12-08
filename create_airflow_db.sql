-- create airflow db (if you need it for Airflow metadata)
CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO db_user;

CREATE DATABASE docker_real_estate;
GRANT ALL PRIVILEGES ON DATABASE docker_real_estate TO db_user;

