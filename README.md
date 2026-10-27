# DataMart S.A.S - ETL Data Pipeline

This repository contains an automated ETL pipeline built with Apache Airflow, Docker, and PostgreSQL. It extracts e-commerce transaction data, applies business rules and data quality transformations, and loads the cleaned data into an analytical Star Schema Data Warehouse.


## Quick Start Guide (Run in < 10 mins)

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* Git installed.

### 2. Clone the Repository
git clone <https://github.com/heldareyes0216/prueba_desempe-o_2.git>
cd prueba_desempeño_2

### 3. Environment setup
create the .env file usign the provided example:

cp .env.example .env

### 4. Deploy infrastructure
start the PostgresSQL data warehouse ande apache airflow services using docker compose:

docker compose up -d --build

Wait approximately 1-2 minutes for Airflow to initialize the database and the webserver.

### 5. Execute the Pipeline
* Open your browser and navigate to http://localhost:8081 (or 8080 depending on your port mapping).
* Log in with the credentials provided in your .env file.
* Verify Airflow Configurations:
  Connections: Go to Admin > Connections. Ensure dwh_db is configured (Host: postgres-dwh, Login: postgres, Password: postgres, Port: 5432).

  Variables: Go to Admin > Variables. Ensure RAW_DATA_PATH is set to /opt/airflow/data/.

* In the main DAGs view, unpause (toggle) the datamart_etl_pipeline.
* Click the Trigger DAG (Play) button.
* Click on the DAG name and go to the Graph tab to monitor the execution. Wait until all tasks turn dark green (Success).

### 6. Verify Analytical Data
Once the pipeline succeeds, connect to the DWH to validate the data:

docker compose exec postgres-dwh psql -U postgres -d datamart_dwh

Run validation queries located in docs/validation_queries.sql to answer business questions.