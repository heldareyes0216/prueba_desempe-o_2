import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SOURCE_FILENAME = "online_retail.csv"


def get_data_path():
    """Return the folder containing the source CSV."""
    return Variable.get(
        "RAW_DATA_PATH",
        default_var="/opt/airflow/data/",
    )


def get_db_engine():
    """Connect to the data warehouse using Airflow's connection."""
    connection = BaseHook.get_connection("dwh_db")
    uri = connection.get_uri()
    
    # Parche obligatorio para SQLAlchemy 2.0+
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
        
    return create_engine(uri)


def normalize_columns(df):
    """Normalize column names and remove surrounding spaces."""
    df.columns = [
        str(column).strip().replace(" ", "").upper()
        for column in df.columns
    ]
    return df


# DAG DEFINITION
@dag(
    dag_id="datamart_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline for the Online Retail data warehouse",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "datamart", "retail"],
)
def datamart_etl_pipeline():

    # TASK 1: EXTRACT AND TRANSFORM
    @task()
    def extract_and_transform():
        data_path = get_data_path()
        source_file = os.path.join(data_path, SOURCE_FILENAME)

        if not os.path.exists(source_file):
            raise FileNotFoundError(
                f"Source CSV not found: {source_file}"
            )

        # Read the original CSV.
        df = pd.read_csv(
            source_file,
            encoding="ISO-8859-1",
            low_memory=False,
        )

        df = normalize_columns(df)

        required_columns = [
            "INVOICENO",
            "STOCKCODE",
            "DESCRIPTION",
            "QUANTITY",
            "INVOICEDATE",
            "UNITPRICE",
            "CUSTOMERID",
            "COUNTRY",
        ]

        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

        # Keep the original row number for audit purposes.
        df["source_row"] = df.index + 2

        # Normalize data types.
        df["INVOICENO"] = df["INVOICENO"].astype("string").str.strip()
        df["STOCKCODE"] = df["STOCKCODE"].astype("string").str.strip()
        df["DESCRIPTION"] = df["DESCRIPTION"].astype("string").str.strip()
        df["COUNTRY"] = df["COUNTRY"].astype("string").str.strip()

        df["QUANTITY"] = pd.to_numeric(
            df["QUANTITY"], errors="coerce"
        )
        df["UNITPRICE"] = pd.to_numeric(
            df["UNITPRICE"], errors="coerce"
        )
        df["CUSTOMERID"] = pd.to_numeric(
            df["CUSTOMERID"], errors="coerce"
        )
        df["INVOICEDATE"] = pd.to_datetime(
            df["INVOICEDATE"], errors="coerce"
        )

        # Separate invalid records for the rejection log.
        invalid = (
            df["INVOICENO"].isna()
            | df["INVOICENO"].eq("")
            | df["STOCKCODE"].isna()
            | df["STOCKCODE"].eq("")
            | df["QUANTITY"].isna()
            | df["UNITPRICE"].isna()
            | df["INVOICEDATE"].isna()
            | df["QUANTITY"].eq(0)
            | df["UNITPRICE"].lt(0)
        )

        rejected = df.loc[invalid].copy()
        rejected["reject_reason"] = "Missing or invalid required data"

        valid = df.loc[~invalid].copy()

        # Convert quantities to integers where possible.
        valid = valid.loc[
            valid["QUANTITY"] % 1 == 0
        ].copy()
        valid["QUANTITY"] = valid["QUANTITY"].astype("int64")

        # Create the product dimension.
        products = (
            valid.groupby("STOCKCODE", as_index=False)
            .agg(
                description=("DESCRIPTION", "first"),
                avg_unit_price=("UNITPRICE", "mean"),
            )
            .rename(columns={"STOCKCODE": "stock_code"})
        )

        # Create the customer dimension.
        customer_rows = valid.loc[
            valid["CUSTOMERID"].notna()
        ].copy()

        customers = (
            customer_rows.groupby("CUSTOMERID", as_index=False)
            .agg(country=("COUNTRY", "first"))
            .rename(columns={"CUSTOMERID": "customer_id"})
        )

        customers["customer_id"] = customers["customer_id"].astype("int64")

        # Returns are identified by a cancellation invoice or
        # a negative quantity.
        is_return = (
            valid["INVOICENO"].str.upper().str.startswith("C", na=False)
            | valid["QUANTITY"].lt(0)
        )

        sales = valid.loc[
            (~is_return) & valid["QUANTITY"].gt(0)
        ].copy()

        returns = valid.loc[is_return].copy()

        def prepare_fact(frame):
            result = pd.DataFrame({
                "invoice_no": frame["INVOICENO"].astype(str),
                "stock_code": frame["STOCKCODE"].astype(str),
                "customer_id": frame["CUSTOMERID"],
                "invoice_date": frame["INVOICEDATE"],
                "quantity": frame["QUANTITY"].abs().astype("int64"),
                "unit_price": frame["UNITPRICE"],
                "line_total": (
                    frame["QUANTITY"].abs() * frame["UNITPRICE"]
                ),
                "country": frame["COUNTRY"],
            })
            result["customer_id"] = result["customer_id"].astype("Int64")
            return result

        fact_sales = prepare_fact(sales)
        fact_returns = prepare_fact(returns)

        # Store intermediate results in the shared data folder.
        output_path = os.path.join(data_path, "processed")
        os.makedirs(output_path, exist_ok=True)

        outputs = {
            "dim_customers": customers,
            "dim_products": products,
            "fact_sales": fact_sales,
            "fact_returns": fact_returns,
            "log_rejections": rejected[
                ["source_row", "INVOICENO", "reject_reason"]
            ].rename(columns={"INVOICENO": "invoice_no"}),
        }

        output_files = {}

        for name, dataframe in outputs.items():
            file_path = os.path.join(output_path, f"{name}.csv")
            dataframe.to_csv(file_path, index=False)
            output_files[name] = file_path

        print(f"Valid rows: {len(valid)}")
        print(f"Rejected rows: {len(rejected)}")
        print(f"Sales rows: {len(fact_sales)}")
        print(f"Return rows: {len(fact_returns)}")

        return output_files


    # TASK 2: LOAD DATA WAREHOUSE
    # TASK 2: LOAD DATA WAREHOUSE
    @task()
    def load_dwh(output_files):
        engine = get_db_engine()

        try:
            # 1. Limpiar las tablas (Idempotencia) respetando las dependencias
            with engine.begin() as connection:
                for table_name in output_files.keys():
                    connection.exec_driver_sql(f"TRUNCATE TABLE {table_name} CASCADE;")

            # 2. Insertar los datos limpios
            for table_name, file_path in output_files.items():
                dataframe = pd.read_csv(file_path)

                dataframe.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists="append",  # Cambiado de 'replace' a 'append'
                    index=False,
                    chunksize=10000,
                    method="multi",
                )

                print(f"Loaded {len(dataframe)} rows into {table_name}")

        finally:
            engine.dispose()

    # TASK DEPENDENCIES
    transformed_files = extract_and_transform()
    loading = load_dwh(transformed_files)

    transformed_files >> loading


# IMPORTANT: instantiate the DAG outside the function.
dag_instance = datamart_etl_pipeline()