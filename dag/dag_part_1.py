import os
import pandas as pd
import numpy as np
import json
import logging
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'huyduc-bde-a3',
    'start_date': datetime.now() - timedelta(days=1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='bronze_load',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
AIRBNB = AIRFLOW_DATA + "/airbnb/"
CENSUS = AIRFLOW_DATA + "/census/"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################
def ensure_archive(dir_path):
    archive_folder = os.path.join(dir_path, "archive")
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    return archive_folder


def load_airbnb_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    if not os.path.exists(AIRBNB):
        logging.info(f"No AIRBNB directory found: {AIRBNB}")
        return None

    files = sorted([f for f in os.listdir(AIRBNB) if f.endswith(".csv")])
    if len(files) == 0:
        logging.info("No Airbnb CSV files found.")
        return None

    archive_folder = ensure_archive(AIRBNB)

    expected_cols = [
        "listing_id","scrape_id","scraped_date","host_id","host_name","host_since",
        "host_is_superhost","host_neighbourhood","listing_neighbourhood","property_type",
        "room_type","accommodates","price","has_availability","availability_30",
        "number_of_reviews","review_scores_rating","review_scores_accuracy",
        "review_scores_cleanliness","review_scores_checkin",
        "review_scores_communication","review_scores_value","source_file"
    ]

    # Generate dataframe by reading the CSV file
    for file in files:
        file_path = os.path.join(AIRBNB, file)
        df = pd.read_csv(file_path)

        if len(df) == 0:
            continue

        # Normalize headers to lowercase
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Add source_file column
        df["source_file"] = file

        # Keep only expected columns
        for c in expected_cols:
            if c not in df.columns:
                df[c] = np.nan
        df = df[expected_cols]

        for col in ["scraped_date", "host_since"]:
            if col in df.columns:
                df[col] = (
                    pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                    .dt.strftime("%Y-%m-%d")
                )
        df = df.replace({pd.NaT: None, "NaT": None})

        values = df.to_dict("split")["data"]

        insert_sql = """
            INSERT INTO bronze.airbnb_listings_raw(
                listing_id,scrape_id,scraped_date,host_id,host_name,host_since,
                host_is_superhost,host_neighbourhood,listing_neighbourhood,property_type,
                room_type,accommodates,price,has_availability,availability_30,
                number_of_reviews,review_scores_rating,review_scores_accuracy,
                review_scores_cleanliness,review_scores_checkin,
                review_scores_communication,review_scores_value,source_file
            ) VALUES %s
        """

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        shutil.move(file_path, os.path.join(archive_folder, file))
        logging.info(f"Loaded and archived {file}")
    return None


def load_census_g01_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    g01_path = os.path.join(CENSUS, "2016Census_G01_NSW_LGA.csv")
    if not os.path.exists(g01_path):
        logging.info("Census G01 file not found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(g01_path)
    if len(df) == 0:
        return None

    code_col = next((c for c in df.columns if c.lower().startswith("lga_code")), None)
    if not code_col:
        raise ValueError("No LGA code column found in G01.")

    values = []
    for _, row in df.iterrows():
        lga_code = str(row[code_col])
        payload = row.drop([code_col]).dropna().to_dict()
        values.append((lga_code, json.dumps(payload), "2016Census_G01_NSW_LGA.csv"))

    insert_sql = """
        INSERT INTO bronze.census_g01_raw (lga_code, payload, source_file)
        VALUES %s
    """

    execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df),
                   template="(%s,%s::jsonb,%s)")
    conn_ps.commit()

    # Move the processed file to the archive folder
    shutil.move(g01_path, os.path.join(ensure_archive(CENSUS), "2016Census_G01_NSW_LGA.csv"))
    return None


def load_census_g02_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    g02_path = os.path.join(CENSUS, "2016Census_G02_NSW_LGA.csv")
    if not os.path.exists(g02_path):
        logging.info("Census G02 file not found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(g02_path)
    if len(df) == 0:
        return None

    code_col = next((c for c in df.columns if c.lower().startswith("lga_code")), None)

    values = []
    for _, row in df.iterrows():
        lga_code = str(row[code_col])
        payload = row.drop([code_col]).dropna().to_dict()
        values.append((lga_code, json.dumps(payload), "2016Census_G02_NSW_LGA.csv"))

    insert_sql = """
        INSERT INTO bronze.census_g02_raw (lga_code, payload, source_file)
        VALUES %s
    """

    execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df),
                   template="(%s,%s::jsonb,%s)")
    conn_ps.commit()

    # Move the processed file to the archive folder
    shutil.move(g02_path, os.path.join(ensure_archive(CENSUS), "2016Census_G02_NSW_LGA.csv"))
    return None


def load_lga_code_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    lga_code_path = os.path.join(CENSUS, "NSW_LGA_CODE.csv")
    if not os.path.exists(lga_code_path):
        logging.info("NSW_LGA_CODE.csv not found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(lga_code_path)
    code_col = next((c for c in df.columns if c.lower().startswith("lga_code")), None)
    name_col = next((c for c in df.columns if c.lower().startswith("lga_name")), None)

    df = df[[code_col, name_col]].rename(columns={code_col: "lga_code", name_col: "lga_name"})
    df["source_file"] = "NSW_LGA_CODE.csv"

    values = df[["lga_code", "lga_name", "source_file"]].to_dict('split')['data']

    insert_sql = """
        INSERT INTO bronze.nsw_lga_code_raw (lga_code, lga_name, source_file)
        VALUES %s
    """

    execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
    conn_ps.commit()

    # Move the processed file to the archive folder
    shutil.move(lga_code_path, os.path.join(ensure_archive(CENSUS), "NSW_LGA_CODE.csv"))
    return None


def load_lga_suburb_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    suburb_path = os.path.join(CENSUS, "NSW_LGA_SUBURB.csv")
    if not os.path.exists(suburb_path):
        logging.info("NSW_LGA_SUBURB.csv not found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(suburb_path)
    keep = [c for c in df.columns if not str(c).lower().startswith("unnamed")]
    df = df[keep]

    name_map = {}
    for c in df.columns:
        lc = c.lower()
        if "lga" in lc and "name" in lc:
            name_map[c] = "lga_name"
        elif "suburb" in lc:
            name_map[c] = "suburb_name"
    df = df.rename(columns=name_map)
    df = df[["lga_name", "suburb_name"]]
    df["source_file"] = "NSW_LGA_SUBURB.csv"

    values = df[["lga_name", "suburb_name", "source_file"]].to_dict('split')['data']

    insert_sql = """
        INSERT INTO bronze.nsw_lga_suburb_raw (lga_name, suburb_name, source_file)
        VALUES %s
    """

    execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
    conn_ps.commit()

    # Move the processed file to the archive folder
    shutil.move(suburb_path, os.path.join(ensure_archive(CENSUS), "NSW_LGA_SUBURB.csv"))
    return None

#########################################################
#
#   DAG Operator Setup
#
#########################################################


load_airbnb_task = PythonOperator(
    task_id="load_airbnb_id",
    python_callable=load_airbnb_func,
    provide_context=True,
    dag=dag
)

load_census_g01_task = PythonOperator(
    task_id="load_census_g01_id",
    python_callable=load_census_g01_func,
    provide_context=True,
    dag=dag
)

load_census_g02_task = PythonOperator(
    task_id="load_census_g02_id",
    python_callable=load_census_g02_func,
    provide_context=True,
    dag=dag
)

load_lga_code_task = PythonOperator(
    task_id="load_lga_code_id",
    python_callable=load_lga_code_func,
    provide_context=True,
    dag=dag
)

load_lga_suburb_task = PythonOperator(
    task_id="load_lga_suburb_id",
    python_callable=load_lga_suburb_func,
    provide_context=True,
    dag=dag
)


load_airbnb_task >> [load_census_g01_task, load_census_g02_task, load_lga_code_task, load_lga_suburb_task]



