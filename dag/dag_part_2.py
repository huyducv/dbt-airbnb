from __future__ import annotations
import os
import pandas as pd
from typing import List, Tuple
import numpy as np
import json
import logging, io, re
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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
    dag_id='load_full_monthly_data',
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
BUCKET = "australia-southeast1-bde-74ad9458-bucket"
AIRBNB_PREFIX = "data/airbnb/"
ARCHIVE_PREFIX = "data/airbnb/archive/" 
TABLE = "bronze.airbnb_listings_raw"

#########################################################
#
#   Helpers Functions
#
#########################################################
def list_month_objects(gcs: GCSHook) -> List[str]:
    objs = gcs.list(bucket_name=BUCKET, prefix=AIRBNB_PREFIX) or []
    pat = re.compile(r".*/(\d{2})_(\d{4})\.csv$", re.IGNORECASE)
    parsed = []
    for o in objs:
        if "/archive/" in o.lower():      # <-- skip archived files
            continue
        m = pat.match(o)
        if m:
            mm, yyyy = int(m.group(1)), int(m.group(2))
            parsed.append((yyyy, mm, o))
    parsed.sort()
    return [o for (_, _, o) in parsed]

def delete_by_source(cur, table: str, source: str):
    """Idempotent load — delete rows for this source_file if they exist."""
    cur.execute(f"DELETE FROM {table} WHERE source_file=%s;", (source,))

def move_to_archive(gcs: GCSHook, source_obj: str):
    file_name = source_obj.split("/")[-1]
    dest_obj = f"{ARCHIVE_PREFIX}{file_name}"
    gcs.copy(
        source_bucket=BUCKET,
        source_object=source_obj,
        destination_bucket=BUCKET,
        destination_object=dest_obj,
    )
    gcs.delete(bucket_name=BUCKET, object_name=source_obj)
    logging.info(f"Archived {source_obj} -> {dest_obj}")


#########################################################
#
#   Custom Logics for Operator
#
#########################################################
def load_all_months(**_):
    gcs = GCSHook()
    objs = list_month_objects(gcs)
    if not objs:
        logging.warning(f"No monthly files found in gs://{BUCKET}/{AIRBNB_PREFIX}")
        return

    pg = PostgresHook(postgres_conn_id="postgres")
    conn = pg.get_conn()
    cur = conn.cursor()

    expected_cols = [
        "listing_id","scrape_id","scraped_date","host_id","host_name","host_since",
        "host_is_superhost","host_neighbourhood","listing_neighbourhood","property_type",
        "room_type","accommodates","price","has_availability","availability_30",
        "number_of_reviews","review_scores_rating","review_scores_accuracy",
        "review_scores_cleanliness","review_scores_checkin",
        "review_scores_communication","review_scores_value","source_file"
    ]

    for obj in objs:
        logging.info(f"Processing {obj} ...")
        data = gcs.download_as_byte_array(bucket_name=BUCKET, object_name=obj)
        df = pd.read_csv(io.BytesIO(data))
        if df.empty:
            logging.info(f"{obj} is empty — archiving and skipping.")
            move_to_archive(gcs, obj)
            continue

        # normalize headers
        df.columns = [c.strip().lower() for c in df.columns]
        if "neighbourhood_cleansed" in df.columns and "listing_neighbourhood" not in df.columns:
            df = df.rename(columns={"neighbourhood_cleansed": "listing_neighbourhood"})

        df["source_file"] = obj

        # align all expected columns
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None
        df = df[expected_cols]

        # cast date columns
        for dcol in ["scraped_date","host_since"]:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date

        # delete previous load of same file
        delete_by_source(cur, TABLE, obj)
        conn.commit()

        # insert data
        insert_sql = f"""
            INSERT INTO {TABLE}(
                listing_id,scrape_id,scraped_date,host_id,host_name,host_since,
                host_is_superhost,host_neighbourhood,listing_neighbourhood,property_type,
                room_type,accommodates,price,has_availability,availability_30,
                number_of_reviews,review_scores_rating,review_scores_accuracy,
                review_scores_cleanliness,review_scores_checkin,
                review_scores_communication,review_scores_value,source_file
            ) VALUES %s
        """

        # cast date columns
        for dcol in ["scraped_date", "host_since"]:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce", dayfirst=True).dt.date

        # Replace invalid / missing values with None so psycopg2 can handle them
        df = df.replace({pd.NaT: None, np.nan: None, "NaT": None, "NaN": None})
        
        values = df.to_dict("split")["data"]
        execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

        # archive file
        move_to_archive(gcs, obj)
        logging.info(f"Loaded {len(df)} rows from {obj} and archived it")

    cur.close(); conn.close()
    logging.info("All months processed and archived successfully!")


#########################################################
#
#   DAG Operator Setup
#
#########################################################

load_airbnb_monthly = PythonOperator(
    task_id="load_airbnb_monthly",
    python_callable=load_all_months,
    provide_context=True,
    dag=dag
)



