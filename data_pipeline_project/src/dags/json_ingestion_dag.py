from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import shutil
from configparser import ConfigParser

# Add project root to sys.path to resolve absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main.s3.upload_to_s3 import upload_file                   # ✅ Corrected
from src.main.db.load_json_to_rds import load_json_to_rds
from src.main.db.rds_connector import DBConnection                 # ✅ Corrected

# Load config
config = ConfigParser()
config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../resources/config.ini')))

# Constants
json_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data_uploads/json_files'))
archive_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data_uploads/archive'))
json_filename = config["local"]["json_filename"]
json_path = os.path.join(json_folder, json_filename)

# DB Connection
db_connection = DBConnection(config)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='json_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Change as needed
    catchup=False,
    description='Orchestrate JSON ingestion from local to S3 to RDS',
)

def check_file_exists():
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"No JSON file found at: {json_path}")

def move_to_archive():
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    shutil.move(json_path, os.path.join(archive_folder, json_filename))

with dag:
    t1 = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists
    )

    t2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_file,
        op_args=[config]
    )

    t3 = PythonOperator(
        task_id='move_to_archive',
        python_callable=move_to_archive
    )

    t4 = PythonOperator(
        task_id='load_json_to_rds',
        python_callable=load_json_to_rds,
        op_args=[config, db_connection]
    )

    t1 >> t2 >> t3 >> t4
