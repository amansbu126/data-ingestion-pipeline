from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from configparser import ConfigParser
from loguru import logger # Assuming loguru is installed and available in Airflow's env

# Import subprocess for the debug task (Crucial for environment debugging)
import subprocess

# ✅ Add root project path for custom modules
sys.path.append("/home/aman_kumar/wns_projects/data_pipeline_project")

# ✅ Project imports
from src.main.s3.upload_to_s3 import upload_file
from src.main.db.load_json_to_rds import load_json_to_rds
from src.main.db.rds_connector import PostgresConnection as DBConnection
from src.main.db.rds_table_manager import RDSTableManager

# ✅ Config loader
def get_config():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../resources/config.ini'))
    print(f"🔍 Using config file: {config_path}")
    config = ConfigParser()
    files_read = config.read(config_path)
    if not files_read:
        raise FileNotFoundError(f"❌ Config file not found at: {config_path}")
    return config

# ✅ Path resolution
def get_paths():
    config = get_config()
    json_path = os.path.abspath(config["local"]["json_file_path"])
    json_filename = os.path.basename(json_path)
    return config, json_filename, json_path

# ✅ Task 1: Check if file exists
def check_file_exists():
    _, _, json_path = get_paths()
    print(f"🔍 Looking for JSON file: {json_path}")
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"❌ No JSON file found at: {json_path}")
    logger.info("✅ JSON file found.")

# ✅ Task 2: Upload to S3 and return key
def upload_to_s3_callable(ti):
    config, json_filename, _ = get_paths()
    s3_key = upload_file(config) # This now returns the full S3 key
    logger.info(f"🚀 Uploaded to S3 with key: {s3_key}")
    ti.xcom_push(key="s3_key", value=s3_key) # Send key to next task

# ✅ NEW DEBUG TASK:
def debug_python_environment():
    logger.info(f"DEBUG: Python executable: {sys.executable}")
    logger.info(f"DEBUG: Python version: {sys.version}")

    # List installed packages using the current Python executable's pip
    try:
        result = subprocess.run([sys.executable, '-m', 'pip', 'freeze'], capture_output=True, text=True, check=True)
        logger.info("DEBUG: Installed Python packages:\n" + result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"DEBUG: Error running pip freeze: {e}")
        logger.error(f"DEBUG: Stderr: {e.stderr}")
    except FileNotFoundError:
        logger.error("DEBUG: 'pip' command not found. Ensure pip is installed and in PATH for this Python environment.")


# ✅ Task 3: Load JSON from S3 to RDS
def load_json_to_rds_callable(ti):
    config, _, _ = get_paths()
    s3_key = ti.xcom_pull(task_ids="upload_to_s3", key="s3_key")
    if not s3_key:
        raise ValueError("❌ S3 key not found in XCom.")

    logger.info(f"📥 Loading JSON from S3 key: {s3_key}")

    db_connection = DBConnection(config)
    db_connection.connect()

    try:
        table_manager = RDSTableManager(db_connection)
        table_manager.create_employee_table()

        load_json_to_rds(config, db_connection, s3_key)

    finally:
        db_connection.close()

# ✅ DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 25), # Using a future date, adjust as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='json_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Orchestrates JSON ingestion from local folder to S3 and RDS.',
)

# ✅ DAG tasks
with dag:
    t1_check = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists
    )

    t2_upload = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3_callable
    )

    # NEW: Add the debug task into the DAG sequence
    t_debug_env = PythonOperator(
        task_id='debug_python_environment',
        python_callable=debug_python_environment,
        dag=dag,
    )

    t3_load = PythonOperator(
        task_id='load_json_to_rds',
        python_callable=load_json_to_rds_callable
    )

    # ✅ Task sequence - updated to include the debug task
    t1_check >> t2_upload >> t_debug_env >> t3_load