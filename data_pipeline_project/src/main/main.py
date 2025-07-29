from src.main.db.rds_connector import PostgresConnection
from src.main.db.rds_table_manager import RDSTableManager
from src.main.s3.upload_to_s3 import S3Uploader, upload_file
from src.main.db.load_json_to_rds import load_json_to_rds

import configparser
import os
from loguru import logger


def main():
    # Load configuration
    config_file = os.path.abspath("resources/config_file.ini")
    config = configparser.ConfigParser()
    config.read(config_file)

    if "database" not in config or "s3" not in config:
        logger.error("❌ Missing required [database] or [s3] section in config file.")
        return

    # Step 1: Upload JSON to S3
    s3_config = config["s3"]
    bucket_name = s3_config["bucket"]
    s3_key = s3_config["key"]
    local_json_path = os.path.abspath("data_uploads/json_files/data.json")

    uploader = S3Uploader(bucket_name)
    uploader.upload_file(local_path=local_json_path, s3_key=s3_key)

    # Step 2: Connect to RDS
    db_connection = PostgresConnection(config)
    db_connection.connect()

    # Step 3: Create table
    table_manager = RDSTableManager(db_connection)
    table_manager.create_employee_table()

    # Step 4: Load JSON from S3 → Flatten → Load to RDS → Delete from S3
    load_json_to_rds(config, db_connection)

    # Step 5: Close DB connection
    db_connection.close()

    upload_file(config)

if __name__ == "__main__":
    main()
