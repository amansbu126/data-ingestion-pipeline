import boto3
import json
import pandas as pd
import tempfile
from loguru import logger
from src.main.utils.flatten_json import flatten_json_file  # Correct function name

def load_json_to_rds(config, db_connection):
    bucket = config["s3"]["bucket"]
    key = config["s3"]["key"]

    try:
        logger.info("☁️ Downloading JSON from S3...")
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        json_data = obj["Body"].read()
        data = json.loads(json_data)

        # Save JSON temporarily to a file for flattening
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp:
            json.dump(data, tmp)
            tmp.seek(0)
            df = flatten_json_file(tmp.name)

        if df.empty:
            logger.warning("⚠️ Flattened DataFrame is empty. Skipping RDS load.")
            return

        logger.info(f"🧾 Flattened DataFrame shape: {df.shape}")

        # Insert into RDS
        logger.info("📤 Loading data into RDS table 'employee_details'...")
        df.to_sql(
            name="employee_details",
            con=db_connection.engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        logger.success("✅ Data loaded into 'employee_details' successfully.")

        # Delete from S3 after successful load
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"🗑️ Deleted uploaded file from S3: s3://{bucket}/{key}")

    except Exception as e:
        logger.error(f"❌ Failed to load data into RDS: {e}")
