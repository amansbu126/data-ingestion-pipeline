import boto3
import json
import pandas as pd
import tempfile
import os
from loguru import logger
from sqlalchemy import text # Make sure sqlalchemy is imported here, though it's implicitly used by pandas

from src.main.utils.flatten_json import flatten_json_file

def load_json_to_rds(config, db_connection, s3_key):
    bucket = config["aws"]["bucket_name"]
    region = config["aws"]["region_name"]
    schema = config["database"].get("schema", "public")

    access_key = config["aws"].get("aws_access_key_id")
    secret_key = config["aws"].get("aws_secret_access_key")

    try:
        logger.info(f"☁️ Connecting to S3 bucket: {bucket}, key: {s3_key}")

        # ✅ Initialize S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        logger.info("✅ S3 client initialized.")

        # ✅ Read S3 object
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        json_data = obj["Body"].read().decode("utf-8")
        data = json.loads(json_data)
        logger.success("📥 JSON data downloaded and loaded into memory.")

        # ✅ Write to temporary file
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
            json.dump(data, tmp)
            tmp_path = tmp.name
        logger.info(f"📝 JSON temporarily written to: {tmp_path}")

        # ✅ Flatten the JSON file
        df = flatten_json_file(tmp_path)
        os.remove(tmp_path)
        logger.success("📄 JSON successfully flattened into DataFrame.")

        if df.empty:
            logger.warning("⚠️ Flattened DataFrame is empty. Skipping RDS load.")
            return

        logger.info(f"📐 DataFrame shape: {df.shape}")
        logger.info(f"📊 DataFrame preview:\n{df.head(2).to_string()}")

        if "id" in df.columns:
            logger.warning("🧹 Dropping 'id' column from DataFrame to avoid primary key conflict.")
            df.drop(columns=["id"], inplace=True)

        logger.debug(f"🧪 DataFrame columns: {df.columns.tolist()}")
        logger.debug(f"🧪 DataFrame dtypes:\n{df.dtypes}")

        # --- START OF MODIFICATION FOR PANDAS TO_SQL ---
        # Reconstruct the connection string using the config object
        # This is needed because Pandas might have issues recognizing the SQLAlchemy Engine
        # passed directly in some environments, or might expect a URI string for internal engine creation.
        db_config = config["database"]
        user = db_config["user"]
        password = db_config["password"]
        host = db_config["host"]
        port = db_config["port"]
        database_name = db_config["database"] # 'database' is also a key in your config
        db_type = db_config.get("db_type", "postgresql")
        schema = db_config.get("schema", "public")

        connection_string_for_pandas = (
            f"{db_type}://{user}:{password}@{host}:{port}/{database_name}"
            f"?options=-csearch_path={schema}"
        )
        # Log a masked version of the connection string for security
        logger.debug(f"Constructed connection string for Pandas: {connection_string_for_pandas.split('//')[0]}//****:****@{connection_string_for_pandas.split('@')[1]}")

        logger.info("📤 Writing data to RDS using database connection string for Pandas...")
        df.to_sql(
            name="employee_details",
            con=connection_string_for_pandas, # <--- THIS IS THE KEY CHANGE
            schema=schema,
            if_exists="append",
            index=False,
            method="multi" # 'multi' is generally efficient for bulk inserts
        )
        # --- END OF MODIFICATION ---

        logger.success("✅ Data loaded successfully into RDS.")

        # ✅ Optional row count check
        # Use db_connection.engine here because this object is confirmed to be properly
        # instantiated as a PostgresConnection and holds a valid SQLAlchemy Engine.
        with db_connection.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.employee_details"))
            logger.info(f"📊 Row count in 'employee_details': {result.scalar()}")

    except Exception as e:
        logger.error("❌ Exception occurred while processing JSON from S3.")
        logger.exception(e) # This prints the full traceback to the log
        raise e # Re-raise the exception to propagate it up to Airflow