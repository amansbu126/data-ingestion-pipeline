from configparser import ConfigParser
import boto3
import os
from loguru import logger
from datetime import datetime


class S3Uploader:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def upload_file(self, local_path, s3_key):
        try:
            self.s3.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"✅ File uploaded to S3: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"❌ Failed to upload file to S3: {e}")
            raise

def upload_file(config):
    # 🗂️ Read local JSON file path from config
    json_path = config["local"]["json_file_path"]

    # 🕒 Create S3 key with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key_prefix = config["aws"].get("s3_key_prefix", "")
    s3_key = os.path.join(s3_key_prefix, f"data_{timestamp}.json")

    # ☁️ Upload file
    uploader = S3Uploader(
        aws_access_key_id=config["aws"]["aws_access_key_id"],
        aws_secret_access_key=config["aws"]["aws_secret_access_key"],
        region_name=config["aws"]["region_name"],
        bucket_name=config["aws"]["bucket_name"]
    )
    uploader.upload_file(json_path, s3_key)

    return s3_key  # ✅ Return the key for XCom
