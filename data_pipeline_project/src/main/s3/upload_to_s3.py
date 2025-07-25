import boto3
import os
import shutil
from datetime import datetime
from loguru import logger


class S3Uploader:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def upload_file(self, local_path, s3_key):
        try:
            self.s3.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"✅ File uploaded to S3: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"❌ Failed to upload file to S3: {e}")
            raise


def upload_file(config):
    source_path = os.path.abspath("data_uploads/json_files/data.json")
    archive_dir = os.path.abspath("data_uploads/archive")
    os.makedirs(archive_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = os.path.join(archive_dir, f"data_{timestamp}.json")

    try:
        shutil.copy2(source_path, archive_file)
        logger.info(f"📁 Archived JSON to: {archive_file}")
    except Exception as e:
        logger.error(f"❌ Failed to archive JSON file: {e}")
        raise
