"""
DataVault Pipeline - S3 Helper
S3 pe data likhne aur padhne ke liye.
"""

import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from config.settings import AWSConfig
from scripts.utils.logger import get_logger

log = get_logger(__name__)


class S3Helper:
    """S3 ke saath kaam karne ke liye helper class."""

    def __init__(self):
        self.bucket = AWSConfig.BUCKET_NAME
        self.raw_prefix = AWSConfig.RAW_PREFIX
        self.processed_prefix = AWSConfig.PROCESSED_PREFIX

        # S3 client banao
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
            region_name=AWSConfig.REGION,
        )
        log.info(f"S3Helper ready | bucket: {self.bucket}")

    def build_raw_key(self, filename: str, dt: datetime = None) -> str:
        """
        Date-based S3 key banao raw data ke liye.
        Example: raw/crypto/year=2026/month=03/day=14/crypto_20260314.json
        """
        dt = dt or datetime.utcnow()
        return (
            f"{self.raw_prefix}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            f"{filename}"
        )

    def build_processed_key(self, filename: str, dt: datetime = None) -> str:
        """
        Date-based S3 key banao processed data ke liye.
        """
        dt = dt or datetime.utcnow()
        return (
            f"{self.processed_prefix}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            f"{filename}"
        )

    def write_json(self, data: dict, key: str) -> str:
        """
        Dictionary ko JSON banao aur S3 pe upload karo.
        Returns: s3://bucket/key
        """
        try:
            body = json.dumps(data, default=str, indent=2).encode("utf-8")
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            uri = f"s3://{self.bucket}/{key}"
            log.info(f"S3 write success | {uri} | {len(body)} bytes")
            return uri

        except ClientError as e:
            log.error(f"S3 write failed | key: {key} | error: {e}")
            raise

    def read_json(self, key: str) -> dict:
        """S3 se JSON file padho aur dict return karo."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            log.info(f"S3 read success | key: {key}")
            return data

        except ClientError as e:
            log.error(f"S3 read failed | key: {key} | error: {e}")
            raise

    def check_connection(self) -> bool:
        """
        S3 connection test karo.
        Returns True agar bucket accessible hai.
        """
        try:
            self.client.head_bucket(Bucket=self.bucket)
            log.info(f"S3 connection OK | bucket: {self.bucket}")
            return True
        except ClientError as e:
            log.warning(f"S3 connection failed | {e}")
            return False