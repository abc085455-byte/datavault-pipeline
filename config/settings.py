"""
DataVault Pipeline - Central Configuration
Sari settings .env file se load hoti hain.
"""

import os
from dotenv import load_dotenv

# .env file load karo
load_dotenv()


class PipelineConfig:
    """Pipeline ki general settings."""
    ENV = os.getenv("PIPELINE_ENV", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


class AWSConfig:
    """AWS / S3 settings."""
    ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
    SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
    RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/crypto")
    PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed/crypto")


class CoinGeckoConfig:
    """CoinGecko API settings."""
    BASE_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
    API_KEY = os.getenv("COINGECKO_API_KEY", "")
    COINS = os.getenv("CRYPTO_COINS", "bitcoin,ethereum,solana")

    @staticmethod
    def get_coins_list():
        """String se list banao: 'bitcoin,ethereum' → ['bitcoin', 'ethereum']"""
        coins = os.getenv("CRYPTO_COINS", "bitcoin,ethereum,solana")
        return [c.strip() for c in coins.split(",")]


class SnowflakeConfig:
    """Snowflake settings."""
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
    USER = os.getenv("SNOWFLAKE_USER", "")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
    DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DATAVAULT_DB")
    SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "GOLD")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DATAVAULT_WH")
    ROLE = os.getenv("SNOWFLAKE_ROLE", "DATAVAULT_ROLE")


class DatabricksConfig:
    """Databricks settings."""
    HOST = os.getenv("DATABRICKS_HOST", "")
    TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "")
    NOTEBOOK_PATH = os.getenv("DATABRICKS_NOTEBOOK_PATH", "")