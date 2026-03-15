"""
DataVault Pipeline - Snowflake Loader
S3 se data padho aur Snowflake Gold layer mein load karo.
"""

from datetime import datetime
from typing import Optional

import snowflake.connector

from config.settings import SnowflakeConfig
from scripts.utils.logger import get_logger

log = get_logger(__name__)


class SnowflakeLoader:
    """Snowflake mein data load karne ki class."""

    def __init__(self):
        self.config = SnowflakeConfig
        self.conn = None
        log.info(f"SnowflakeLoader ready | account: {self.config.ACCOUNT}")

    def connect(self):
        """Snowflake se connect karo."""
        log.info("Snowflake connecting...")
        self.conn = snowflake.connector.connect(
            account=self.config.ACCOUNT,
            user=self.config.USER,
            password=self.config.PASSWORD,
            warehouse=self.config.WAREHOUSE,
            database=self.config.DATABASE,
            schema=self.config.SCHEMA,
            role=self.config.ROLE,
        )
        log.info("Snowflake connected!")

    def disconnect(self):
        """Connection band karo."""
        if self.conn:
            self.conn.close()
            self.conn = None
            log.info("Snowflake disconnected")

    def execute(self, sql: str, values: tuple = None):
        """SQL run karo."""
        cursor = self.conn.cursor()
        try:
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            return cursor
        finally:
            cursor.close()

    def load_market_data(self, market_data: list, run_id: str, extracted_at: str):
        """
        Market data list ko CRYPTO_RAW table mein insert karo.
        """
        log.info(f"Loading {len(market_data)} records | run_id: {run_id}")

        insert_sql = """
            INSERT INTO CRYPTO_RAW (
                run_id, extracted_at, coin_id, symbol, name,
                current_price_usd, high_24h_usd, low_24h_usd,
                market_cap_usd, market_cap_rank, total_volume_usd,
                circulating_supply, total_supply,
                price_change_pct_24h, price_change_pct_1h, price_change_pct_7d,
                all_time_high_usd, all_time_low_usd,
                market_cap_tier, performance_24h
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
        """

        rows_loaded = 0
        for coin in market_data:
            # Market cap tier calculate karo
            market_cap = coin.get("market_cap") or 0
            if market_cap >= 100_000_000_000:
                tier = "mega_cap"
            elif market_cap >= 10_000_000_000:
                tier = "large_cap"
            elif market_cap >= 1_000_000_000:
                tier = "mid_cap"
            else:
                tier = "small_cap"

            # 24h performance classify karo
            change_24h = coin.get("price_change_percentage_24h") or 0
            if change_24h >= 5:
                performance = "strongly_bullish"
            elif change_24h >= 1:
                performance = "bullish"
            elif change_24h >= -1:
                performance = "neutral"
            elif change_24h >= -5:
                performance = "bearish"
            else:
                performance = "strongly_bearish"

            values = (
                run_id,
                extracted_at,
                coin.get("id"),
                coin.get("symbol", "").upper(),
                coin.get("name"),
                coin.get("current_price"),
                coin.get("high_24h"),
                coin.get("low_24h"),
                coin.get("market_cap"),
                coin.get("market_cap_rank"),
                coin.get("total_volume"),
                coin.get("circulating_supply"),
                coin.get("total_supply"),
                coin.get("price_change_percentage_24h"),
                coin.get("price_change_percentage_1h_in_currency"),
                coin.get("price_change_percentage_7d_in_currency"),
                coin.get("ath"),
                coin.get("atl"),
                tier,
                performance,
            )

            self.execute(insert_sql, values)
            rows_loaded += 1

        log.info(f"Rows loaded: {rows_loaded} | run_id: {run_id}")
        return rows_loaded

    def update_daily_summary(self):
        """CRYPTO_DAILY_SUMMARY table update karo."""
        log.info("Daily summary update ho rahi hai...")

        sql = """
            MERGE INTO CRYPTO_DAILY_SUMMARY tgt
            USING (
                SELECT
                    DATE(extracted_at)          AS price_date,
                    coin_id, symbol, name,
                    AVG(current_price_usd)      AS avg_price_usd,
                    MAX(high_24h_usd)           AS day_high_usd,
                    MIN(low_24h_usd)            AS day_low_usd,
                    SUM(total_volume_usd)       AS total_volume_usd,
                    AVG(price_change_pct_24h)   AS avg_price_change_pct_24h,
                    COUNT(*)                    AS observation_count,
                    MAX(run_id)                 AS latest_run_id
                FROM CRYPTO_RAW
                WHERE DATE(extracted_at) = CURRENT_DATE()
                GROUP BY 1,2,3,4
            ) src
            ON tgt.coin_id = src.coin_id
            AND tgt.price_date = src.price_date
            WHEN MATCHED THEN UPDATE SET
                avg_price_usd            = src.avg_price_usd,
                day_high_usd             = src.day_high_usd,
                day_low_usd              = src.day_low_usd,
                total_volume_usd         = src.total_volume_usd,
                avg_price_change_pct_24h = src.avg_price_change_pct_24h,
                observation_count        = src.observation_count,
                latest_run_id            = src.latest_run_id,
                updated_at               = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                price_date, coin_id, symbol, name,
                avg_price_usd, day_high_usd, day_low_usd,
                total_volume_usd, avg_price_change_pct_24h,
                observation_count, latest_run_id
            ) VALUES (
                src.price_date, src.coin_id, src.symbol, src.name,
                src.avg_price_usd, src.day_high_usd, src.day_low_usd,
                src.total_volume_usd, src.avg_price_change_pct_24h,
                src.observation_count, src.latest_run_id
            );
        """
        self.execute(sql)
        log.info("Daily summary updated!")

    def load(self, payload: dict) -> dict:
        """
        Main load function:
        1. Connect karo
        2. Market data insert karo
        3. Daily summary update karo
        4. Disconnect karo
        """
        result = {"status": "failed", "rows_loaded": 0}

        try:
            self.connect()

            metadata = payload.get("metadata", {})
            run_id = metadata.get("run_id", "unknown")
            extracted_at = metadata.get("extracted_at")
            market_data = payload.get("market_data", [])

            # Data load karo
            rows = self.load_market_data(market_data, run_id, extracted_at)

            # Summary update karo
            self.update_daily_summary()

            result["status"] = "success"
            result["rows_loaded"] = rows
            result["run_id"] = run_id

            log.info(f"Load complete | {result}")
            return result

        except Exception as e:
            log.error(f"Load failed | error: {e}")
            result["error"] = str(e)
            raise

        finally:
            self.disconnect()


def run_load(payload: dict) -> dict:
    """Airflow PythonOperator is function ko call karega."""
    loader = SnowflakeLoader()
    return loader.load(payload)