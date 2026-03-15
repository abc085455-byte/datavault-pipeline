"""
DataVault Pipeline - API Extractor
CoinGecko se crypto data fetch karo aur S3 Bronze layer mein save karo.
"""

import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import CoinGeckoConfig
from scripts.utils.logger import get_logger
from scripts.utils.s3_helper import S3Helper

log = get_logger(__name__)

# Rate limit ke liye pause
RATE_LIMIT_PAUSE = 1.2
REQUEST_TIMEOUT = 30


class CoinGeckoExtractor:
    """CoinGecko API se data extract karne ki class."""

    def __init__(self):
        self.base_url = CoinGeckoConfig.BASE_URL
        self.api_key = CoinGeckoConfig.API_KEY
        self.coins = CoinGeckoConfig.get_coins_list()
        self.s3 = S3Helper()
        self.session = self._build_session()
        log.info(f"Extractor ready | coins: {self.coins}")

    def _build_session(self) -> requests.Session:
        """Retry logic ke saath HTTP session banao."""
        session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        # API key hai to header mein lagao
        if self.api_key:
            session.headers.update({"x-cg-demo-api-key": self.api_key})

        return session

    def fetch_market_data(self) -> list:
        """
        CoinGecko /coins/markets endpoint se data lao.
        Returns: list of coin data dicts
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(self.coins),
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "1h,24h,7d",
        }

        log.info(f"API call shuru | coins: {self.coins}")

        response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        data = response.json()
        log.info(f"API call success | records: {len(data)}")
        return data

    def fetch_global_stats(self) -> dict:
        """Global crypto market stats lao."""
        url = f"{self.base_url}/global"
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("data", {})

    def extract(self, execution_date: datetime = None) -> str:
        """
        Main extraction function:
        1. API se data fetch karo
        2. Envelope mein wrap karo
        3. S3 Bronze layer mein save karo
        4. S3 URI return karo
        """
        dt = execution_date or datetime.now(timezone.utc)
        run_id = dt.strftime("%Y%m%d_%H%M%S")

        log.info(f"Extraction shuru | run_id: {run_id}")

        # Step 1: Market data fetch
        market_data = self.fetch_market_data()

        # Rate limit pause
        time.sleep(RATE_LIMIT_PAUSE)

        # Step 2: Global stats fetch
        try:
            global_stats = self.fetch_global_stats()
        except Exception as e:
            log.warning(f"Global stats fetch fail | error: {e}")
            global_stats = {}

        # Step 3: Envelope banao
        payload = {
            "metadata": {
                "run_id": run_id,
                "extracted_at": dt.isoformat(),
                "source": "coingecko_api_v3",
                "layer": "bronze",
                "coin_count": len(market_data),
                "coins": self.coins,
            },
            "global_stats": global_stats,
            "market_data": market_data,
        }

        # Step 4: S3 mein save karo
        filename = f"crypto_{run_id}.json"
        key = self.s3.build_raw_key(filename, dt)

        uri = self.s3.write_json(data=payload, key=key)

        log.info(f"Extraction complete | run_id: {run_id} | uri: {uri}")
        return uri


def run_extraction(execution_date: datetime = None) -> str:
    """Airflow PythonOperator is function ko call karega."""
    extractor = CoinGeckoExtractor()
    return extractor.extract(execution_date=execution_date)