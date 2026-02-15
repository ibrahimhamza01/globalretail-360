import pandas as pd
import requests
from time import sleep
from src.utils.logger import get_logger
from src.utils.config import EXCHANGE_RATE_API_KEY, EXCHANGE_RATE_API_URL, FAKE_STORE_API_URL

logger = get_logger(__name__)


def _fetch_json(url: str, params: dict = None, max_retries: int = 3, sleep_sec: int = 2) -> dict:
    """
    Fetch JSON data from a URL with retries and logging.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt} failed for URL {url}: {e}")
            if attempt < max_retries:
                sleep(sleep_sec)
            else:
                logger.error(f"All {max_retries} attempts failed for URL {url}")
                raise


def load_exchange_rates(base_currency: str = "USD") -> pd.DataFrame:
    """
    Load current exchange rates from ExchangeRate API.
    
    Returns a DataFrame with columns: ['currency', 'rate']
    """
    logger.info(f"Fetching exchange rates from ExchangeRate API | Base: {base_currency}")

    url = EXCHANGE_RATE_API_URL.format(api_key=EXCHANGE_RATE_API_KEY, base=base_currency)

    data = _fetch_json(url)
    
    if "conversion_rates" not in data:
        logger.error(f"No conversion_rates found in response: {data}")
        raise ValueError("Invalid response from ExchangeRate API")

    df = pd.DataFrame(
        list(data["conversion_rates"].items()),
        columns=["currency", "rate"]
    )

    logger.info(f"Exchange rates fetched: {len(df)} currencies")
    return df


def load_fake_store_products() -> pd.DataFrame:
    """
    Load product catalog from Fake Store API.
    Returns a DataFrame with product details.
    """
    logger.info("Fetching products from Fake Store API")

    data = _fetch_json(FAKE_STORE_API_URL)

    if not isinstance(data, list):
        logger.error(f"Unexpected response from Fake Store API: {data}")
        raise ValueError("Invalid response from Fake Store API")

    df = pd.DataFrame(data)
    logger.info(f"Fake Store products fetched: {len(df)} items")
    return df