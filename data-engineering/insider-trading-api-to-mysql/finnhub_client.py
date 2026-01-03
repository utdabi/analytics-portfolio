"""
Finnhub API Client for Insider Trading Data
Handles authentication, rate limiting, retries, and error handling
"""

import requests
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import config

logger = logging.getLogger(__name__)


class FinnhubAPIError(Exception):
    """Custom exception for Finnhub API errors"""
    pass


class FinnhubClient:
    """
    Client for interacting with Finnhub Stock API
    
    Features:
    - Authentication with API token
    - Rate limiting (60 calls/minute for free tier)
    - Automatic retry with exponential backoff
    - Error handling and logging
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Finnhub API client
        
        Args:
            api_key (str, optional): Finnhub API token. If None, fetches from config.
        """
        self.api_key = api_key or config.get_finnhub_api_key()
        self.base_url = config.FINNHUB_BASE_URL
        self.rate_limit = config.FINNHUB_RATE_LIMIT
        self.retry_attempts = config.FINNHUB_RETRY_ATTEMPTS
        self.retry_delay = config.FINNHUB_RETRY_DELAY
        
        # Rate limiting tracking
        self.call_timestamps = []
        
        logger.info(f"Finnhub client initialized (rate limit: {self.rate_limit} calls/min)")
    
    def _wait_for_rate_limit(self):
        """
        Implement rate limiting by tracking API calls
        Wait if we've exceeded the rate limit in the current minute
        """
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        # Remove timestamps older than 1 minute
        self.call_timestamps = [ts for ts in self.call_timestamps if ts > one_minute_ago]
        
        # If we've hit the rate limit, wait
        if len(self.call_timestamps) >= self.rate_limit:
            oldest_call = self.call_timestamps[0]
            wait_time = 60 - (now - oldest_call).total_seconds()
            
            if wait_time > 0:
                logger.warning(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time + 1)  # Add 1 second buffer
                self.call_timestamps = []
        
        # Record this call
        self.call_timestamps.append(now)
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """
        Make HTTP request to Finnhub API with retry logic
        
        Args:
            endpoint (str): API endpoint path
            params (dict): Query parameters
            
        Returns:
            dict: API response JSON
            
        Raises:
            FinnhubAPIError: If request fails after all retries
        """
        url = f"{self.base_url}{endpoint}"
        params['token'] = self.api_key
        
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self._wait_for_rate_limit()
                
                logger.debug(f"API request: {endpoint} (attempt {attempt}/{self.retry_attempts})")
                response = requests.get(url, params=params, timeout=30)
                
                # Check for HTTP errors
                if response.status_code == 401:
                    raise FinnhubAPIError("Invalid API key. Check FINNHUB_API_KEY in Colab Secrets.")
                elif response.status_code == 429:
                    logger.warning("Rate limit exceeded (429). Waiting before retry...")
                    time.sleep(self.retry_delay * attempt)
                    continue
                elif response.status_code >= 500:
                    logger.warning(f"Server error ({response.status_code}). Retrying...")
                    time.sleep(self.retry_delay * attempt)
                    continue
                
                response.raise_for_status()
                
                data = response.json()
                logger.debug(f"API response received successfully")
                return data
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt}/{self.retry_attempts})")
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)
                    continue
                else:
                    raise FinnhubAPIError("Request timed out after all retry attempts")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)
                    continue
                else:
                    raise FinnhubAPIError(f"Request failed after {self.retry_attempts} attempts: {str(e)}")
        
        raise FinnhubAPIError("Unexpected error in API request")
    
    def get_insider_transactions(
        self, 
        symbol: str, 
        from_date: str, 
        to_date: str
    ) -> List[Dict]:
        """
        Fetch insider transactions for a specific stock symbol
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL')
            from_date (str): Start date in YYYY-MM-DD format
            to_date (str): End date in YYYY-MM-DD format
            
        Returns:
            list: List of transaction dictionaries
            
        Example:
            client = FinnhubClient()
            transactions = client.get_insider_transactions('AAPL', '2024-01-01', '2024-12-31')
        """
        logger.info(f"Fetching insider transactions for {symbol} ({from_date} to {to_date})")
        
        params = {
            'symbol': symbol.upper(),
            'from': from_date,
            'to': to_date
        }
        
        try:
            response = self._make_request(config.FINNHUB_INSIDER_ENDPOINT, params)
            
            # Finnhub returns {"data": [...]} structure
            transactions = response.get('data', [])
            
            if not transactions:
                logger.info(f"No transactions found for {symbol}")
                return []
            
            logger.info(f"Retrieved {len(transactions)} transactions for {symbol}")
            return transactions
            
        except FinnhubAPIError as e:
            logger.error(f"Failed to fetch transactions for {symbol}: {str(e)}")
            raise
    
    def get_bulk_insider_transactions(
        self, 
        symbols: List[str], 
        from_date: str, 
        to_date: str,
        continue_on_error: bool = True
    ) -> Dict[str, List[Dict]]:
        """
        Fetch insider transactions for multiple symbols
        
        Args:
            symbols (list): List of stock symbols
            from_date (str): Start date in YYYY-MM-DD format
            to_date (str): End date in YYYY-MM-DD format
            continue_on_error (bool): If True, continue processing on errors
            
        Returns:
            dict: Dictionary mapping symbols to transaction lists
                  Format: {'AAPL': [...], 'MSFT': [...]}
        """
        logger.info(f"Fetching bulk transactions for {len(symbols)} symbols")
        
        results = {}
        errors = {}
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"Processing {i}/{len(symbols)}: {symbol}")
                transactions = self.get_insider_transactions(symbol, from_date, to_date)
                results[symbol] = transactions
                
                # Brief pause between symbols to be respectful
                if i < len(symbols):
                    time.sleep(0.5)
                    
            except FinnhubAPIError as e:
                error_msg = f"Error fetching {symbol}: {str(e)}"
                logger.error(error_msg)
                errors[symbol] = str(e)
                
                if not continue_on_error:
                    raise FinnhubAPIError(f"Bulk fetch aborted at {symbol}: {str(e)}")
        
        # Summary
        total_transactions = sum(len(txns) for txns in results.values())
        logger.info(f"Bulk fetch complete: {len(results)} symbols succeeded, "
                   f"{len(errors)} failed, {total_transactions} total transactions")
        
        if errors:
            logger.warning(f"Errors occurred for symbols: {list(errors.keys())}")
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test API connection and authentication
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info("Testing Finnhub API connection...")
            # Try a simple request with minimal data
            test_data = self.get_insider_transactions('AAPL', '2026-01-01', '2026-01-02')
            logger.info("✓ API connection successful")
            return True
        except FinnhubAPIError as e:
            logger.error(f"✗ API connection failed: {str(e)}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_client() -> FinnhubClient:
    """Create and return configured Finnhub client"""
    return FinnhubClient()


def fetch_all_magnificent_10(from_date: str, to_date: str) -> Dict[str, List[Dict]]:
    """
    Convenience function to fetch all Magnificent 10 stocks
    
    Args:
        from_date (str): Start date (YYYY-MM-DD)
        to_date (str): End date (YYYY-MM-DD)
        
    Returns:
        dict: Symbol -> transactions mapping
    """
    client = create_client()
    symbols = config.get_symbols_for_query()
    return client.get_bulk_insider_transactions(symbols, from_date, to_date)


if __name__ == "__main__":
    # Test script
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    print("Testing Finnhub API Client...")
    client = create_client()
    
    if client.test_connection():
        print("\n✓ API client is working correctly")
    else:
        print("\n✗ API client test failed")
