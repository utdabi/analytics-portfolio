"""
Fetches Finnhub API insider transactions with basic retry logic
"""

import requests
import time
import config


class APIError(Exception):
    """API request failed"""
    pass


def fetch_insider_transactions(symbol, from_date, to_date) -> list:
    """
    Fetch insider transactions for a single symbol
   
    """
    url = f"{config.API_BASE_URL}/stock/insider-transactions"
    params = {
        'symbol': symbol.upper(),
        'from': from_date,
        'to': to_date,
        'token': config.get_api_key()
    }
    
    # Simple retry logic
    for attempt in range(config.API_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=config.API_TIMEOUT)
            
            if response.status_code == 401:
                raise APIError("Invalid API key")
            if response.status_code == 429:
                print(f"Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            
            response.raise_for_status()
            data = response.json()
            return data.get('data', [])
            
        except Exception as e:
            raise APIError(f"Request failed: {e}")
    
    return []


def fetch_all_symbols(from_date=None, to_date=None, symbols=None) -> dict:
    """
    Fetch insider transactions for all configured symbols
    """
    from_date = from_date or config.FROM_DATE
    to_date = to_date or config.TO_DATE
    symbols = symbols or config.SYMBOLS
    
    results = {}
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] Fetching {symbol}...", end=" ")
        
        try:
            transactions = fetch_insider_transactions(symbol, from_date, to_date)
            results[symbol] = transactions
            print(f"{len(transactions)} transactions")
        except APIError as e:
            print(f"Error: {e}")
            results[symbol] = []
        
        # Small delay between requests
        if i < len(symbols):
            time.sleep(0.5)
    
    total = sum(len(txns) for txns in results.values())
    print(f"\nTotal: {total} transactions from {len(symbols)} symbols")
    
    return results


def test_connection():
    """Test API connection"""
    try:
        transactions = fetch_insider_transactions('AAPL', '2024-01-01', '2024-01-02')
        print("API connection successful")
        return True
    except Exception as e:
        print(f"API connection failed: {e}")
        return False
