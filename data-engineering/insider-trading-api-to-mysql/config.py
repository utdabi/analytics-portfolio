from datetime import date

# Stock symbols to fetch
SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 'AMD', 'PLTR']

# Date range
FROM_DATE = '2024-01-01'
TO_DATE = date.today().isoformat()

# API settings
API_BASE_URL = 'https://finnhub.io/api/v1'
API_TIMEOUT = 30
API_RETRIES = 3

# Database settings
DB_NAME = 'insider_transactions'
DB_PORT = 3306


def get_api_key():
    """Get Finnhub API key from Colab secrets"""
    from google.colab import userdata
    return userdata.get('FINNHUB_API_KEY')


def get_db_config():
    """Get MySQL connection config from Colab secrets"""
    from google.colab import userdata
    return {
        'host': userdata.get('AZURE_MYSQL_HOST'),
        'port': DB_PORT,
        'database': DB_NAME,
        'user': userdata.get('AZURE_MYSQL_USER'),
        'password': userdata.get('AZURE_MYSQL_PASSWORD')
    }
