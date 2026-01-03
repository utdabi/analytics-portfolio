"""
Configuration module for Insider Trading Data Pipeline
Contains constants, API settings, and secrets management
"""

from datetime import date

# ============================================================================
# STOCK SYMBOLS
# ============================================================================
MAGNIFICENT_10_SYMBOLS = [
    'AAPL',   # Apple
    'MSFT',   # Microsoft
    'GOOGL',  # Alphabet
    'AMZN',   # Amazon
    'NVDA',   # NVIDIA
    'META',   # Meta Platforms
    'TSLA',   # Tesla
    'AVGO',   # Broadcom
    'AMD',    # Advanced Micro Devices
    'PLTR'    # Palantir Technologies
]

# ============================================================================
# DATE RANGES
# ============================================================================
FROM_DATE = '2024-01-01'  # Fixed start date for historical load
TO_DATE = date.today().isoformat()  # Through today

# ============================================================================
# FINNHUB API CONFIGURATION
# ============================================================================
FINNHUB_BASE_URL = 'https://finnhub.io/api/v1'
FINNHUB_INSIDER_ENDPOINT = '/stock/insider-transactions'
FINNHUB_RATE_LIMIT = 60  # Free tier: 60 calls per minute
FINNHUB_RETRY_ATTEMPTS = 3
FINNHUB_RETRY_DELAY = 2  # seconds

# ============================================================================
# MYSQL DATABASE CONFIGURATION
# ============================================================================
MYSQL_PORT = 3306
MYSQL_DATABASE = 'insider_transactions'

# ============================================================================
# DATA PIPELINE CONFIGURATION
# ============================================================================
PIPELINE_VERSION = 'v1.0.0'

# Transaction type mappings (Finnhub codes to readable labels)
TRANSACTION_TYPE_MAP = {
    'P': 'Purchase',
    'S': 'Sale',
    'A': 'Grant',
    'D': 'Disposition',
    'M': 'Option Exercise',
    'G': 'Gift',
    'C': 'Conversion',
    'W': 'Will',
    'I': 'In-Kind',
    'F': 'Tax Payment',
    'J': 'Other',
    'U': 'Tender Offer'
}

# Direction mappings
DIRECTION_MAP = {
    'P': 'buy',
    'A': 'buy',
    'M': 'buy',
    'S': 'sell',
    'D': 'sell',
    'F': 'sell',
    'G': 'other',
    'C': 'other',
    'W': 'other',
    'I': 'other',
    'J': 'other',
    'U': 'other'
}

# Controlled vocabulary for relationship/titles
RELATIONSHIP_NORMALIZATION = {
    'ceo': 'CEO',
    'chief executive officer': 'CEO',
    'cfo': 'CFO',
    'chief financial officer': 'CFO',
    'coo': 'COO',
    'chief operating officer': 'COO',
    'cto': 'CTO',
    'chief technology officer': 'CTO',
    'director': 'Director',
    'board member': 'Director',
    '10% owner': '10% Owner',
    'beneficial owner': '10% Owner',
    'president': 'President',
    'vp': 'Vice President',
    'vice president': 'Vice President',
    'svp': 'Senior Vice President',
    'evp': 'Executive Vice President',
    'officer': 'Officer',
    'general counsel': 'General Counsel',
    'secretary': 'Secretary',
    'treasurer': 'Treasurer',
    'controller': 'Controller',
    'trustee': 'Trustee'
}

# ============================================================================
# DATA VALIDATION RULES
# ============================================================================
VALIDATION_RULES = {
    'required_fields': ['symbol', 'transaction_date'],
    'min_shares': 0,  # Allow 0 for certain transaction types
    'max_transaction_value': 1_000_000_000,  # $1B threshold for flagging
    'max_future_days': 7  # Allow transactions dated up to 7 days in future
}

# ============================================================================
# SECRETS MANAGEMENT (Google Colab)
# ============================================================================
def get_secret(key_name):
    """
    Retrieve secret from Google Colab Secrets
    
    Args:
        key_name (str): Name of the secret key
        
    Returns:
        str: Secret value
        
    Raises:
        Exception: If secret not found or not in Colab environment
    """
    try:
        from google.colab import userdata
        return userdata.get(key_name)
    except ImportError:
        raise Exception("Not in Google Colab environment. Use environment variables instead.")
    except Exception as e:
        raise Exception(f"Failed to retrieve secret '{key_name}': {str(e)}")


def get_finnhub_api_key():
    """Get Finnhub API key from Colab Secrets"""
    return get_secret('FINNHUB_API_KEY')


def get_mysql_config():
    """
    Get MySQL connection configuration from Colab Secrets
    
    Returns:
        dict: MySQL connection parameters
    """
    return {
        'host': get_secret('AZURE_MYSQL_HOST'),
        'port': MYSQL_PORT,
        'database': MYSQL_DATABASE,
        'user': get_secret('AZURE_MYSQL_USER'),
        'password': get_secret('AZURE_MYSQL_PASSWORD')
    }


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s | %(levelname)s | %(message)s',
    'datefmt': '%Y-%m-%d %H:%M:%S'
}

# ============================================================================
# SCIKIT-LEARN PIPELINE CONFIGURATION
# ============================================================================
PIPELINE_FILE_PATH = 'preprocessing_pipeline.pkl'
IMPUTATION_STRATEGY = 'median'  # For missing price values

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_symbols_for_query():
    """Returns list of symbols to query"""
    return MAGNIFICENT_10_SYMBOLS


def is_grant_or_option(transaction_type, price):
    """
    Determine if transaction is a grant or option exercise
    
    Args:
        transaction_type (str): Transaction type code
        price (float): Transaction price
        
    Returns:
        bool: True if grant/option
    """
    return transaction_type in ['A', 'M'] or (price is not None and price == 0)


def normalize_transaction_type(code):
    """Map transaction type code to readable label"""
    return TRANSACTION_TYPE_MAP.get(code, code)


def get_direction(transaction_type):
    """Get direction (buy/sell/other) from transaction type"""
    return DIRECTION_MAP.get(transaction_type, 'other')


def normalize_relationship(raw_relationship):
    """
    Normalize insider relationship/title to controlled vocabulary
    
    Args:
        raw_relationship (str): Raw relationship string
        
    Returns:
        str: Normalized relationship
    """
    if not raw_relationship:
        return 'Unknown'
    
    relationship_lower = raw_relationship.lower().strip()
    
    # Check for exact matches in normalization map
    for key, value in RELATIONSHIP_NORMALIZATION.items():
        if key in relationship_lower:
            return value
    
    # Return title case of original if no match
    return raw_relationship.title()
