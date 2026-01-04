"""
Database Operations for Insider Trading Pipeline
"""

import mysql.connector
import pandas as pd
import config


class DBError(Exception):
    """Database operation failed"""
    pass


def get_connection():
    """Get database connection"""
    return mysql.connector.connect(**config.get_db_config())


def test_connection():
    """Test database connection"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("Database connection successful")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def transform_to_dataframe(raw_data):
    """
    Transform raw API data to DataFrame ready for insertion
    
    Args:
        raw_data: dict {symbol: [transactions]} from API
    
    Returns:
        DataFrame with cleaned data
    """
    all_records = []
    
    for symbol, transactions in raw_data.items():
        for txn in transactions:
            # Skip if missing required fields
            if not txn.get('transactionDate'):
                continue
            
            shares = abs(txn.get('change', 0) or txn.get('share', 0) or 0)
            price = txn.get('transactionPrice')
            
            record = {
                'symbol': symbol.upper(),
                'insider_name': txn.get('name', 'Unknown'),
                'transaction_date': txn.get('transactionDate'),
                'filing_date': txn.get('filingDate'),
                'transaction_code': txn.get('transactionCode'),
                'shares': shares,
                'price': price if price and price > 0 else None,
                'transaction_value': shares * price if price and price > 0 else None,
                'change_amount': txn.get('change', 0),
                'filing_id': txn.get('id')
            }
            all_records.append(record)
    
    if not all_records:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_records)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['filing_date'] = pd.to_datetime(df['filing_date'])
    
    return df


def insert_transactions(df) -> dict:
    """
    Insert transactions into database (skips duplicates)
    """
    if df.empty:
        return {'inserted': 0, 'skipped': 0, 'total': 0}
    
    conn = get_connection()
    cursor = conn.cursor()
    
    insert_sql = """
        INSERT IGNORE INTO insider_transactions 
        (symbol, insider_name, transaction_date, filing_date, 
         transaction_code, shares, price, transaction_value, 
         change_amount, filing_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    records = []
    for _, row in df.iterrows():
        records.append((
            row['symbol'],
            row['insider_name'],
            row['transaction_date'],
            row['filing_date'],
            row['transaction_code'],
            int(row['shares']) if pd.notna(row['shares']) else 0,
            float(row['price']) if pd.notna(row['price']) else None,
            float(row['transaction_value']) if pd.notna(row['transaction_value']) else None,
            int(row['change_amount']) if pd.notna(row['change_amount']) else 0,
            row['filing_id']
        ))
    
    try:
        cursor.executemany(insert_sql, records)
        conn.commit()
        inserted = cursor.rowcount
        
        result = {
            'inserted': inserted,
            'skipped': len(records) - inserted,
            'total': len(records)
        }
        
        print(f"Inserted: {inserted}, Skipped (duplicates): {result['skipped']}")
        return result
        
    except Exception as e:
        conn.rollback()
        raise DBError(f"Insert failed: {e}")
    finally:
        cursor.close()
        conn.close()


def get_stats() -> dict:
    """Get database statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # Total count
    cursor.execute("SELECT COUNT(*) FROM insider_transactions")
    stats['total_transactions'] = cursor.fetchone()[0]
    
    # Count by symbol
    cursor.execute("""
        SELECT symbol, COUNT(*) as cnt 
        FROM insider_transactions 
        GROUP BY symbol 
        ORDER BY cnt DESC
    """)
    stats['by_symbol'] = dict(cursor.fetchall())
    
    # Date range
    cursor.execute("""
        SELECT MIN(transaction_date), MAX(transaction_date) 
        FROM insider_transactions
    """)
    result = cursor.fetchone()
    if result[0]:
        stats['date_range'] = {
            'from': result[0].strftime('%Y-%m-%d'),
            'to': result[1].strftime('%Y-%m-%d')
        }
    
    cursor.close()
    conn.close()
    
    return stats
