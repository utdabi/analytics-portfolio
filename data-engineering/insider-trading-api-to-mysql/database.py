"""
Database Operations Layer
Handles MySQL connections, CRUD operations, and bulk inserts with deduplication
"""

import mysql.connector
from mysql.connector import Error, pooling
import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager

import config

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


class DatabaseManager:
    """
    Manages MySQL database connections and operations
    
    Features:
    - Connection pooling
    - Transaction management
    - Dimension table lookups/inserts
    - Bulk fact table inserts with INSERT IGNORE
    - Error handling and logging
    """
    
    def __init__(self, mysql_config: Dict = None):
        """
        Initialize database manager
        
        Args:
            mysql_config (dict, optional): MySQL connection config. If None, fetches from config.
        """
        self.config = mysql_config or config.get_mysql_config()
        self.connection_pool = None
        self._create_connection_pool()
        logger.info("Database manager initialized")
    
    def _create_connection_pool(self):
        """ 
        Create connection pool for efficient connection reuse with 
        lock timeout protection
        """
        try:
            # Add timeout settings to the config
            pool_config = self.config.copy()
            pool_config['connect_timeout'] = 60
            pool_config['use_pure'] = True
            
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name="insider_trading_pool",
                pool_size=5,
                pool_reset_session=True,
                **pool_config
            )
            
            # Set lock timeouts on pool connections
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 300")
                cursor.execute("SET SESSION lock_wait_timeout = 300")
                cursor.execute("SET SESSION wait_timeout = 600")
                cursor.close()
            
            logger.info("✓ Connection pool created successfully with extended timeouts")
        except Error as e:
            logger.error(f"Failed to create connection pool: {str(e)}")
            raise DatabaseError(f"Connection pool creation failed: {str(e)}")
        
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Usage:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                # ... do work
        """
        conn = None
        try:
            conn = self.connection_pool.get_connection()
            yield conn
        except Error as e:
            logger.error(f"Database connection error: {str(e)}")
            raise DatabaseError(f"Connection failed: {str(e)}")
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                
                if result:
                    logger.info("✓ Database connection successful")
                    return True
                return False
        except Exception as e:
            logger.error(f"✗ Database connection failed: {str(e)}")
            return False

    def unlock_tables(self):
        """
        Unlock all tables and flush to clear any stale locks
        Useful for recovering from lock timeout errors
        
        Returns:
            bool: True if unlock successful
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                logger.info("Unlocking tables and flushing...")
                cursor.execute("UNLOCK TABLES")
                cursor.execute("FLUSH TABLES")
                conn.commit()
                cursor.close()
                
                logger.info("✓ Tables unlocked and flushed successfully")
                return True
                
        except Error as e:
            logger.error(f"Failed to unlock tables: {e}")
            return False
    
    # ========================================================================
    # DIMENSION TABLE OPERATIONS
    # ========================================================================
    
    def get_or_create_stock(self, symbol: str, company_name: str = None) -> int:
        """
        Get stock_id for symbol, or create if doesn't exist
        
        Args:
            symbol (str): Stock symbol (e.g., 'AAPL')
            company_name (str, optional): Company name
            
        Returns:
            int: stock_id
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Try to get existing
            cursor.execute(
                "SELECT id FROM stocks WHERE symbol = %s",
                (symbol.upper(),)
            )
            result = cursor.fetchone()
            
            if result:
                stock_id = result[0]
                cursor.close()
                return stock_id
            
            # Create new (should rarely happen since we seed the table)
            company_name = company_name or f"{symbol} Inc."
            cursor.execute(
                "INSERT INTO stocks (symbol, company_name) VALUES (%s, %s)",
                (symbol.upper(), company_name)
            )
            conn.commit()
            stock_id = cursor.lastrowid
            cursor.close()
            
            logger.info(f"Created new stock: {symbol} (id={stock_id})")
            return stock_id
    
    def get_or_create_insider(
        self, 
        normalized_name: str, 
        raw_name: str = None, 
        relationship: str = None
    ) -> int:
        """
        Get insider_id for name, or create if doesn't exist
        
        Args:
            normalized_name (str): Normalized insider name
            raw_name (str, optional): Original raw name
            relationship (str, optional): Insider's relationship/title
            
        Returns:
            int: insider_id
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Try to get existing by normalized name
            cursor.execute(
                "SELECT id FROM insiders WHERE normalized_name = %s",
                (normalized_name,)
            )
            result = cursor.fetchone()
            
            if result:
                insider_id = result[0]
                cursor.close()
                return insider_id
            
            # Create new insider
            cursor.execute(
                """INSERT INTO insiders (normalized_name, raw_name, relationship) 
                   VALUES (%s, %s, %s)""",
                (normalized_name, raw_name or normalized_name, relationship)
            )
            conn.commit()
            insider_id = cursor.lastrowid
            cursor.close()
            
            logger.debug(f"Created new insider: {normalized_name} (id={insider_id})")
            return insider_id
    
    def bulk_get_or_create_insiders(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Efficiently get or create insiders for all unique names in DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame with 'normalized_name' column
            
        Returns:
            dict: Mapping of normalized_name -> insider_id
        """
        if 'normalized_name' not in df.columns:
            raise DatabaseError("DataFrame must have 'normalized_name' column")
        
        unique_names = df['normalized_name'].unique()
        name_to_id = {}
        
        logger.info(f"Processing {len(unique_names)} unique insiders")
        
        for name in unique_names:
            # Get raw name and relationship from first occurrence
            row = df[df['normalized_name'] == name].iloc[0]
            raw_name = row.get('raw_name', name)
            relationship = row.get('normalized_relationship', None)
            
            insider_id = self.get_or_create_insider(name, raw_name, relationship)
            name_to_id[name] = insider_id
        
        logger.info(f"✓ Processed {len(name_to_id)} insiders")
        return name_to_id
    
    # ========================================================================
    # FACT TABLE OPERATIONS
    # ========================================================================
    
    def insert_transactions(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Bulk insert transactions into insider_transactions table
        Uses INSERT IGNORE to skip duplicates silently
        
        Args:
            df (pd.DataFrame): DataFrame with transaction data
            
        Returns:
            dict: Statistics (inserted, skipped, total)
        """
        if df.empty:
            logger.warning("No transactions to insert")
            return {'total': 0, 'inserted': 0, 'skipped': 0}
        
        # Validate required columns exist
        required_columns = [
            'symbol', 'normalized_name', 'transaction_date', 'shares'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise DatabaseError(error_msg)
        
        logger.info(f"Inserting {len(df)} transactions")
        
        # Map symbols to stock_ids
        stock_id_map = {}
        for symbol in df['symbol'].unique():
            stock_id_map[symbol] = self.get_or_create_stock(symbol)
        
        df['stock_id'] = df['symbol'].map(stock_id_map)
        
        # Map insider names to insider_ids
        insider_id_map = self.bulk_get_or_create_insiders(df)
        df['insider_id'] = df['normalized_name'].map(insider_id_map)
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = (
                int(row['stock_id']),
                int(row['insider_id']),
                row.get('transaction_date'),
                row.get('filing_date'),
                row.get('transaction_type'),
                int(row.get('shares', 0)) if pd.notna(row.get('shares')) else 0,
                float(row.get('price')) if pd.notna(row.get('price')) else None,
                float(row.get('transaction_value')) if pd.notna(row.get('transaction_value')) else None,
                row.get('ownershipNature', 'direct'),
                row.get('direction', 'other'),
                bool(row.get('is_grant_option', False)),
                bool(row.get('price_imputed', False))
            )
            records.append(record)
        
        # Execute bulk insert with INSERT IGNORE
        insert_query = """
            INSERT IGNORE INTO insider_transactions (
                stock_id, insider_id, transaction_date, filing_date, 
                transaction_type, shares, price, transaction_value,
                ownership_type, direction, is_grant_option, price_imputed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.executemany(insert_query, records)
                conn.commit()
                
                inserted_count = cursor.rowcount
                skipped_count = len(records) - inserted_count
                
                cursor.close()
                
                logger.info(f"✓ Inserted: {inserted_count}, Skipped (duplicates): {skipped_count}")
                
                return {
                    'total': len(records),
                    'inserted': inserted_count,
                    'skipped': skipped_count
                }
                
            except Error as e:
                conn.rollback()
                logger.error(f"Bulk insert failed: {str(e)}")
                raise DatabaseError(f"Transaction insert failed: {str(e)}")
    
    # ========================================================================
    # QUERY OPERATIONS
    # ========================================================================
    
    def get_latest_transaction_date(self, symbol: str = None) -> Optional[str]:
        """
        Get the latest transaction date in database (for incremental loads)
        
        Args:
            symbol (str, optional): Specific symbol to check. If None, checks all.
            
        Returns:
            str or None: Latest transaction date (YYYY-MM-DD) or None if no data
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if symbol:
                # Get latest for specific symbol
                cursor.execute("""
                    SELECT MAX(t.transaction_date)
                    FROM insider_transactions t
                    JOIN stocks s ON t.stock_id = s.id
                    WHERE s.symbol = %s
                """, (symbol.upper(),))
            else:
                # Get latest across all symbols
                cursor.execute("""
                    SELECT MAX(transaction_date)
                    FROM insider_transactions
                """)
            
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0]:
                latest_date = result[0].strftime('%Y-%m-%d')
                logger.info(f"Latest transaction date: {latest_date}")
                return latest_date
            
            logger.info("No existing transactions found")
            return None
    
    def get_transaction_count(self, symbol: str = None) -> int:
        """
        Get count of transactions in database
        
        Args:
            symbol (str, optional): Specific symbol to count
            
        Returns:
            int: Transaction count
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if symbol:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM insider_transactions t
                    JOIN stocks s ON t.stock_id = s.id
                    WHERE s.symbol = %s
                """, (symbol.upper(),))
            else:
                cursor.execute("SELECT COUNT(*) FROM insider_transactions")
            
            result = cursor.fetchone()
            cursor.close()
            
            count = result[0] if result else 0
            return count
    
    def get_database_stats(self) -> Dict:
        """
        Get summary statistics about database contents
        
        Returns:
            dict: Statistics about records in each table
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Count stocks
            cursor.execute("SELECT COUNT(*) FROM stocks")
            stats['stocks'] = cursor.fetchone()[0]
            
            # Count insiders
            cursor.execute("SELECT COUNT(*) FROM insiders")
            stats['insiders'] = cursor.fetchone()[0]
            
            # Count transactions
            cursor.execute("SELECT COUNT(*) FROM insider_transactions")
            stats['transactions'] = cursor.fetchone()[0]
            
            # Date range
            cursor.execute("""
                SELECT MIN(transaction_date), MAX(transaction_date)
                FROM insider_transactions
            """)
            result = cursor.fetchone()
            if result[0] and result[1]:
                stats['date_range'] = {
                    'earliest': result[0].strftime('%Y-%m-%d'),
                    'latest': result[1].strftime('%Y-%m-%d')
                }
            else:
                stats['date_range'] = None
            
            cursor.close()
            
            return stats
    
    def close(self):
        """Close database connection pool"""
        if self.connection_pool:
            # Connection pools don't have explicit close in mysql-connector
            # Connections are automatically returned to pool
            logger.info("Database manager closed")


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_database_manager() -> DatabaseManager:
    """Create and return configured DatabaseManager"""
    return DatabaseManager()


def test_database_connection() -> bool:
    """Test database connection and return status"""
    try:
        db = create_database_manager()
        return db.test_connection()
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        return False


if __name__ == "__main__":
    # Test script
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    print("Testing Database Manager...")
    
    try:
        db = create_database_manager()
        
        if db.test_connection():
            print("✓ Database connection successful")
            
            stats = db.get_database_stats()
            print("\nDatabase Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        else:
            print("✗ Database connection failed")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
