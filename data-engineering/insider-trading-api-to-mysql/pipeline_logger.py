"""
Metadata Logging Module
Tracks pipeline runs and stores metrics in data_collection_log table
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import config

logger = logging.getLogger(__name__)


class PipelineLogger:
    """
    Logs pipeline execution metadata to data_collection_log table
    
    Tracks:
    - Run timestamp
    - Symbols queried
    - Date ranges
    - API call counts
    - Records fetched/inserted/skipped
    - Success/failure status
    - Error messages
    """
    
    def __init__(self, db_manager):
        """
        Initialize pipeline logger
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db = db_manager
        self.current_run = None
        logger.info("Pipeline logger initialized")
    
    def start_run(
        self, 
        symbols: List[str], 
        from_date: str, 
        to_date: str
    ) -> Dict:
        """
        Start a new pipeline run and return run metadata
        
        Args:
            symbols (list): List of symbols being queried
            from_date (str): Start date for query
            to_date (str): End date for query
            
        Returns:
            dict: Run metadata dictionary
        """
        self.current_run = {
            'run_timestamp': datetime.now(),
            'symbols_queried': ','.join(symbols),
            'from_date': from_date,
            'to_date': to_date,
            'records_fetched': 0,
            'records_inserted': 0,
            'records_skipped': 0,
            'api_calls_made': 0,
            'status': 'running',
            'error_message': None,
            'pipeline_version': config.PIPELINE_VERSION
        }
        
        logger.info("=" * 70)
        logger.info("PIPELINE RUN STARTED")
        logger.info("=" * 70)
        logger.info(f"Timestamp:        {self.current_run['run_timestamp']}")
        logger.info(f"Symbols:          {self.current_run['symbols_queried']}")
        logger.info(f"Date Range:       {from_date} to {to_date}")
        logger.info(f"Pipeline Version: {config.PIPELINE_VERSION}")
        logger.info("=" * 70)
        
        return self.current_run
    
    def update_metrics(
        self,
        records_fetched: int = 0,
        records_inserted: int = 0,
        records_skipped: int = 0,
        api_calls: int = 0
    ):
        """
        Update run metrics (cumulative)
        
        Args:
            records_fetched (int): Number of records fetched from API
            records_inserted (int): Number of records inserted to DB
            records_skipped (int): Number of duplicate records skipped
            api_calls (int): Number of API calls made
        """
        if not self.current_run:
            logger.warning("No active run to update metrics")
            return
        
        self.current_run['records_fetched'] += records_fetched
        self.current_run['records_inserted'] += records_inserted
        self.current_run['records_skipped'] += records_skipped
        self.current_run['api_calls_made'] += api_calls
        
        logger.debug(f"Metrics updated: +{records_fetched} fetched, "
                    f"+{records_inserted} inserted, +{records_skipped} skipped")
    
    def mark_success(self):
        """Mark current run as successful"""
        if not self.current_run:
            logger.warning("No active run to mark as success")
            return
        
        self.current_run['status'] = 'success'
        logger.info("✓ Pipeline run marked as SUCCESS")
    
    def mark_failure(self, error_message: str):
        """
        Mark current run as failed with error message
        
        Args:
            error_message (str): Description of the error
        """
        if not self.current_run:
            logger.warning("No active run to mark as failure")
            return
        
        self.current_run['status'] = 'failed'
        self.current_run['error_message'] = error_message
        logger.error(f"✗ Pipeline run marked as FAILED: {error_message}")
    
    def mark_partial(self, error_message: str):
        """
        Mark run as partial success (some symbols failed)
        
        Args:
            error_message (str): Description of partial failure
        """
        if not self.current_run:
            logger.warning("No active run to mark as partial")
            return
        
        self.current_run['status'] = 'partial'
        self.current_run['error_message'] = error_message
        logger.warning(f"⚠ Pipeline run marked as PARTIAL: {error_message}")
    
    def end_run(self) -> Dict:
        """
        End current run and save to database
        
        Returns:
            dict: Final run metadata
        """
        if not self.current_run:
            logger.warning("No active run to end")
            return {}
        
        # Save to database
        self._save_to_database()
        
        # Log summary
        self._log_summary()
        
        run_data = self.current_run.copy()
        self.current_run = None
        
        return run_data
    
    def _save_to_database(self):
        """Save current run metadata to data_collection_log table"""
        if not self.current_run:
            return
        
        insert_query = """
            INSERT INTO data_collection_log (
                run_timestamp, symbols_queried, from_date, to_date,
                records_fetched, records_inserted, records_skipped,
                api_calls_made, status, error_message, pipeline_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            self.current_run['run_timestamp'],
            self.current_run['symbols_queried'],
            self.current_run['from_date'],
            self.current_run['to_date'],
            self.current_run['records_fetched'],
            self.current_run['records_inserted'],
            self.current_run['records_skipped'],
            self.current_run['api_calls_made'],
            self.current_run['status'],
            self.current_run['error_message'],
            self.current_run['pipeline_version']
        )
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(insert_query, values)
                conn.commit()
                log_id = cursor.lastrowid
                cursor.close()
                
                logger.info(f"✓ Run metadata saved to database (log_id={log_id})")
                
        except Exception as e:
            logger.error(f"Failed to save run metadata: {str(e)}")
            # Don't raise - this is just logging, shouldn't break pipeline
    
    def _log_summary(self):
        """Log run summary to console"""
        if not self.current_run:
            return
        
        run = self.current_run
        duration = (datetime.now() - run['run_timestamp']).total_seconds()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("PIPELINE RUN COMPLETED")
        logger.info("=" * 70)
        logger.info(f"Status:           {run['status'].upper()}")
        logger.info(f"Duration:         {duration:.1f} seconds")
        logger.info(f"API Calls:        {run['api_calls_made']}")
        logger.info(f"Records Fetched:  {run['records_fetched']}")
        logger.info(f"Records Inserted: {run['records_inserted']}")
        logger.info(f"Records Skipped:  {run['records_skipped']}")
        
        if run['records_fetched'] > 0:
            duplicate_rate = (run['records_skipped'] / run['records_fetched']) * 100
            logger.info(f"Duplicate Rate:   {duplicate_rate:.1f}%")
        
        if run['error_message']:
            logger.info(f"Error:            {run['error_message']}")
        
        logger.info("=" * 70)
        logger.info("")
    
    # ========================================================================
    # QUERY METHODS
    # ========================================================================
    
    def get_recent_runs(self, limit: int = 10) -> List[Dict]:
        """
        Get recent pipeline runs from log
        
        Args:
            limit (int): Number of recent runs to retrieve
            
        Returns:
            list: List of run dictionaries
        """
        query = """
            SELECT 
                id, run_timestamp, symbols_queried, from_date, to_date,
                records_fetched, records_inserted, records_skipped,
                api_calls_made, status, error_message, pipeline_version
            FROM data_collection_log
            ORDER BY run_timestamp DESC
            LIMIT %s
        """
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, (limit,))
                runs = cursor.fetchall()
                cursor.close()
                
                return runs
                
        except Exception as e:
            logger.error(f"Failed to retrieve recent runs: {str(e)}")
            return []
    
    def get_run_statistics(self) -> Dict:
        """
        Get aggregate statistics from all runs
        
        Returns:
            dict: Aggregate statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial_runs,
                SUM(records_fetched) as total_records_fetched,
                SUM(records_inserted) as total_records_inserted,
                SUM(api_calls_made) as total_api_calls,
                MAX(run_timestamp) as last_run_time
            FROM data_collection_log
        """
        
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query)
                stats = cursor.fetchone()
                cursor.close()
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to retrieve run statistics: {str(e)}")
            return {}


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_logger(db_manager) -> PipelineLogger:
    """
    Create and return PipelineLogger instance
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        PipelineLogger: Configured logger
    """
    return PipelineLogger(db_manager)


def log_run_summary(pipeline_logger: PipelineLogger):
    """
    Print summary of recent runs
    
    Args:
        pipeline_logger: PipelineLogger instance
    """
    print("\n" + "=" * 70)
    print("RECENT PIPELINE RUNS")
    print("=" * 70)
    
    runs = pipeline_logger.get_recent_runs(limit=5)
    
    if not runs:
        print("No runs found in log")
        return
    
    for run in runs:
        print(f"\nRun ID: {run['id']}")
        print(f"  Time:     {run['run_timestamp']}")
        print(f"  Status:   {run['status']}")
        print(f"  Symbols:  {run['symbols_queried']}")
        print(f"  Fetched:  {run['records_fetched']}")
        print(f"  Inserted: {run['records_inserted']}")
        print(f"  Skipped:  {run['records_skipped']}")
        
        if run['error_message']:
            print(f"  Error:    {run['error_message']}")
    
    print("\n" + "=" * 70)
    
    # Overall statistics
    stats = pipeline_logger.get_run_statistics()
    if stats:
        print("\nOVERALL STATISTICS")
        print("=" * 70)
        print(f"Total Runs:       {stats['total_runs']}")
        print(f"  Successful:     {stats['successful_runs']}")
        print(f"  Failed:         {stats['failed_runs']}")
        print(f"  Partial:        {stats['partial_runs']}")
        print(f"Total Records:    {stats['total_records_inserted']}")
        print(f"Total API Calls:  {stats['total_api_calls']}")
        print(f"Last Run:         {stats['last_run_time']}")
        print("=" * 70)


if __name__ == "__main__":
    # Test script
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    print("Pipeline Logger module loaded successfully")
    print("✓ Ready to track pipeline runs")
