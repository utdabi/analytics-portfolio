"""
Data Quality Validation Module
Implements validation rules and rejection logic for insider transactions
"""

import pandas as pd
import logging
from datetime import date, timedelta
from typing import Dict, Tuple, List

import config

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results"""
    
    def __init__(self):
        self.valid_records = pd.DataFrame()
        self.rejected_records = pd.DataFrame()
        self.flagged_records = pd.DataFrame()
        self.validation_summary = {}
    
    def __repr__(self):
        return (f"ValidationResult(valid={len(self.valid_records)}, "
                f"rejected={len(self.rejected_records)}, "
                f"flagged={len(self.flagged_records)})")


class DataValidator:
    """
    Validates insider transaction data according to defined rules
    
    Validation Levels:
    - REJECT: Critical issues, record will not be inserted
    - FLAG: Suspicious but acceptable, record inserted with warning
    - PASS: Valid record
    """
    
    def __init__(self):
        """Initialize validator with rules from config"""
        self.rules = config.VALIDATION_RULES
        self.today = date.today()
        logger.info("Data validator initialized")
    
    # ========================================================================
    # CRITICAL VALIDATIONS (REJECT)
    # ========================================================================
    
    def validate_required_fields(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate that required fields are present and not null
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            tuple: (valid_df, rejected_df)
        """
        if df.empty:
            return df, pd.DataFrame()
        
        required = self.rules['required_fields']
        logger.info(f"Validating required fields: {required}")
        
        # Check for missing columns
        missing_cols = [col for col in required if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame(), df.copy()
        
        # Identify rows with null values in required fields
        null_mask = df[required].isna().any(axis=1)
        
        valid_df = df[~null_mask].copy()
        rejected_df = df[null_mask].copy()
        rejected_df['rejection_reason'] = 'Missing required field(s)'
        
        if len(rejected_df) > 0:
            logger.warning(f"Rejected {len(rejected_df)} records due to missing required fields")
        
        return valid_df, rejected_df
    
    def validate_transaction_date(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate transaction_date is not in the future (beyond allowed buffer)
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            tuple: (valid_df, rejected_df)
        """
        if df.empty or 'transactionDate' not in df.columns:
            return df, pd.DataFrame()
        
        max_future_days = self.rules['max_future_days']
        max_allowed_date = self.today + timedelta(days=max_future_days)
        
        logger.info(f"Validating transaction dates (max allowed: {max_allowed_date})")
        
        # Convert to date for comparison
        df['transactionDate'] = pd.to_datetime(df['transactionDate'])
        future_mask = df['transactionDate'].dt.date > max_allowed_date
        
        valid_df = df[~future_mask].copy()
        rejected_df = df[future_mask].copy()
        rejected_df['rejection_reason'] = f'Transaction date > {max_allowed_date}'
        
        if len(rejected_df) > 0:
            logger.warning(f"Rejected {len(rejected_df)} records with future dates")
        
        return valid_df, rejected_df
    
    def validate_shares(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate shares field (must be positive after pipeline transformation)
        
        After data pipeline, shares should always be positive (absolute value).
        Negative values indicate a pipeline error.
        We reject zero shares as they indicate no actual transaction occurred.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            tuple: (valid_df, rejected_df)
        """
        if df.empty:
            return df, pd.DataFrame()
        
        # Check for standardized 'shares' column
        if 'shares' not in df.columns:
            logger.error("No shares column found in DataFrame")
            rejected_df = df.copy()
            rejected_df['rejection_reason'] = 'Missing shares data'
            return pd.DataFrame(), rejected_df
        
        logger.info(f"Validating shares column")
        
        # Reject records with null, zero, or negative shares
        # Note: Negative shares here indicate pipeline error (should have been converted to absolute)
        invalid_mask = df['shares'].isna() | (df['shares'] <= 0)
        
        valid_df = df[~invalid_mask].copy()
        rejected_df = df[invalid_mask].copy()
        rejected_df['rejection_reason'] = 'Invalid shares (null, zero, or negative after transformation)'
        
        if len(rejected_df) > 0:
            logger.warning(f"Rejected {len(rejected_df)} records with invalid shares")
        
        return valid_df, rejected_df
    
    # ========================================================================
    # WARNING VALIDATIONS (FLAG but allow)
    # ========================================================================
    
    def flag_missing_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag transactions with missing price (common for grants/options)
        These are NOT rejected, just flagged
        
        Args:
            df (pd.DataFrame): DataFrame to check
            
        Returns:
            pd.DataFrame: DataFrame with 'price_warning' flag
        """
        if df.empty or 'price' not in df.columns:
            return df
        
        df['price_warning'] = df['price'].isna()
        
        flagged_count = df['price_warning'].sum()
        if flagged_count > 0:
            logger.info(f"Flagged {flagged_count} records with missing price")
        
        return df
    
    def flag_high_value_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag unusually high transaction values for review
        
        Args:
            df (pd.DataFrame): DataFrame to check
            
        Returns:
            pd.DataFrame: DataFrame with 'high_value_warning' flag
        """
        if df.empty or 'transaction_value' not in df.columns:
            return df
        
        threshold = self.rules['max_transaction_value']
        df['high_value_warning'] = df['transaction_value'] > threshold
        
        flagged_count = df['high_value_warning'].sum()
        if flagged_count > 0:
            logger.warning(f"Flagged {flagged_count} records with transaction value > ${threshold:,.0f}")
        
        return df
    
    def flag_old_filing_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag transactions where filing date is significantly after transaction date
        
        Args:
            df (pd.DataFrame): DataFrame to check
            
        Returns:
            pd.DataFrame: DataFrame with 'late_filing_warning' flag
        """
        if df.empty:
            return df
        
        if 'transactionDate' not in df.columns or 'filingDate' not in df.columns:
            return df
        
        # Calculate days between transaction and filing
        df['filing_delay_days'] = (
            pd.to_datetime(df['filingDate']) - pd.to_datetime(df['transactionDate'])
        ).dt.days
        
        # Flag if delayed more than 30 days
        df['late_filing_warning'] = df['filing_delay_days'] > 30
        
        flagged_count = df['late_filing_warning'].sum()
        if flagged_count > 0:
            logger.info(f"Flagged {flagged_count} records with late filing (>30 days)")
        
        return df
    
    # ========================================================================
    # COMPLETE VALIDATION WORKFLOW
    # ========================================================================
 
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Run complete validation workflow on DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            ValidationResult: Object containing valid, rejected, and flagged records
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            result = ValidationResult()
            result.validation_summary = {'status': 'empty', 'total_records': 0}
            return result  # Early return for empty DataFrames
        
        logger.info(f"Starting validation on {len(df)} records")
        
        result = ValidationResult()
        original_count = len(df)
        rejected_dfs = []
        
        # Step 1: Validate required fields
        df, rejected = self.validate_required_fields(df)
        if not rejected.empty:
            rejected_dfs.append(rejected)
        
        # Step 2: Validate transaction date  
        if not df.empty:
            df, rejected = self.validate_transaction_date(df)
            if not rejected.empty:
                rejected_dfs.append(rejected)
        
        # Step 3: Validate shares
        if not df.empty:
            df, rejected = self.validate_shares(df)
            if not rejected.empty:
                rejected_dfs.append(rejected)
        
        # Step 4: Apply warning flags (non-rejecting)
        if not df.empty:
            df = self.flag_missing_price(df)
            df = self.flag_high_value_transactions(df)
            df = self.flag_old_filing_dates(df)
        
        # Compile results
        result.valid_records = df
        
        if rejected_dfs:
            result.rejected_records = pd.concat(rejected_dfs, ignore_index=True)
        
        # Identify flagged records (valid but with warnings)
        if not df.empty:
            warning_cols = [col for col in df.columns if col.endswith('_warning')]
            if warning_cols:
                flagged_mask = df[warning_cols].any(axis=1)
                result.flagged_records = df[flagged_mask].copy()
        
        # Create summary
        result.validation_summary = {
            'status': 'complete',
            'total_records': original_count,
            'valid_records': len(result.valid_records),
            'rejected_records': len(result.rejected_records),
            'flagged_records': len(result.flagged_records),
            'rejection_rate': len(result.rejected_records) / original_count if original_count > 0 else 0,
            'flag_rate': len(result.flagged_records) / original_count if original_count > 0 else 0
        }
        
        # Log summary
        logger.info("=" * 60)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records:    {original_count}")
        logger.info(f"✓ Valid:          {len(result.valid_records)} "
                f"({len(result.valid_records)/original_count*100:.1f}%)")
        logger.info(f"✗ Rejected:       {len(result.rejected_records)} "
                f"({len(result.rejected_records)/original_count*100:.1f}%)")
        logger.info(f"⚠ Flagged:        {len(result.flagged_records)} "
                f"({len(result.flagged_records)/original_count*100:.1f}%)")
        logger.info("=" * 60)
        
        # Add rejection analysis AFTER validation completes
        if not result.rejected_records.empty:
            rejection_summary = self.get_rejection_reasons(result.rejected_records)
            logger.info("REJECTION REASONS:")
            for reason, count in rejection_summary.items():
                logger.info(f"  • {reason}: {count} records")
            logger.info("=" * 60)
            
            # Save rejected records for manual review
            result.rejected_records.to_csv('rejected_transactions.csv', index=False)
            logger.info("💾 Rejected records saved to: rejected_transactions.csv")
        
        return result   
    
    def get_rejection_reasons(self, rejected_df: pd.DataFrame) -> Dict[str, int]:
        """
        Get summary of rejection reasons
        
        Args:
            rejected_df (pd.DataFrame): DataFrame of rejected records
            
        Returns:
            dict: Counts by rejection reason
        """
        if rejected_df.empty or 'rejection_reason' not in rejected_df.columns:
            return {}
        
        reason_counts = rejected_df['rejection_reason'].value_counts().to_dict()
        return reason_counts


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def validate_dataframe(df: pd.DataFrame) -> ValidationResult:
    """
    Convenience function to validate a DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        
    Returns:
        ValidationResult: Validation results
    """
    validator = DataValidator()
    return validator.validate(df)


def get_valid_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Quick validation that returns only valid records
    
    Args:
        df (pd.DataFrame): DataFrame to validate
        
    Returns:
        pd.DataFrame: Only valid records
    """
    result = validate_dataframe(df)
    return result.valid_records


if __name__ == "__main__":
    # Test script
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    print("Data Validator module loaded successfully")
    validator = DataValidator()
    print(f"✓ Validator ready with rules: {validator.rules}")
