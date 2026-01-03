"""
Data Cleaning and Transformation Pipeline
4-stage pipeline: Raw Cleaning -> Normalization -> Feature Engineering -> Scikit-learn
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import joblib
import os

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

import config

logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Comprehensive data cleaning and transformation pipeline for insider transactions
    
    Stages:
    1. Raw data cleaning (type conversions, null handling)
    2. Text normalization (names, transaction types)
    3. Feature engineering (calculated fields, flags)
    4. Scikit-learn pipeline (imputation, optional scaling)
    """
    
    def __init__(self):
        """Initialize pipeline"""
        self.sklearn_pipeline = None
        self.fitted = False
        self.fitted_features = []  # Track which features pipeline was fitted on
        logger.info("Data pipeline initialized")
    
    # ========================================================================
    # STAGE 1: RAW DATA CLEANING
    # ========================================================================
    
    def clean_raw_data(self, transactions: List[Dict]) -> pd.DataFrame:
        """
        Stage 1: Convert raw API response to clean DataFrame with proper types
        
        Args:
            transactions (list): Raw transaction dictionaries from API
            
        Returns:
            pd.DataFrame: Cleaned DataFrame with proper data types
        """
        if not transactions:
            logger.warning("No transactions to clean")
            return pd.DataFrame()
        
        logger.info(f"Stage 1: Cleaning {len(transactions)} raw transactions")
        
        # Convert to DataFrame
        df = pd.DataFrame(transactions)
        
        # Date conversions and standardization
        date_columns = ['transactionDate', 'filingDate']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Standardize column names (camelCase → snake_case)
        if 'transactionDate' in df.columns:
            df['transaction_date'] = df['transactionDate']
        if 'filingDate' in df.columns:
            df['filing_date'] = df['filingDate']
        if 'transactionCode' in df.columns:
            df['transaction_code'] = df['transactionCode']
        
        # Numeric conversions and standardization
        # Handle both 'share' and 'change' (API variations)
        # Keep 'change' for direction determination, create 'shares' as absolute value
        if 'change' in df.columns:
            df['change'] = pd.to_numeric(df['change'], errors='coerce')
            df['shares'] = df['change'].abs()  # Shares is always positive (quantity)
        elif 'share' in df.columns:
            df['shares'] = pd.to_numeric(df['share'], errors='coerce').abs()
        else:
            df['shares'] = 0
            df['change'] = 0
        
        # Handle price field - API uses 'transactionPrice', we standardize to 'price'
        if 'transactionPrice' in df.columns:
            df['price'] = pd.to_numeric(df['transactionPrice'], errors='coerce')
        elif 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        else:
            df['price'] = np.nan
        
        # Replace 0 prices with NaN (common for grants/options)
        if 'price' in df.columns and pd.notna(df['price']).any():
            df['price'] = df['price'].replace(0, np.nan)
        
        # Handle missing values in text fields
        text_columns = ['name', 'relationship', 'transaction_code']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')
        
        logger.info(f"✓ Stage 1 complete: {len(df)} records cleaned")
        return df
    
    # ========================================================================
    # STAGE 2: TEXT NORMALIZATION
    # ========================================================================
    
    def normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Stage 2: Normalize text fields (names, transaction types, relationships)
        
        Args:
            df (pd.DataFrame): DataFrame with raw text fields
            
        Returns:
            pd.DataFrame: DataFrame with normalized text
        """
        if df.empty:
            return df
        
        logger.info(f"Stage 2: Normalizing text fields")
        
        # Normalize insider names
        if 'name' in df.columns:
            df['normalized_name'] = df['name'].apply(self._normalize_name)
            df['raw_name'] = df['name']
        
        # Normalize transaction types
        if 'transaction_code' in df.columns:
            df['transaction_type'] = df['transaction_code'].apply(
                config.normalize_transaction_type
            )
        
        # Normalize relationships/titles
        if 'relationship' in df.columns:
            df['normalized_relationship'] = df['relationship'].apply(
                config.normalize_relationship
            )
        
        logger.info(f"✓ Stage 2 complete: Text fields normalized")
        return df
    
    def _normalize_name(self, name: str) -> str:
        """
        Normalize insider name (handle various formats)
        
        Args:
            name (str): Raw name string
            
        Returns:
            str: Normalized name
        """
        if not name or pd.isna(name):
            return 'Unknown'
        
        # Strip whitespace
        name = name.strip()
        
        # Handle "LastName, FirstName" format
        if ',' in name:
            parts = name.split(',')
            if len(parts) == 2:
                last, first = parts
                name = f"{first.strip()} {last.strip()}"
        
        # Title case
        name = name.title()
        
        return name
    
    # ========================================================================
    # STAGE 3: FEATURE ENGINEERING
    # ========================================================================
    
    def engineer_features(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Stage 3: Create calculated fields and feature flags
        
        Args:
            df (pd.DataFrame): DataFrame with normalized data
            symbol (str): Stock symbol for this batch
            
        Returns:
            pd.DataFrame: DataFrame with engineered features
        """
        if df.empty:
            return df
        
        logger.info(f"Stage 3: Engineering features")
        
        # Add symbol
        df['symbol'] = symbol.upper()
        
        # Calculate transaction value if missing (shares is already absolute)
        if 'price' in df.columns and 'shares' in df.columns:
            df['transaction_value'] = df.apply(
                lambda row: self._calculate_transaction_value(row), 
                axis=1
            )
        
        # Determine direction (buy/sell/other) from transaction code AND change sign
        if 'transaction_code' in df.columns:
            df['direction'] = df.apply(self._determine_direction, axis=1)
        elif 'change' in df.columns:
            # Fallback: use change sign if transaction_code not available
            df['direction'] = df['change'].apply(
                lambda x: 'buy' if x > 0 else 'sell' if x < 0 else 'other'
            )
        
        # Flag grants and option exercises (zero-price transactions)
        if 'transaction_code' in df.columns and 'price' in df.columns:
            df['is_grant_option'] = df.apply(
                lambda row: config.is_grant_or_option(
                    row.get('transaction_code', ''), 
                    row.get('price')
                ),
                axis=1
            )
        
        # Flag imputed prices
        df['price_imputed'] = False
        
        # Extract date components (useful for analysis)
        if 'transaction_date' in df.columns:
            df['transaction_year'] = df['transaction_date'].dt.year
            df['transaction_month'] = df['transaction_date'].dt.month
            df['transaction_quarter'] = df['transaction_date'].dt.quarter
        
        # Determine ownership type (direct/indirect)
        # Finnhub may have this field; if not, default to 'direct'
        if 'ownershipNature' not in df.columns:
            df['ownershipNature'] = 'direct'
        
        logger.info(f"✓ Stage 3 complete: Features engineered")
        return df
    
    def _determine_direction(self, row: pd.Series) -> str:
        """
        Determine transaction direction from transaction code and change sign
        
        Args:
            row (pd.Series): Row with 'transaction_code' and 'change'
            
        Returns:
            str: 'buy', 'sell', or 'other'
        """
        code = row.get('transaction_code', '')
        change = row.get('change', 0)
        
        # Transaction codes that indicate buys
        buy_codes = ['P', 'A', 'M']  # Purchase, Award/Grant, Option Exercise
        # Transaction codes that indicate sells
        sell_codes = ['S', 'D', 'F']  # Sale, Disposition, Tax Payment
        # Other transaction codes
        other_codes = ['G', 'J', 'C', 'W', 'I', 'U']  # Gift, Other, Conversion, Will, In-Kind, Tender
        
        if code in buy_codes and change > 0:
            return 'buy'
        elif code in sell_codes and change < 0:
            return 'sell'
        elif code in other_codes:
            return 'other'
        else:
            # Fallback: use change sign
            return 'buy' if change > 0 else 'sell' if change < 0 else 'other'
    
    def _calculate_transaction_value(self, row: pd.Series) -> Optional[float]:
        """
        Calculate transaction value from shares and price
        Shares is already absolute value, so no need to abs() again
        
        Args:
            row (pd.Series): Row with 'shares' and 'price'
            
        Returns:
            float or None: Transaction value
        """
        shares = row.get('shares')
        price = row.get('price')
        
        if pd.notna(shares) and pd.notna(price):
            return shares * price  # shares is already absolute
        
        return None
    
    # ========================================================================
    # STAGE 4: SCIKIT-LEARN PIPELINE
    # ========================================================================
    
    def fit_sklearn_pipeline(self, df: pd.DataFrame):
        """
        Stage 4: Fit scikit-learn pipeline for numeric imputation
        
        Args:
            df (pd.DataFrame): Training DataFrame
        """
        if df.empty:
            logger.warning("Cannot fit pipeline on empty DataFrame")
            return
        
        logger.info("Stage 4: Fitting scikit-learn pipeline")
        
        # Create pipeline for numeric imputation
        numeric_features = ['price', 'transaction_value']
        
        # Build pipeline
        self.sklearn_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy=config.IMPUTATION_STRATEGY)),
            # Optionally add scaler if needed for analysis
            # ('scaler', StandardScaler())
        ])
        
        # Fit on numeric columns that exist
        available_features = [f for f in numeric_features if f in df.columns]
        
        if available_features:
            # Handle missing values before fitting
            X = df[available_features].copy()
            
            # Check if there's at least some non-null data to fit on
            if X.notna().any().any():
                self.sklearn_pipeline.fit(X)
                self.fitted = True
                self.fitted_features = available_features  # Save which features were fitted
                logger.info(f"✓ Pipeline fitted on features: {available_features}")
            else:
                logger.warning(f"All values in {available_features} are null. Skipping pipeline fitting.")
                logger.info("Pipeline will not perform imputation - all prices are likely grants/options")
        else:
            logger.warning("No numeric features available for pipeline fitting")
    
    def transform_with_sklearn(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply fitted scikit-learn pipeline to transform data
        
        Args:
            df (pd.DataFrame): DataFrame to transform
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        if df.empty or not self.fitted:
            return df
        
        logger.info("Stage 4: Applying scikit-learn transformations")
        
        # Use the SAME features the pipeline was fitted on
        if not self.fitted_features:
            logger.warning("No fitted features found, skipping transformation")
            return df
        
        # Check which fitted features are available in current DataFrame
        available_fitted_features = [f for f in self.fitted_features if f in df.columns]
        
        if not available_fitted_features:
            logger.warning(f"None of the fitted features {self.fitted_features} found in DataFrame")
            return df
        
        # Ensure we have ALL the fitted features
        if len(available_fitted_features) != len(self.fitted_features):
            missing = set(self.fitted_features) - set(available_fitted_features)
            logger.warning(f"Missing fitted features: {missing}. Skipping transformation.")
            return df
        
        # STORE NULL MASKS BEFORE TRANSFORMATION
        null_masks = {}
        for feature in self.fitted_features:
            null_masks[feature] = df[feature].isna()
        
        # Transform using the exact features pipeline was fitted on
        X = df[self.fitted_features].copy()
        X_transformed = self.sklearn_pipeline.transform(X)
        
        # Safety check: Ensure transform output has the expected number of columns
        if X_transformed.shape[1] == 0:
            logger.warning("Transform returned 0 columns. Skipping transformation.")
            logger.warning("This likely means the pipeline was fitted on all-null data.")
            return df
        
        if X_transformed.shape[1] != len(self.fitted_features):
            logger.error(f"Shape mismatch: expected {len(self.fitted_features)} columns, got {X_transformed.shape[1]}")
            return df
        
        # Update original dataframe and mark imputed values
        for i, feature in enumerate(self.fitted_features):
            df[feature] = X_transformed[:, i]
            # Mark values that were null before but are not NaN after imputation
            was_null = null_masks[feature]
            is_now_valid = pd.notna(X_transformed[:, i])
            df.loc[was_null & is_now_valid, 'price_imputed'] = True
        
        logger.info(f"✓ Stage 4 complete: Sklearn transformations applied")
        return df
    
    # ========================================================================
    # PIPELINE PERSISTENCE
    # ========================================================================
    
    def save_pipeline(self, filepath: str = None):
        """
        Save fitted pipeline to disk
        
        Args:
            filepath (str): Path to save pipeline (default: config.PIPELINE_FILE_PATH)
        """
        if not self.fitted:
            logger.warning("Pipeline not fitted yet, nothing to save")
            return
        
        filepath = filepath or config.PIPELINE_FILE_PATH
        
        try:
            # Save both pipeline and fitted_features
            pipeline_data = {
                'pipeline': self.sklearn_pipeline,
                'fitted_features': self.fitted_features
            }
            joblib.dump(pipeline_data, filepath)
            logger.info(f"✓ Pipeline saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save pipeline: {str(e)}")
            raise
    
    def load_pipeline(self, filepath: str = None):
        """
        Load fitted pipeline from disk
        
        Args:
            filepath (str): Path to load pipeline from
        """
        filepath = filepath or config.PIPELINE_FILE_PATH
        
        if not os.path.exists(filepath):
            logger.warning(f"Pipeline file not found: {filepath}")
            return False
        
        try:
            pipeline_data = joblib.load(filepath)
            
            # Handle both old format (just pipeline) and new format (dict with pipeline + features)
            if isinstance(pipeline_data, dict):
                self.sklearn_pipeline = pipeline_data['pipeline']
                self.fitted_features = pipeline_data.get('fitted_features', [])
            else:
                # Old format - just the pipeline
                self.sklearn_pipeline = pipeline_data
                self.fitted_features = []
                logger.warning("Loaded old pipeline format without fitted_features")
            
            self.fitted = True
            logger.info(f"✓ Pipeline loaded from {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to load pipeline: {str(e)}")
            return False
    
    # ========================================================================
    # COMPLETE PIPELINE EXECUTION
    # ========================================================================
    
    def process_transactions(
        self, 
        transactions: List[Dict], 
        symbol: str,
        fit_pipeline: bool = False
    ) -> pd.DataFrame:
        """
        Execute complete 4-stage pipeline on raw transactions
        
        Args:
            transactions (list): Raw transaction dictionaries from API
            symbol (str): Stock symbol
            fit_pipeline (bool): If True, fit sklearn pipeline on this data
            
        Returns:
            pd.DataFrame: Fully processed DataFrame ready for database insertion
        """
        logger.info(f"Processing {len(transactions)} transactions for {symbol}")
        
        # Stage 1: Raw cleaning
        df = self.clean_raw_data(transactions)
        
        if df.empty:
            return df
        
        # Stage 2: Text normalization
        df = self.normalize_text_fields(df)
        
        # Stage 3: Feature engineering
        df = self.engineer_features(df, symbol)
        
        # Stage 4: Scikit-learn pipeline
        if fit_pipeline and not self.fitted:
            self.fit_sklearn_pipeline(df)
        
        if self.fitted:
            df = self.transform_with_sklearn(df)
        
        logger.info(f"✓ Pipeline complete: {len(df)} records processed for {symbol}")
        return df
    
    def process_bulk_transactions(
        self, 
        bulk_data: Dict[str, List[Dict]],
        fit_pipeline: bool = False
    ) -> pd.DataFrame:
        """
        Process transactions for multiple symbols and combine into single DataFrame
        
        Args:
            bulk_data (dict): Dictionary mapping symbols to transaction lists
            fit_pipeline (bool): If True, fit pipeline on first batch
            
        Returns:
            pd.DataFrame: Combined processed DataFrame
        """
        all_processed = []
        
        for i, (symbol, transactions) in enumerate(bulk_data.items()):
            if not transactions:
                logger.info(f"Skipping {symbol}: No transactions")
                continue
            
            # Fit pipeline on first symbol's data
            should_fit = fit_pipeline and i == 0 and not self.fitted
            
            df = self.process_transactions(transactions, symbol, fit_pipeline=should_fit)
            
            if not df.empty:
                all_processed.append(df)
        
        if not all_processed:
            logger.warning("No transactions to combine")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(all_processed, ignore_index=True)
        logger.info(f"✓ Bulk processing complete: {len(combined_df)} total records")
        
        return combined_df


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_pipeline() -> DataPipeline:
    """Create and return new DataPipeline instance"""
    return DataPipeline()


def load_or_create_pipeline() -> DataPipeline:
    """
    Load existing pipeline from disk, or create new one if not found
    
    Returns:
        DataPipeline: Pipeline instance
    """
    pipeline = DataPipeline()
    
    if os.path.exists(config.PIPELINE_FILE_PATH):
        pipeline.load_pipeline()
        logger.info("Using existing fitted pipeline")
    else:
        logger.info("No existing pipeline found, will fit on first run")
    
    return pipeline


if __name__ == "__main__":
    # Test script
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    print("Data Pipeline module loaded successfully")
    pipeline = create_pipeline()
    print("✓ Pipeline ready for use")
