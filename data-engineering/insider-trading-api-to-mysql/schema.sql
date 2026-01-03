-- ============================================================================
-- Insider Trading Data Pipeline - MySQL Schema
-- Database: insider_transactions
-- ============================================================================

CREATE DATABASE insider_transactions;

USE insider_transactions;

-- Drop tables if they exist (for clean setup)
-- DROP TABLE IF EXISTS insider_transactions;
-- DROP TABLE IF EXISTS data_collection_log;
-- DROP TABLE IF EXISTS insiders;
-- DROP TABLE IF EXISTS stocks;


-- ============================================================================
-- DIMENSION TABLE: stocks
-- ============================================================================
CREATE TABLE stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol (symbol)
);

-- ============================================================================
-- DIMENSION TABLE: insiders
-- ============================================================================
CREATE TABLE insiders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    normalized_name VARCHAR(255) UNIQUE NOT NULL,
    raw_name VARCHAR(255),
    relationship VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_normalized_name (normalized_name)
);

-- ============================================================================
-- FACT TABLE: insider_transactions
-- ============================================================================
CREATE TABLE insider_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    insider_id INT NOT NULL,
    transaction_date DATE NOT NULL,
    filing_date DATE,
    transaction_type VARCHAR(50),
    shares INT NOT NULL,
    price DECIMAL(15,4),
    transaction_value DECIMAL(20,2),
    ownership_type VARCHAR(20),
    direction VARCHAR(10),
    is_grant_option BOOLEAN DEFAULT FALSE,
    price_imputed BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE,
    FOREIGN KEY (insider_id) REFERENCES insiders(id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_transaction (stock_id, insider_id, transaction_date, transaction_type, shares),
    
    INDEX idx_stock_date (stock_id, transaction_date),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_insider (insider_id)
);

-- ============================================================================
-- METADATA TABLE: data_collection_log
-- ============================================================================
CREATE TABLE data_collection_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_timestamp DATETIME NOT NULL,
    symbols_queried TEXT,
    from_date DATE,
    to_date DATE,
    records_fetched INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_skipped INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    api_calls_made INT DEFAULT 0,
    status VARCHAR(50),
    error_message TEXT,
    pipeline_version VARCHAR(20),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run_timestamp (run_timestamp),
    INDEX idx_status (status)
);

-- ============================================================================
-- SEED DATA: Magnificent 10 Stocks
-- ============================================================================
INSERT INTO stocks (symbol, company_name, sector) VALUES
('AAPL', 'Apple Inc.', 'Technology'),
('MSFT', 'Microsoft Corporation', 'Technology'),
('GOOGL', 'Alphabet Inc.', 'Technology'),
('AMZN', 'Amazon.com Inc.', 'Consumer Cyclical'),
('NVDA', 'NVIDIA Corporation', 'Technology'),
('META', 'Meta Platforms Inc.', 'Technology'),
('TSLA', 'Tesla Inc.', 'Automotive'),
('AVGO', 'Broadcom Inc.', 'Technology'),
('AMD', 'Advanced Micro Devices Inc.', 'Technology'),
('PLTR', 'Palantir Technologies Inc.', 'Technology');