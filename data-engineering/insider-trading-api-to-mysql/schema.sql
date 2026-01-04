-- DROP DATABASE insider_transactions;

-- CREATE DATABASE insider_transactions;

-- USE insider_transactions;

CREATE TABLE IF NOT EXISTS insider_transactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    insider_name VARCHAR(200),
    transaction_date DATE NOT NULL,
    filing_date DATE,
    transaction_code VARCHAR(5),
    shares INT,
    price DECIMAL(10,2),
    transaction_value DECIMAL(15,2),
    change_amount INT,
    filing_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE KEY unique_txn (symbol, insider_name(100), transaction_date, shares, filing_id),
    
    -- Indexes for common queries
    INDEX idx_symbol (symbol),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_insider (insider_name(100))
);