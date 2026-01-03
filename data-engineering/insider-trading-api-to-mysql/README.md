# Insider Trading Data Pipeline
## Finnhub API → MySQL Database

A production-ready data engineering pipeline that extracts insider trading data from Finnhub API, applies comprehensive data cleaning and validation, and loads it into Azure MySQL database.

---

## 📋 Project Overview

**Objective**: Build an automated ETL pipeline to track insider trading activities for the "Magnificent 10" technology stocks.

**Data Source**: [Finnhub Stock API](https://finnhub.io/)  
**Target Database**: Azure MySQL  
**Execution Environment**: Google Colab (manual MVP)  
**Pipeline Version**: v1.0.0

### Tracked Stocks (Magnificent 10)
- Apple (AAPL)
- Microsoft (MSFT)
- Alphabet (GOOGL)
- Amazon (AMZN)
- NVIDIA (NVDA)
- Meta Platforms (META)
- Tesla (TSLA)
- Broadcom (AVGO)
- Advanced Micro Devices (AMD)
- Palantir Technologies (PLTR)

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Finnhub API    │
│  (60 calls/min) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  API Client     │
│  - Auth         │
│  - Rate Limit   │
│  - Retry Logic  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Data Pipeline   │
│  Stage 1: Raw   │
│  Stage 2: Text  │
│  Stage 3: Feat  │
│  Stage 4: ML    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Validators     │
│  - Required     │
│  - Date Rules   │
│  - Warnings     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Azure MySQL DB  │
│  - stocks       │
│  - insiders     │
│  - transactions │
│  - logs         │
└─────────────────┘
```

---

## 🗄️ Database Schema

### Star Schema Design

**Dimension Tables:**
- `stocks` - Stock symbols and company info
- `insiders` - Normalized insider names and relationships

**Fact Table:**
- `insider_transactions` - Transaction records with metrics

**Metadata Table:**
- `data_collection_log` - Pipeline run tracking

#### Key Features:
- ✅ Foreign key constraints
- ✅ Unique constraint for deduplication
- ✅ Optimized indexes for queries
- ✅ AUTO_INCREMENT primary keys
- ✅ UTF-8 character encoding

See [schema.sql](schema.sql) for full DDL.

---

## 📊 Data Pipeline

### 4-Stage Transformation

**Stage 1: Raw Data Cleaning**
- Convert date strings to datetime objects
- Parse numeric fields (shares, price, value)
- Handle null/missing values
- Type coercion with error handling

**Stage 2: Text Normalization**
- Standardize insider names (title case, trim whitespace)
- Map transaction codes to readable labels (P→Purchase, S→Sale)
- Normalize relationships to controlled vocabulary (CEO, CFO, Director)

**Stage 3: Feature Engineering**
- Calculate `transaction_value` (shares × price)
- Derive `direction` field (buy/sell/other)
- Flag zero-price transactions (`is_grant_option`)
- Extract date components (year, month, quarter)

**Stage 4: Scikit-learn Pipeline**
- Median imputation for missing prices
- Standardization (optional, for analysis)
- Persist fitted pipeline with joblib

---

## ✅ Data Quality Validation

### Rejection Rules (Critical)
- ❌ Missing required fields (symbol, transaction_date)
- ❌ Transaction date > today + 7 days
- ❌ Negative or null shares

### Warning Flags (Non-blocking)
- ⚠️ Missing price (common for grants/options)
- ⚠️ Transaction value > $1 billion
- ⚠️ Filing delay > 30 days

**All rejected records are logged with reasons.**

---

## 🚀 Setup Instructions

### 1. Prerequisites

**Accounts Needed:**
- Finnhub API account (free tier)
- Azure MySQL database
- Google Colab account

### 2. Database Setup

```bash
# Connect to Azure MySQL
mysql -h yourserver.mysql.database.azure.com -u admin_user -p

# Create database
CREATE DATABASE insider_transactions;
USE insider_transactions;

# Run schema script
SOURCE schema.sql;

# Verify tables created
SHOW TABLES;
```

### 3. Configure Colab Secrets

In Google Colab, add these secrets (🔑 → Secrets):

```
FINNHUB_API_KEY          = your_finnhub_token
AZURE_MYSQL_HOST         = yourserver.mysql.database.azure.com
AZURE_MYSQL_USER         = your_admin_username
AZURE_MYSQL_PASSWORD     = your_password
```

### 4. Upload Project Files

Upload these files to Colab session:
- `config.py`
- `finnhub_client.py`
- `data_pipeline.py`
- `validators.py`
- `database.py`
- `pipeline_logger.py`

### 5. Run the Pipeline

Open `main.ipynb` in Google Colab and execute cells:

```python
# First time: Historical load (24 months)
run_historical_load()

# Subsequent runs: Incremental load (daily)
run_incremental_load(days_back=1)
```

---

## 📁 File Structure

```
insider-trading-api-to-mysql/
│
├── schema.sql              # MySQL DDL (tables, indexes, seed data)
├── config.py               # Configuration and constants
├── finnhub_client.py       # API client with rate limiting
├── data_pipeline.py        # 4-stage transformation pipeline
├── validators.py           # Data quality validation
├── database.py             # MySQL operations layer
├── pipeline_logger.py      # Metadata logging
├── main.ipynb              # Main orchestration notebook (Colab)
└── README.md               # This file
```

---

## 🔄 Pipeline Workflows

### Historical Load (First Run)

**Purpose**: Load 24 months of historical data (2024-01-01 to today)

**Steps**:
1. Fetch all transactions for Magnificent 10 stocks
2. Process through 4-stage pipeline
3. Fit and save scikit-learn pipeline
4. Validate all records
5. Bulk insert to database (INSERT IGNORE for duplicates)
6. Log run metadata

**Expected Volume**: ~5,000-15,000 transactions (varies by insider activity)

### Incremental Load (Daily)

**Purpose**: Fetch new transactions from previous day

**Steps**:
1. Fetch yesterday's transactions only
2. Process using saved pipeline (no refitting)
3. Validate records
4. Insert new records (duplicates auto-skipped)
5. Log run metadata

**Expected Volume**: 0-50 new transactions per day

---

## 📈 Monitoring & Logging

### Run Metadata Tracked

Every pipeline run logs:
- ⏰ Run timestamp
- 📊 Symbols queried
- 📅 Date range
- 🔢 Records fetched/inserted/skipped
- 📞 API calls made
- ✅ Status (success/partial/failed)
- ❌ Error messages
- 🏷️ Pipeline version

### Query Logs

```python
# View recent runs
pipeline_logger.log_run_summary(run_logger)

# Get statistics
stats = run_logger.get_run_statistics()
```

### Database Statistics

```python
# Check database contents
stats = db.get_database_stats()
print(stats)
# Output: {'stocks': 10, 'insiders': 847, 'transactions': 12543, ...}
```

---

## 🔧 Configuration

### Modify Stock List

Edit `config.py`:

```python
MAGNIFICENT_10_SYMBOLS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
    'META', 'TSLA', 'AVGO', 'AMD', 'PLTR'
]
```

### Adjust Date Range

```python
FROM_DATE = '2024-01-01'  # Fixed start date
TO_DATE = date.today().isoformat()
```

### Validation Rules

```python
VALIDATION_RULES = {
    'required_fields': ['symbol', 'transaction_date'],
    'max_transaction_value': 1_000_000_000,
    'max_future_days': 7
}
```

---

## 🛡️ Error Handling

### API Errors
- ✅ Automatic retry (3 attempts with exponential backoff)
- ✅ Rate limiting enforcement (60 calls/min)
- ✅ Timeout handling (30 seconds)
- ✅ Continue on error (partial success mode)

### Database Errors
- ✅ Connection pooling for reliability
- ✅ Transaction rollback on failure
- ✅ Duplicate detection via UNIQUE constraint

### Data Quality
- ✅ Rejected records logged with reasons
- ✅ Validation summary printed
- ✅ Partial success tracking

---

## 📊 Sample Queries

### Recent Insider Transactions

```sql
SELECT 
    s.symbol,
    i.normalized_name,
    t.transaction_date,
    t.direction,
    t.shares,
    t.price,
    t.transaction_value
FROM insider_transactions t
JOIN stocks s ON t.stock_id = s.id
JOIN insiders i ON t.insider_id = i.id
WHERE t.transaction_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY t.transaction_date DESC;
```

### Top Insiders by Transaction Volume

```sql
SELECT 
    i.normalized_name,
    i.relationship,
    COUNT(*) as transaction_count,
    SUM(t.transaction_value) as total_value
FROM insider_transactions t
JOIN insiders i ON t.insider_id = i.id
GROUP BY i.id
ORDER BY total_value DESC
LIMIT 10;
```

### Buy vs Sell Analysis

```sql
SELECT 
    s.symbol,
    t.direction,
    COUNT(*) as count,
    SUM(t.transaction_value) as total_value
FROM insider_transactions t
JOIN stocks s ON t.stock_id = s.id
WHERE t.transaction_date >= '2024-01-01'
GROUP BY s.symbol, t.direction
ORDER BY s.symbol, t.direction;
```

---

## 🚧 Known Limitations

1. **Manual Execution**: MVP requires manual notebook runs (Phase 2: automation with Cloud Scheduler)
2. **Free Tier API**: Limited to 60 calls/min (sufficient for 10 stocks)
3. **Price Imputation**: Median strategy may not be ideal for all cases
4. **Name Matching**: Simple text matching; may miss insider merges

---

## 🔮 Future Enhancements

### Phase 2 - Automation
- [ ] Google Cloud Functions for scheduled runs
- [ ] Cloud Scheduler for daily triggers
- [ ] Email alerts on failures

### Phase 3 - Analysis
- [ ] dbt models for aggregations
- [ ] Looker Studio dashboard
- [ ] Anomaly detection for unusual activity

### Phase 4 - Expansion
- [ ] Add more stock symbols
- [ ] Form 4 filing links
- [ ] Sentiment analysis from SEC filings

---

## 📚 Dependencies

```
pandas >= 1.5.0
scikit-learn >= 1.2.0
mysql-connector-python >= 8.0.0
requests >= 2.28.0
joblib >= 1.2.0
```

Install in Colab:
```bash
!pip install mysql-connector-python pandas scikit-learn joblib requests
```

---

## 👤 Author

**Data Engineering Project**  
Portfolio: Analytics & Data Engineering  
Tech Stack: Python, MySQL, Finnhub API, Google Colab

---

## 📄 License

Educational project - Part of analytics portfolio

---

## 🐛 Troubleshooting

### Issue: API Connection Failed
**Solution**: Check `FINNHUB_API_KEY` in Colab Secrets. Get new key from finnhub.io

### Issue: Database Connection Failed
**Solution**: 
1. Verify Azure MySQL credentials
2. Check firewall rules (allow Colab IPs)
3. Test connection: `telnet yourserver.mysql.database.azure.com 3306`

### Issue: Duplicate Records
**Solution**: This is normal! `INSERT IGNORE` automatically skips duplicates. Check `records_skipped` in logs.

### Issue: All Records Rejected
**Solution**: Check validation logs for rejection reasons. Common issues:
- Missing transaction_date
- Future dates
- Invalid shares

---

## 📞 Support

For issues or questions:
1. Check `data_collection_log` table for error messages
2. Review validation summary for rejected records
3. Enable DEBUG logging: `logging.basicConfig(level=logging.DEBUG)`

---

**Last Updated**: 2026-01-02  
**Version**: 1.0.0
