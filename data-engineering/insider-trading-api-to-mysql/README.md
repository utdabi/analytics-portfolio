# Insider Trading Data Pipeline
## Finnhub API → Azure MySQL

An automated ETL pipeline that extracts insider trading data from Finnhub API, applies multi-stage data cleaning and validation, and loads it into Azure MySQL database.

---

## 📋 Project Overview

**Objective**: Track insider trading activities for the "Magnificent 10" technology stocks.

**Data Source**: [Finnhub Stock API](https://finnhub.io/) (Free Tier)  
**Target Database**: Azure MySQL  
**Execution**: Google Colab (manual runs)  
**Tech Stack**: Python, MySQL, pandas, scikit-learn

### Tracked Stocks
AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, AVGO, AMD, PLTR

---

## 🏗️ Architecture

```
Finnhub API (60 calls/min)
    ↓
API Client (rate limiting, retries)
    ↓
4-Stage Data Pipeline (clean → normalize → engineer → ML)
    ↓
Validators (reject bad data, flag warnings)
    ↓
Azure MySQL (star schema)
    ↓
Logging & Monitoring
```

---

## 🗄️ Database Schema

**Star Schema Design:**

- **Dimension Tables**: `stocks`, `insiders`
- **Fact Table**: `insider_transactions`
- **Metadata**: `data_collection_log`

**Key Features:**
- Foreign key constraints
- Automatic deduplication (INSERT IGNORE)
- Optimized indexes
- UTF-8 encoding

See [`schema.sql`](schema.sql) for full DDL.

---

## 📊 Data Pipeline

### 4-Stage Transformation

1. **Raw Cleaning**: Type conversions, null handling, parse dates/numbers
2. **Text Normalization**: Standardize names, map transaction codes
3. **Feature Engineering**: Calculate values, derive direction (buy/sell), extract date parts
4. **ML Pipeline**: Scikit-learn imputation for missing prices, persist with joblib

### Key Transformations

- `change` (can be negative) → `shares` (absolute value, always positive)
- Transaction code + change sign → `direction` (buy/sell/other)
- `shares` × `price` → `transaction_value`

**Important**: Negative values in raw API data represent **sales/disposals** (not errors). The pipeline converts these to positive share quantities with correct direction flags.

---

## ✅ Data Quality Validation

### Rejection Rules (Critical)
- ❌ Missing symbol or transaction_date
- ❌ Transaction date > today + 7 days
- ❌ Shares = 0 or null (after transformation)

### Warning Flags (Non-blocking)
- ⚠️ Missing price (common for grants/options)
- ⚠️ Transaction value > $1B
- ⚠️ Filing delay > 30 days

All rejected records logged with reasons in `rejected_transactions.csv`.

---

## 🚀 Quick Start

### 1. Prerequisites
- Finnhub API key (free account at finnhub.io)
- Azure MySQL database
- Google Colab account

### 2. Database Setup

```sql
-- Run in Azure MySQL portal
CREATE DATABASE insider_transactions;
USE insider_transactions;
SOURCE schema.sql;
```

### 3. Configure Colab Secrets

Add these in Colab (🔑 → Secrets):
```
FINNHUB_API_KEY
AZURE_MYSQL_HOST
AZURE_MYSQL_USER
AZURE_MYSQL_PASSWORD
```

### 4. Upload Files & Run

Upload all `.py` files to Colab, then open [`main.ipynb`](main.ipynb):

```python
# First run: Load 24 months of history
run_historical_load()

# Daily runs: Load yesterday's transactions
run_incremental_load(days_back=1)
```

---

## 📁 Project Structure

```
├── schema.sql              # MySQL database schema
├── config.py               # Configuration & secrets
├── finnhub_client.py       # API client with rate limiting
├── data_pipeline.py        # 4-stage transformation
├── validators.py           # Data quality rules
├── database.py             # MySQL operations
├── pipeline_logger.py      # Run metadata tracking
├── main.ipynb              # Orchestration notebook
└── README.md
```

---

## 🔄 Pipeline Workflows

### Historical Load (First Run)
- Loads 24 months of data (2024-01-01 to today)
- Expected volume: 5,000-15,000 transactions
- Fits and saves scikit-learn pipeline

### Incremental Load (Daily)
- Fetches previous day's transactions
- Expected volume: 0-50 transactions
- Uses saved pipeline (no refitting)

Both modes log metadata to `data_collection_log` table.

---

## 📊 Analytics Dashboard

**Coming Soon**

---

## 🛡️ Error Handling

- **API**: Auto-retry (3 attempts), rate limiting, timeout handling
- **Database**: Connection pooling, transaction rollback, duplicate skipping
- **Data Quality**: Rejected records logged with reasons, validation summaries

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| API Connection Failed | Check `FINNHUB_API_KEY` in Colab Secrets |
| Database Connection Failed | Verify Azure credentials and firewall rules |
| Lock Timeout | Run `UNLOCK TABLES; FLUSH TABLES;` in Azure portal or use `db.unlock_tables()` |
| High Rejection Rate | Check `rejected_transactions.csv` for reasons |
| All Records Skipped | Normal if data already loaded (duplicates ignored) |

---

## 📚 Dependencies

```bash
pip install mysql-connector-python pandas scikit-learn joblib requests
```

**Versions**: Python 3.12, MySQL 8.4

---

## 🔮 Future Enhancements

- **Phase 2**: Automate with Cloud Scheduler + Cloud Functions
- **Phase 3**: dbt models for advanced analytics
- **Phase 4**: Expand to more stocks, add SEC filing links

---

## 📊 Key Metrics

After successful historical load:
- **Stocks**: 10
- **Insiders**: ~800-1,000
- **Transactions**: ~5,000-15,000
- **Date Range**: 2024-01-01 to present
- **Rejection Rate**: <5% (typical)

---

## 📄 License

Educational project