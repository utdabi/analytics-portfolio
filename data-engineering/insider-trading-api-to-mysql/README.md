# Insider Trading Data Pipeline

An ETL pipeline that extracts insider trading data from the Finnhub API, transforms and validates it, and loads it into an Azure MySQL database for analytics.

## 📋 Project Overview

This project demonstrates end-to-end data engineering capabilities by:
- **Extracting** insider trading data from Finnhub's public API for the "Magnificent 10" tech stocks
- **Transforming** semi-structured JSON data into a clean, standardized format
- **Loading** validated data into a cloud-hosted MySQL database
- **Maintaining** data quality through validation and duplicate prevention

**Data Coverage**: 10 major tech stocks (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, AVGO, AMD, PLTR)  
**Historical Range**: 24 months of insider transactions  
**Update Frequency**: As needed manually triggered

---

## 🎯 Key Features

### RESTful API Integration
- Programmatic data collection from Finnhub public API
- Rate-limited requests with automatic retry logic
- Bulk fetching across multiple stock symbols

### JSON Data Transformation
- Semi-structured JSON to structured relational format
- Data type standardization and validation
- Calculated fields (transaction value = shares × price)
- Missing value handling with conditional logic

### Cloud MySQL Database
- Azure cloud hosted MySQL 8.4 database
- Duplicate prevention and data integrity checks

---

## 🛠️ Technology Stack

**Language**: Python 3.12  
**Database**: MySQL 8.4 (Azure Cloud)  
**Development Environment**: Google Colab  

**Core Libraries**:
- `requests` – API data extraction
- `pandas` – Data transformation and manipulation
- `mysql-connector-python` – Database connectivity
- `datetime` – Date handling and range calculations

**External Services**:
- Finnhub API – Insider trading data source
- Azure MySQL – Cloud database hosting

---

## 🚀 Getting Started

### Prerequisites

1. **Finnhub API Key**  
   Register at [finnhub.io](https://finnhub.io) and obtain a free API key

2. **Azure MySQL Database**  
   - MySQL 8.4 instance

### Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd insider-trading-api-to-mysql
   ```

2. **Configure Secrets in Google Colab**  
   Navigate to Secrets (🔑 icon) and add:
   - `FINNHUB_API_KEY`
   - `AZURE_MYSQL_HOST`
   - `AZURE_MYSQL_USER`
   - `AZURE_MYSQL_PASSWORD`

3. **Initialize Database Schema**
   ```sql
   -- Run schema.sql in your MySQL client
   SOURCE schema.sql;
   ```

4. **Open the Notebook**  
   Upload `Insider_Trading_API_Pipeline.ipynb` to Google Colab and run cells sequentially

---

## 📄 License

This project is available for educational and portfolio purposes.
