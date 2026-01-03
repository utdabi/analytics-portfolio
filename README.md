# Analytics Portfolio

Data analytics projects spanning exploratory analysis, predictive modeling, data engineering, and utilities.

## 📊 Exploratory Analysis

### [Digital Advertisement Analytics](./exploratory-analysis/Digital_Advertisement_Python/)
EDA of 12K ad impressions, identifying cost drivers and revenue opportunities.  
**Tech**: Python | pandas, numpy, matplotlib

## 🎯 Predictive Modeling

### [Hotel Booking Cancellation](./predictive-modeling/Hotel_Booking_R/)
Built decision tree and logistic regression models to predict booking cancellations using real transactional data from a city hotel's booking system in Lisbon, Portugal (76,730 bookings, 21 features), achieving 77% accuracy.

**Tech**: R | Binary Classification, EDA, Feature Engineering


---

## 🔧 Data Engineering

### [Insider Trading API to MySQL](./data-engineering/insider-trading-api-to-mysql/)
Automated ETL pipeline that extracts insider trading data from Finnhub API for the "Magnificent 10" tech stocks, applies 4-stage data cleaning and validation, and loads into Azure MySQL star schema database.

**Features**:
- Rate-limited API client (60 calls/min)
- Multi-stage data pipeline (clean → normalize → engineer → ML)
- Data quality validation with rejection logging
- Scikit-learn imputation with pipeline persistence
- Historical load (24 months) + incremental daily updates

**Tech**: Python, MySQL, Google Colab | pandas, scikit-learn, mysql-connector
