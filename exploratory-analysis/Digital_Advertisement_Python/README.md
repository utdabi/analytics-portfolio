# Digital Advertisement Analytics – EDA & Visualization

## 📊 Project Overview

Comprehensive exploratory data analysis of 12,000 digital ad impressions to identify cost drivers and performance patterns across ad formats, ratings, campaigns, and seasonal trends. Delivered actionable insights for platform optimization and revenue growth through multi-dimensional EDA and strategic data imputation techniques.

---

## 🎯 Business Problem

Digital advertising platforms must optimize their ad inventory mix to maximize revenue while maintaining advertiser ROI. This analysis addresses:
- Which ad formats and placements drive the highest revenue?
- How do seasonal factors impact campaign performance?
- What categories offer untapped growth opportunities?
- How should missing data be handled to preserve analytical integrity?

---

## 🔍 Approach & Methodology

### **Data Loading & Object-Oriented Architecture**
- Loaded data from class-based pickle files using Python's object-oriented structure
- Handled 12,000+ ad impression records with multi-dimensional attributes
- Preserved data relationships through proper deserialization

### **Exploratory Data Analysis (EDA)**
Conducted systematic exploration across multiple dimensions:
- **Ad Format Analysis**: Distribution and performance by format type (Display, Video, Native)
- **Rating Analysis**: Click-through rates and costs by ad quality rating
- **Campaign Analysis**: Performance patterns across different campaign types
- **Seasonal Analysis**: Temporal trends affecting CTR and costs
- **Category Analysis**: Revenue potential across industry verticals (Finance, Travel, etc.)

### **Strategic Missing Value Imputation**
Applied domain-informed imputation strategies to preserve data integrity:
- **Numerical variables**: Used `groupby()` with mean/median based on distribution characteristics
- **Categorical variables**: Applied mode imputation within logical groupings
- **Rationale**: Maintained category-specific patterns rather than applying global imputation

### **Multi-Dimensional Visualization**
- Cost distribution analysis by format and rating
- Click-through rate trends across seasons
- Revenue comparison across categories
- Format mix analysis for optimization opportunities

---

## 💡 Key Findings & Insights

### **1. Ad Performance Influencers**
Ad performance is shaped by **ad sub-type** and **seasonal factors** affecting click-through rates. Analysis revealed that format selection and timing are critical levers for campaign success.

### **2. Platform Mix Modernization Opportunity**
**Recommendation**: Ad platform should modernize format mix to promote **Native (InApp) placements** for higher revenue. Native ads demonstrated superior engagement and revenue potential compared to traditional display formats.

### **3. Seasonal Impact on Campaign Optimization**
Seasonal trends influence both **CTR and costs**, emphasizing the importance of strategic timing for optimal campaign results. Campaign scheduling should align with identified seasonal peaks.

### **4. Category Expansion Strategy**
**Strategic Opportunity**: Increase adoption across **Finance and Travel** categories, which show high ROI potential. Diversifying revenue streams in these verticals would increase overall platform value and reduce dependency on saturated categories.

---

## 🛠️ Technology Stack

**Language**: Python 3.x

**Core Libraries**:
- `pandas` – Data manipulation and groupby aggregations
- `numpy` – Numerical operations
- `matplotlib` / `seaborn` – Data visualization
- `pickle` – Object deserialization

**Techniques**:
- Exploratory Data Analysis (EDA)
- Statistical imputation (mean, median, mode)
- Multi-dimensional analysis
- Data visualization and storytelling

---

## 📈 Key Metrics

- **Dataset Size**: 12,000 digital ad impressions
- **Analysis Dimensions**: 5+ (format, rating, campaign, season, category)
- **Imputation Strategy**: Group-based mean/median (numerical) + mode (categorical)
- **Business Impact**: Identified Native (InApp) format as primary revenue growth opportunity

---

## 🚀 How to Run

This notebook is ready to execute in Google Colab with no additional setup required.

---

## 📊 Analysis Highlights

### Data Quality & Preparation
- Loaded object-oriented pickle file structure successfully
- Applied strategic imputation preserving category-level patterns
- Validated data integrity before analysis

### Exploratory Data Analysis
- Examined distributions across all key dimensions
- Identified outliers and data quality issues
- Generated summary statistics by groupings

### Visualization & Insights
- Created multi-dimensional plots showing cost drivers
- Highlighted format mix opportunities through comparative analysis
- Demonstrated seasonal patterns affecting CTR


---

## 👨‍💻 Author

**Abhinav Mahajan**  
MS Business Analytics | UT Dallas  
[GitHub](https://github.com/utdabi)

**Technical Focus**: Data Analytics, Feature Engineering, Python, Statistical Modeling

---

## 📝 Project Context

Developed as part of **Object-Oriented Programming in Python** coursework, demonstrating:
- Proficiency in Python OOP concepts
- Real-world EDA methodology
- Business-focused analytical thinking
- Data storytelling and visualization
- Strategic imputation techniques

---

## 📄 License

This project is available for educational and portfolio purposes.

---

*This analysis demonstrates end-to-end analytical capabilities from data loading through strategic recommendations, emphasizing business impact over technical implementation.*
