# Hotel Booking Cancellation Prediction (R)

Binary classification project predicting which hotel reservations will be canceled, using real booking data from a **city hotel's reservation system in Lisbon, Portugal**

## Data Scope

- **Dataset**: *Hotel Booking Demand Datasets* (Data in Brief, Antonio et al., 2019) [web:36][web:46]
- **Origin**: Actual transactional data from **hotel Property Management System (PMS)** [web:46]
- **Hotels**: City hotel (Lisbon) and Resort hotel (Algarve region), Portugal
- **Analysis focus**: **City hotel (Lisbon) only**
- **Observations**: **76,730 bookings** from the hotel's booking system
- **Features**: **21 predictors** (date columns removed for cross-sectional modeling)

## Modeling Approach

- Target: `is_canceled` (booking canceled vs not canceled)
- Models:
  - **Logistic regression** (baseline, interpretable classification)
  - **Decision tree** (nonlinear patterns and interaction effects)
- Workflow:
  - EDA on city-hotel subset (cancellation rates, lead time, stay length, market segments)
  - Feature engineering from stay lengths, party composition, and booking channels
  - Train/test split and accuracy evaluation

## Results

- **Best model accuracy**: ~**77%** 
- Key takeaway: Cancellation risk is strongly associated with **lead time, deposit type, customer type, and previous cancellations**, enabling more informed overbooking and pricing decisions.

## Tech Stack

- **Language**: R  
- **Libraries**: `Hmisc`, `rpart`, `ggplot2` 
- **Methods**: cross-sectional EDA,Logistic regression, Decision tree, model comparison 

## Notebook

The full analysis is in:

`Hotel_Booking_Cancellation_R.ipynb`


**Data Citation**: Antonio, N., Almeida, A., & Nunes, L. (2019). Hotel booking demand datasets. *Data in Brief*, 22, 41-49.