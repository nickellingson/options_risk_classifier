# Options Risk Classifier

A machine learning pipeline to predict whether stock option contracts will expire **in the money (ITM)** or **out of the money (OTM)** using historical options data.

## Overview
This project builds an end-to-end system for option risk classification:
- **Data Ingestion**: Downloaded historical OPRA options data from Polygon S3 flatfiles.
- **Feature Engineering**: Processed contracts with PySpark, extracting strike, expiration, option type, days-to-expiry, moneyness, and underlying prices.
- **Labeling**: Assigned labels based on whether the option finished ITM or not at expiration.
- **Model Training**: Trained a PyTorch neural network classifier on the engineered dataset.

## Tech Stack
- **PySpark** for large-scale feature engineering
- **Polygon API & S3 flatfiles** for data ingestion
- **PyTorch** for model training
- **Pandas / NumPy** for preprocessing and evaluation

## Results
- Achieved strong predictive performance with AUC ~0.98 on the validation set
- Balanced precision and recall across ITM vs OTM classes