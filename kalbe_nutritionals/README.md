# Kalbe Nutritionals x Rakamin Academy Project-Based Internship
Application: Google Colab, DBeaver, PostgreSQL
Skills: SQL, Exploratory Data Analysis, Clustering Analysis, ARIMA, Dashboard

Please refer to the pdf deck in this directory for further info.

## Business Task:
- Inventory team asks to predict daily stock quantity needed.
- Marketing team asks customer segmentation that will be used to give personalized promotion and sales treatment.
- Create dashboard to monitor sales.

## Method:
### Data ingestion
Ingest csv files to Dbeaver

### Query and Exploratory Data Analysis
Please refer to deck for further detail.

### Create Dashboard
[Dashboard Link](https://public.tableau.com/app/profile/biyan.bahtiar.ramadhan/viz/KalbeNutritionalsxRakaminSalesDashboard/Dashboard1)

### Create ARIMA model
- Prepare the data in Google Colab.
- Train-test split.
- Determine seasonality. using ACF plot I found no seasonality.
- Determine differencing and create model automatically using pmd.auto_arima.
- Plot diagnostics.
- Calculate train and test set RMSE.
- Make out-of-sample prediction.\
Please refer to deck for further detail.

### Create customer segmentation
- Prepare the data in Google Colab, including merging datasets.
- Remove outlier.
- Preprocess the data using standard scaler.
- Decide number of cluster needed using elbow method and silhouete score.
- Interpret the data based on characteristics of each cluster.\
Please refer to deck for futher detail.
