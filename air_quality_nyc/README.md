# Forecasting Air Quality in NYC
Application: Google Colab.\
Skills: Python, ARIMA.\
This is a solo project outside bootcamp. Please refer to the file in this directory for further info.

## Question:
What is the prediction of PM2.5 concentration in April 2017 at New York City?

## Objective:
To create a model that can predict PM2.5 concentration in NYC monthly.

## Method:
### Data cleaning
Data is clean except some missing dates. Since the prediction is average monthly concentration, missing dates is in daily period and scarce, it is not a big deal.

### Exploratory Data Analysis
- From 2000 to Q1 2017, PM2.5 concentration in NYC is trending down.
- From simple EDA, there are no apparent seasonality in yearly, quarterly or monthly. Need statistical tools to ascertain the seasonality.

### Seasonality analysis
- After detrending the data, I made autocorrelation plot for weekly and monthly. I found that there are 3 month seasonality and around 12-13 weekly seasonality.

### Building SARIMA model
- With the help of pmd_arima library, I was able to automatically detect differencing, AR and MA as well as seasonal component needed for SARIMA model. pmdarima.auto_arima select the best differencing based on AIC score. Monthly test set RMSE is 1,644.

### Insights
- Based on the model, it is predicted that in April 2017, PM2.5 concentration is 5.89 μg/m3 (95% CI witihin 1.6 - 10.8)
