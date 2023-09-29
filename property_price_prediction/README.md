# Property Listing Analysis and Price Prediction
Application: Google Spreadsheet.\
Skills: Data cleaning, exploratory data analysis, correlation study, multiple linear regression.

Please refer to the pdf deck in this directory for further info.

## Business Question
1. Eventhough high-priced properties net company the most profit, they are hard to sell. What is considered luxury property and affordable property in company data?
2. User is looking for property with 3 rooms, 4 bathrooms, 3 car park and 2200 sqft. What price can company offer to this user?

## Objective:
1. To know what are the characteristics of luxury property and affordable property.
2. To create model for price prediction and predict the price based on above user input.

## Method:
### Data cleaning
1. Remove extra whitespace and duplicates.
2. Add filter.
3. Remove rows with missing price data.
4. Impute missing data in other column if applicable.
5. Remove irrelevant character in columns value.
6. Convert all measurement metrics into 1 same metrics.
7. Changing data type.
8. Remove illogical data such as property price that is below RM1000.
9. Remove outlier.

### Exploratory Data Analysis
Please refer to deck for further detail.

### Insights
- Luxury property (Q3-Q4 in price) have median room count of 5, median bathroom count of 5, median carpark of 2, price range between RM2,3M to RM130M and median size of 3713,5 sqft.
- Affordable property (Q1-Q2 in price) have median room count of 3, median bathroom count of 2, median carpark count of 2, price range between RM690K to RM1,25M and median size of 1389 sqft.
- Size has the strongest positive correlation to price.
- After checking for statistical assumption for linear regression (no strong multicolinearity, normal error distribution, autocorrelation(not applied) and heteroskedasticity), the formula for multiple linear regression is calculated and based on the formula, the company can offer RM2.210.650 to user.
