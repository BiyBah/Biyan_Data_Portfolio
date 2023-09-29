# Clothing Ecommerce Analysis
Application: BigQuery, Google Spreadsheet\
Skills: SQL, Root-Cause Analysis, Understanding ERD, Exploratory Data Analysis, BCG matrix, cohort retention analysis, T-test.

Please refer to the pdf deck in this directory for further info.

## Business Question:
- Find the categories with the lowest business growth (revenue and profit) in the past 1 year.
- Understand the current retention performance and push new initiative to boost retention rate.
  
## Method:
- Query data from BigQuery
- Exploratory Data Analysis
- Cohort Analysis
- Root Cause Analysis on why retention rate is low.
- T-test on probable cause.

## Insights
- Jumpsuits & Rompers and Leggings are 2 categories that need to deprioritized based on low market share and low profit growth.
- Monthly retention rate on average is 1,53% in 2022, while yearly retention rate is 8,35% from 2019-2021.
- Based on category, Jeans, Sweaters, Intimates, Pants and Leggings are the top 5 user retention rate in 2022.
- Root-cause analysis found that probably users churn are because several factors. The only data available in datasets are competitor has cheaper price(this is assumed by retail price) or slow delivery.
- While retail price is the same as our price, slow delivery could be a problem since there is difference between delivery time of users who churn and not churn.
- T-test unveiled that there is enough evidence to support that in 2021, people who experiences faster delivery time tend to repeat order in the next year.

## Recommendation
- Depriritize Jumpsuits & Rompers and Leggings.
- Try to reduce delivery time and do A/B testing to ascertain the cause and effect relationship.
