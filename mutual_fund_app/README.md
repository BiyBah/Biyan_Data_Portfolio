# Mutual Fund Investment App Cluster Analysis and Churn Prediction
Application: Google Colab.\
Skills: Python, data cleaning, exploratory data analysis, PCA, clustering analysis (K-means), logistic regression, cost-benefit analysis.\
Please refer to the pdf deck in this directory for further info.

## Objectives: 
- Recommend segmented thematic campaign for next month, based on customer preference.
- Select 30% the most potential customer to benefit from marketing campaign.
- Create projection of how profitable the campaign is.

## Method:
### Data cleaning
1. Merge the datasets first before cleaning.
2. Handle missing data: change the blank cell with no in used_referral column. For machine learning purpose later, this will be changed to 0 and 1.
3. No duplicate data.
4. Change date-like column to datetime.
5. Outliers are not removed and will be handled by power transformation.

### Exploratory Data Analysis Info
- 61% users are male.
- 61,9% users don't use referral code.
- Mostly have age between 20-25.
- 53,6% is "Pelajar".
- 62,1% income source is "Gaji".
- 42,7% have income <10 juta.
- Mostly have Pasar Uang in their portfolio (6808 users).

### Cluster Analysis
- Feature selection & engineering
- Dimensionality reduction with PCA to visualize cluster
- K-means clustering after analyzing silhouette and elbow method to choose cluster
- Interpretation.

### Cluster Analysis Insights & Recommendation
- There are 8 cluster, most users in company never invested in any product (6954 users.)
- For this user, company need to remind with pop-up message and make them motivated by giving info about investment result simulation.
- Please refer to deck for further detail.

### Predicting the most beneficial user by using classification model
- To obtain such user, I need to know which user is most likely to churn. That churning user will be targeted by campaign, hoping that they will not churn.
- Step 1: Feature engineering and feature selection.
- Step 2: Model building, model selection and evaluation.
- Step 2a: train test split.
- Step 2b: power transform high skew column.
- Step 2c: recursive feature elimination.
- Step 2d: hyperparameter tuning.
- Step 2e: compare f1-score on test-set.
- Step 3: Obtain prediction probability.
- It is found that logistic regression gave the best f1-score by 0,716.

### Cost-Benefit Analysis
- It is calculated by substracting benefit of making people do transaction and cost of marketing campaign for each cluster.
- Average buy and sell per month is obtained on each cluster.
- Target the top 0.3 percentile user who predicted to churn.
- The result is it is recommended to do campaign only in cluster 1, 4, 5, 6 and 8 with the potential return of Rp 3.330.578,-
