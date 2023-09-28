# **Leads Conversion Rate Analysis in B2B SaaS HRIS Company**
Please refer to the deck in this directory for further info.

## Problem:
B2B SaaS HRIS company is faced with stagnant leads-to-customer conversion rate around 25% in 2022, even though marketing spend keeps increasing by 15% per quarter.

## Objective:
To increase leads-to-customer conversion rate to 28% in 2023.

## Method:
### Root cause analysis
Stagnant conversion rate are probably caused by improper targeting and negative leads experience.

### Exploratory Data Analysis
- Overall leads-to-customer conversion rate in 2022 is 26,76%, with highest conversion rate came from referral 32.03% and direct 28.09%
- Overall Unit Economics (LTV/CAC) in 2022 is at 4.82, which is already above standard. The driver mostly are from CPC and Direct. CPC has the lowest marketing spend and at 2nd highest position in terms of revenue generated.
- The presentation is aimed towards sales department and manager, so detail regarding how many marketing channel, how many industry etc. is not presented in detail.

### Leads segmentation and Analysis on Survey
- To improve targeting, I did leads segmentation which results in 3 leads segment: Big Enterprise, Big Business and Small Business. The segmentation is done with Gaussian Mixture Model. To simplify, the model found differences in characteristics and behaviors from leads with >= 140 employee and below. Leads with >=140 employee can be further separated based on their journey on website. 
- Big Enterprise has the highest conversion rate: 33.22%, while Big Business and Small Business have 27.92% and 24.25% respectively.
- Big Enterprise leads 72.6% came from Referral, Big Business leads came from various channel while Small Business leads 42.8% came from CPC.
- From survey results, Big Enterprise and Big Business dislikes Sales' Product Knowledge and Response Time, While Small Business dislikes Pre-Sales Information.
- From analysis of leads journey on company website, Big Enterprise leads only visit 1-2 pages max, they mostly only visit form to set a meeting. On contrary, Big Business and Small Business leads hop to many different pages and sometimes go back to previously visited page.

### Impact Analysis
- From linear regression, it is found that Direct marketing channel CPC and direct marketing channel are the most efficient in gaining leads or customer. However, only Direct Marketing channel has acceptable conversion rate in 28%. Referral also has high conversion rate but is the worst in terms of marketing spend efficiency.
- To achieve 28% conversion rate, it is estimated that company needs to improve conversion rate from leads F&B <140 employee, Tech <140 employee, education <140 employee, retail <140 employee, retail >= 140 employee, tech >= 140 employee and finance >=140 employee by 1.06x. Why them? Because they are the 80% leads in company.

### Recommendation
- Invest more on Direct Marketing Channel
- Speed up response time and sales product knowledge to handle leads with >=140 employee.
- Improve pre-sales information to handle leads with <140 employee. One way to do this is by improving the website so leads don't need to travel to many pages to obtain information they want and improve feature/portfolio pages.

