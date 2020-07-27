# Firebase + Cloud Trigger Functions
The project created to be a solution for connecting google cloud trigger functions to Firebase realtime database actions.
Contributor: Karen Danielyan

The files in this repository are:
1. README.md
2. scripts.py
3. main.py
4. google_functions.py
5. ngt_software_engineer_test_example_data (clean columns).csv

the csv file contains an example dataset provided by NextGateTech.

## scripts.py
contains functions:
1) check1: checks the formula: [NPV Per Share] = NPV / [Number of shares outstanding] validity.
for all valuation dates, it reports all isins for which the formula is violated by more than the acceptable threshold and the violation percentage as an error rate.

2) check2: checks the Dividend_Payment_Date.
This function tries to find impossible dividend payment dates, as usually the dividend dates cannot be fixed for a very long time in advance.
Therefore, this function calculates the difference between current date and the dividend payment dates and,
if the difference is larger than the acceptable threshold it reports the date and the isins that violated the rule and the year difference.

3) calculate_corr: calculates the correlations between the time series of each portfolios.
The data points are not evenly available, so it perform forward fill for NA values.
Secondly, in order to have a reliable correlation coefficients for time series data, it is necessary to make sure the time series are stationary.
Therefore, the function performs first order differencing on each ts, then calculates elementwise correlations and reports them.

## main.py

Runs all functions from the scripts.py
!! requires env variable GOOGLE_APPLICATION_CREDENTIALS with the service key path. 


## google_functions.py

significant functions are:
1) agg_for_checks: performs aggregation on check1 or check2 outputs on a given period(in days) and the period end date (as of date)
Reports: 
average_count_per_period: average number of isins that have been reported in the check output per day.
average_error_per_period: average difference for expected NAV_Per_Share and given for check1 over the period,
 or the average year difference between the current date and the dividend pay date.
number_of_available_dates_per_period: the number of valuation dates in the calendar period.

2) agg_for_correlations: Aggregates the correlation outputs.
Reports the minimum and maximum correlated porfolios/isins for each isin except from itself, as well as the correlation coefficients. 

3) perform_all_aggregations: call the necessary functions to aggreate all.

google_functions.py is set up in the Google Cloud functions with a write trigger on the path FileOutputs,
which means if the main.py runs, perform_all_aggregations runs by the trigger.

Thanks!
