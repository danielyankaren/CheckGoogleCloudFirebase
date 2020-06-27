import numpy as np
import pandas as pd
import datetime as dt
import firebase_admin
from firebase_admin import db


def init_app():
    firebase_admin.initialize_app(options={
        'databaseURL': 'https://nextgate-b8ba4.firebaseio.com',
    })


def agg_for_checks(check_directory, aggregation_directory, period_days, as_of_date='2018-12-31'):
    '''
    Performs aggregation on the data check outputs.
    Reads the respective path from the database, performs aggregations
    and writes the aggregation results back to the database.
    Args:
        check_directory: str, the path where the check output is stored.
        aggregation_directory: str, the path where the aggregation output should be stored.
        period_days: int, number of days for the aggregation period
        as_of_date: str, period end date.
    '''
    as_of_date2 = pd.to_datetime(as_of_date)
    threshold_date = as_of_date2 - dt.timedelta(days=30)
    data = db.reference('/FileOutputs/{check_directory}'.format(check_directory=check_directory)).get()

    avg_count = 0
    avg_error = 0
    number = 0
    for k in data.keys():
        if pd.to_datetime(k) > threshold_date:
            date_data = data[k]
            avg_count += len(date_data)
            avg_error += np.mean([date_data[isin] for isin in date_data.keys()])
            number += 1

    avg_count = avg_count / number
    avg_error = avg_error / number
    ref_agg = db.reference('/Aggregations_results').child(
        '{aggregation_directory}'.format(aggregation_directory=aggregation_directory))
    ref_agg.set({'average_count_per_period': avg_count,
                 'average_error_per_period': avg_error,
                 'number_of_available_dates_per_period': number,
                 'aggregation_period': period_days,
                 'aggregation_date': as_of_date})


def agg_for_correlations(correlations_directory, aggregation_directory):
    '''
    Reads the correlation results from the database,
    does aggregations and writes the aggregation results back to the database.
    Args:
        correlations_directory: str, the path where the correlations output is stored.
        aggregation_directory: str, the path where the aggregation output should be stored.
    '''
    report_agg = dict()
    data = db.reference('/FileOutputs/{correlations_directory}'.format(correlations_directory=correlations_directory)).get()

    for k in data.keys():
        report_isin = dict()
        isin_corr = data[k]
        sorted_isins = sorted(isin_corr)
        isin_max = sorted_isins[0]
        isin_min = sorted_isins[len(sorted_isins) - 1]
        report_isin['maximum_correlated_isin'] = isin_max
        report_isin['maximum_correlation_coefficient'] = isin_corr[isin_max]
        report_isin['minimum_correlated_isin'] = isin_min
        report_isin['minimum_correlation_coefficient'] = isin_corr[isin_min]
        report_agg[k] = report_isin

    ref_agg = db.reference('/Aggregations_results').child(
        '{aggregation_directory}'.format(aggregation_directory=aggregation_directory))
    ref_agg.set(report_agg)


def perform_all_aggregations(event, context, check1_days=30, check2_days=90, as_of_date='2018-12-31'):
    '''
    Runs all aggregations:
    Args:
         check1_days: int, passes to period_days
         check2_days: int, passes to period_days
         as_of_date: passes to as_of_date
    '''
    init_app()
    agg_for_checks(check_directory='check1', aggregation_directory='agg1', period_days=check1_days, as_of_date=as_of_date)
    agg_for_checks(check_directory='check2', aggregation_directory='agg2', period_days=check2_days, as_of_date=as_of_date)
    agg_for_correlations(correlations_directory='Correlations', aggregation_directory='agg_correlations')
