import pandas as pd
import numpy as np
import datetime as dt


def check1(df, err_tolerance):
    '''
    There is a formula for calculation of NAVPS.
    NAV_Per_Share = Net_Asset_Value / Nb_Shares_Outstanding
    This function performs a check whether the formula is hold in the df.
    Reports all ISIN, Date combinations for which it does not hold.
    Input:
        df: pandas dataframe
        err_tolerance: float
    Output:
        report_dict: dict('Valuation_Date': [ISIN_Codes])
    '''
    df1 = df.copy()
    df1['error'] = np.abs(df1.Net_Asset_Value / df1.Nb_Shares_Outstanding - df1.NAV_Per_Share) / df1.NAV_Per_Share
    report = df1.loc[ df1['error'] > err_tolerance, ['Valuation_Date', 'ISIN_Code', 'error']].copy()
    report_dict = dict()
    for valuation_date in report['Valuation_Date'].unique():
        date_key = str(pd.to_datetime(valuation_date).date())
        report_isin = dict()
        for i, val in report.loc[report['Valuation_Date'] == valuation_date, ['ISIN_Code', 'error']].iterrows():
            report_isin[val['ISIN_Code']] = val['error']
        report_dict[date_key] = report_isin
    return report_dict


def check2(df, year_tolerance):
    '''
    report nonsense dates for Dividend_Payment_Date.
    The column is converted from a decimal number to a date and then quality checked.
    Input:
        df: pandas dataframe
        year_tolerance: float, a tolerated period between dividend payment date and today in years
    Output:
        report_dict: dict('Valuation_Date': [ISIN_Codes])
    '''
    df_ = df.loc[df['Dividend_Payment_Date'] > 0, :].copy()
    df_['Dividend_Payment_Date'] = pd.TimedeltaIndex(df_['Dividend_Payment_Date'], unit='d') + dt.datetime(1900, 1, 1)

    year_fraction_from_now = (df_['Dividend_Payment_Date'] - dt.datetime.now()).dt.days / 365
    df_['error'] = np.abs(year_fraction_from_now)
    report = df_.loc[df_['error'] > year_tolerance, ['Valuation_Date', 'ISIN_Code', 'error']].copy()
    report_dict = dict()
    for valuation_date in report['Valuation_Date'].unique():
        date_key = str(pd.to_datetime(valuation_date).date())
        report_isin = dict()
        for i, val in report.loc[report['Valuation_Date'] == valuation_date, ['ISIN_Code', 'error']].iterrows():
            report_isin[val['ISIN_Code']] = val['error']
        report_dict[date_key] = report_isin
    return report_dict


def calculate_corr(df):
    '''
    report correlations between the portfolios.
    Input:
        df: pandas dataframe
    Output:
        report_dict: dict(ISIN: dict(ISIN: correlation))
    '''
    isin_list = df.ISIN_Code.unique()
    df_corr = pd.DataFrame()
    # construct the df of time series with each isin in a separate column
    for i, isin in enumerate(isin_list):
        df_tmp = df.loc[df.ISIN_Code == isin, ['Valuation_Date', 'NAV_Per_Share']].copy()

        if i == 0:
            df_corr = df_tmp
        else:
            df_corr = pd.merge(df_corr, df_tmp, on='Valuation_Date', how='outer')  # outer join!
        df_corr.rename(columns={'NAV_Per_Share': isin}, inplace=True)  # name the value by isin

        df_corr.sort_values(by='Valuation_Date', inplace=True)  # sort by date

    df_corr_calc = df_corr.ffill(axis=0)  # perform forward fill for NA values
    # As the time series are not stationary, perform first order difference
    df_corr_calc[isin_list] = df_corr_calc[isin_list].diff()
    df_corr_calc = df_corr_calc.corr()  # Calculate elementwise correlations
    # create a report for correlation values for pairwise correlations.
    report_dict = dict()
    for i, isin in enumerate(isin_list):
        report_per_isin = dict()
        for j in range(len(isin_list)):
            if i != j:  # reporting all for convenience, except with itself.
                report_per_isin[isin_list[j]] = df_corr_calc.iloc[i, j]
        report_dict[isin] = report_per_isin
    return report_dict
