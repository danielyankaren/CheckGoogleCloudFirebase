import pandas as pd
import firebase_admin
from firebase_admin import db
from scripts import check1, check2, calculate_corr
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/kdanielyan/NextGate/serviceAccountKey.json"

# set GOOGLE_APPLICATION_CREDENTIALS env variable to the path of service account key.
def main():
    df = pd.read_csv('ngt_software_engineer_test_example_data (clean columns).csv')

    default_app = firebase_admin.initialize_app(options={
        'databaseURL': 'https://nextgate-b8ba4.firebaseio.com/'
    })

    report1 = check1(df=df, err_tolerance=0.02)
    report2 = check2(df=df, year_tolerance=2)
    report_corr = calculate_corr(df)
    ref = db.reference('/FileOutputs/')

    ref_check1 = ref.child('check1')
    ref_check1.set(report1)

    ref_check2 = ref.child('check2')
    ref_check2.set(report2)

    ref_corr = ref.child('Correlations')
    ref_corr.set(report_corr)


if __name__ == '__main__':

    main()
