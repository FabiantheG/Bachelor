

import pandas as pd
import numpy as np


def create_ted_factor():

    df  = pd.read_csv('database/csv_file/ted.csv')
    df['date'] = pd.to_datetime(df['date'],dayfirst=True)
    df.set_index('date', inplace=True)

    df['ted_spread'] = df['CPDR9AFC Index'] - df['USGB090Y Index']
    new_df = df[['ted_spread']]
    df = new_df

    df.columns = ['rate']

    df = df.interpolate(method='linear') # predict NaNs
    df = df.resample('M').last()


    df['rate'] = (1 / 3) * np.log(df['rate'] / df['rate'].shift(3))

    df = df.dropna()
    return df