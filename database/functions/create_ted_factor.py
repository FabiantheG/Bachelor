

import pandas as pd
import numpy as np


def create_ted_factor():

    df  = pd.read_csv('database/csv_file/TEDRATE.csv')
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    df.columns = ['rate']

    df = df.interpolate(method='linear') # predict NaNs
    df = df.resample('M').last()


    df['rate'] = (1 / 3) * np.log(df['rate'] / df['rate'].shift(3))

    df = df.dropna()
    return df