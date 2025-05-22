
from database.functions import *
import pandas as pd
import numpy as np


def create_volatility_factor(base_currency):
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK', 'USD']
    g10 = [curr for curr in g10 if curr != base_currency]


    spot_data = {}

    # 1. Load and resample spot rates relative to the base currency
    for quote in g10:
        spot = get_fx(base_currency, quote, duration='Spot')


        spot_data[quote] = spot['rate']


    # Combine into DataFrame
    spot_df = pd.concat(spot_data, axis=1)

    df = np.log(spot_df / spot_df.shift(1)) # calculate the logreturn
    df = df.shift(1) # shift one because its in the formula
    df = df**2   # square the logreturns
    df = df.resample('M').mean() # take the mean of the logreturns each months
    df = df.mean(axis=1) # take the mean of all different squared logreturns of euach month and each currency


    df = pd.DataFrame(df)

    df.columns = ['rate']

    df['rate'] = (1/3) * np.log(df['rate'] / df['rate'].shift(3))
    df = df.dropna()

    return df







'''
    spot = get_fx(currency,base,'Spot')

    volatility_factor = pd.DataFrame(index=spot.index)



    for t in range(window , len(spot)- 1):
        spot_window = spot.iloc[t - window:t]

        vol = spot_window.iloc[:, 0].std()

        current_date = spot.index[t]
        volatility_factor.at[current_date, 'volatility'+base + currency] = vol

    volatility_factor = volatility_factor.resample('M').last()
    volatility_factor = volatility_factor.dropna()
    return volatility_factor
'''