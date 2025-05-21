from database.models import *
from database.session import session
from database.functions import *
import pandas as pd
import numpy as np


def create_afd_factor(base_currency):


    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK', 'USD']
    g10 = [curr for curr in g10 if curr != base_currency]

    weights = [1/len(g10) for currency in g10]

    spot_data = {}
    fwd_data = {}

    # 1. Load and resample spot and forward rates relative to the base currency
    for quote in g10:
        spot = get_fx(base_currency, quote, duration='Spot').resample('M').last()
        fwd = get_fx(base_currency, quote, duration='1M').resample('M').last()

        spot_data[quote] = spot['rate']
        fwd_data[quote] = fwd['rate']

    # Combine into DataFrames
    spot_df = pd.concat(spot_data, axis=1)
    fwd_df = pd.concat(fwd_data, axis=1)

    # 2. Calculate carry premium = (Spot - Forward) / Spot
    forward_logreturn = np.log(fwd_df / spot_df)
    df = forward_logreturn.dot(weights) # calculate the mean
    df = df.dropna() # drop all NaN
    df = pd.DataFrame(df)
    df.columns = ['rate']


    return df

