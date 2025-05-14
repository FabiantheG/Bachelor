from scipy.signal import dfreqresp

from database.models import *
from database.session import session
from database.functions import *
import pandas as pd
import numpy as np

def create_dollar_factor(base = 'CHF'):
    """
    Create the dollar factor time series.

    This function computes the monthly dollar factor by averaging the one-month FX forward
    returns of the G10 currencies against USD. For each currency in the G10 group (EUR, JPY,
    CHF, GBP, AUD, CAD, NZD, NOK, SEK), it:

    1. Retrieves daily spot and 1-month forward rates via `get_fx('USD', currency, duration)`.
    2. Resamples both series to month-end frequency.
    3. Joins spot and forward rates, drops missing data.
    4. Calculates the forward return for t+1 as `(F_t / S_{t+1}) - 1`.
    5. Averages the cross-sectional returns across all currencies.

    :return: A pandas DataFrame indexed by month-end dates containing the dollar factor.
    :rtype: pandas.DataFrame
    """
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK','USD']
    g10 = [cur for cur in g10 if cur != base] # take all g10 besides the base

    fx_returns = []

    for quote in g10:
        # Retrieve daily spot and 1-month forward exchange rates (USD/quote)
        df = get_fx(base, quote, duration='Spot')


        # Resample to monthly frequency using end-of-month values
        df = df.resample('M').last()

        # calculate the currency return
        df[quote] = np.log(df['rate'] / df['rate'].shift(1))




        fx_returns.append(df[quote])

    # Combine all currency return series into one DataFrame
    fx_matrix = pd.concat(fx_returns, axis=1)


    # Compute the dollar factor as the cross-sectional average of FX returns
    dollar_factor = fx_matrix.mean(axis=1).to_frame(name='dollar_factor')
    dollar_factor = dollar_factor.dropna()

    return dollar_factor
