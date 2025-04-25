from database.models import *
from database.session import session
from database.functions import *
import pandas as pd


def create_dollar_factor():
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
    g10 = ['EUR', 'JPY', 'CHF', 'GBP', 'AUD', 'CAD', 'NZD', 'NOK', 'SEK']
    fx_returns = []

    for quote in g10:
        # Retrieve daily spot and 1-month forward exchange rates (USD/quote)
        spot_df = get_fx('USD', quote, duration='Spot')
        fwd_df = get_fx('USD', quote, duration='1M')

        # Resample to monthly frequency using end-of-month values
        spot_df = spot_df.resample('M').last()
        fwd_df = fwd_df.resample('M').last()

        # Join spot and forward rates into one DataFrame
        merged = spot_df.join(fwd_df, lsuffix='_spot', rsuffix='_fwd').dropna()

        # Calculate the FX forward return for t+1: (F_t / S_{t+1}) - 1
        merged['fx_return'] = (merged['rate_fwd'] / merged['rate_spot'].shift(-1)) - 1

        fx_returns.append(merged['fx_return'])

    # Combine all currency return series into one DataFrame
    fx_matrix = pd.concat(fx_returns, axis=1)
    fx_matrix.columns = g10

    # Compute the dollar factor as the cross-sectional average of FX returns
    dollar_factor = fx_matrix.mean(axis=1).to_frame(name='dollar_factor')
    dollar_factor = dollar_factor.dropna()

    return dollar_factor
