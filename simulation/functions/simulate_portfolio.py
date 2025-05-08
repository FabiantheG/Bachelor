

from database.functions import *
from database.session import session
import pandas as pd
import numpy as np




def simulate_portfolio(asset_return,currencies,fx_return,fwd_return,weights,hedge_ratio   ,start = '2004-01-01',end = '2017-12-31'):
    """
        Simulate the performance of a portfolio with and without currency hedging.

        Computes the local, unhedged, and hedged growth of a multi-asset portfolio using monthly log returns
        of asset prices and foreign exchange rates. It uses the hedge ratios from the dcf_function to calculate
        the hedge growth.

        :param asset_return: DataFrame of asset log returns with assets as columns and dates as index.
        :type asset_return: pandas.DataFrame
        :param currencies: List of currency codes corresponding to each asset in the same order as asset_return columns.
        :type currencies: list[str]
        :param fx_return: DataFrame of FX spot log returns with currency codes as columns and dates as index.
        :type fx_return: pandas.DataFrame
        :param fwd_return: DataFrame of forward hedge log returns with currency codes as columns and dates as index.
        :type fwd_return: pandas.DataFrame
        :param weights: Portfolio weights for each asset. Length must match number of asset columns.
        :type weights: list[float] or numpy.ndarray
        :param hedge_ratio: A DataFrame with time-varying hedge ratios
                            (same index as asset_return and same columns).
        :type hedge_ratio: pandas.DataFrame
        :param start: Start date of the simulation (inclusive, format 'YYYY-MM-DD').
        :type start: str
        :param end: End date of the simulation (inclusive, format 'YYYY-MM-DD').
        :type end: str

        :return: DataFrame with columns 'local_growth', 'unhedged_growth', and 'hedged_growth', representing
                 the cumulative growth of the portfolio under different FX hedging assumptions.
        :rtype: pandas.DataFrame
        """

    df = pd.DataFrame()
    local_growth_df = pd.DataFrame()
    hedged_growth_df = pd.DataFrame()
    unhedged_growth_df = pd.DataFrame()

    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)

    #if hedge_ratio == 1:
    #    hedge_ratio = pd.DataFrame(1, index=asset_return.index, columns=currencies)



    for x in range(len(asset_return.columns)):
        currency = currencies[x]
        asset_ticker = asset_return.columns[x]
        current_asset_return = asset_return[asset_ticker]
        current_fx_return = fx_return[currency]
        current_fwd_return = fwd_return[currency]
        current_hedge_ratio = hedge_ratio[currency]

        current_df = pd.concat([
            current_asset_return,
            current_fx_return,
            current_fwd_return,
            current_hedge_ratio
        ], axis=1, keys=['asset', 'fx', 'fwd', 'current_hedge_ratio'])


        current_df = current_df.dropna()

        current_df['hedge_logreturn']  = (current_df["fwd"] - current_df["fx"]) *  current_df['current_hedge_ratio']

        current_df['total_hedged_logreturn'] = current_df['asset'] + current_df['fx'] + current_df['hedge_logreturn']

        current_df['total_unhedged_logreturn']  = current_df['asset'] + current_df['fx']

        local_growth_df[asset_ticker] = current_df['asset']
        unhedged_growth_df[asset_ticker] = current_df['total_unhedged_logreturn']
        hedged_growth_df[asset_ticker] = current_df['total_hedged_logreturn']


    # filter with start and end date

    local_growth_df = local_growth_df.loc[start_date:end_date]
    unhedged_growth_df = unhedged_growth_df.loc[start_date:end_date]
    hedged_growth_df = hedged_growth_df.loc[start_date:end_date]

    # cumsum
    local_growth_df = local_growth_df.cumsum()
    unhedged_growth_df = unhedged_growth_df.cumsum()
    hedged_growth_df = hedged_growth_df.cumsum()

    # backtransformation to get the growth
    local_growth_df = np.exp(local_growth_df)
    unhedged_growth_df = np.exp(unhedged_growth_df)
    hedged_growth_df = np.exp(hedged_growth_df)

    # Normalize to start at 1
    local_growth_df = local_growth_df / local_growth_df.iloc[0]
    unhedged_growth_df = unhedged_growth_df / unhedged_growth_df.iloc[0]
    hedged_growth_df = hedged_growth_df / hedged_growth_df.iloc[0]

    local_growth_df = local_growth_df.dot(weights)
    unhedged_growth_df = unhedged_growth_df.dot(weights)
    hedged_growth_df = hedged_growth_df.dot(weights)

    df['local_growth'] = local_growth_df
    df['unhedged_growth'] = unhedged_growth_df
    df['hedged_growth'] = hedged_growth_df


    return df

